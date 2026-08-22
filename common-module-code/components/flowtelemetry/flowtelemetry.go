// Package flowtelemetry lets a flow read the platform's own execution traces.
//
// Every hop a flow makes is already recorded as a span. Nothing inside a flow
// could read them back, so an agent could act on a cluster but was blind to its
// own automations — it could not answer which of its flows failed, or where. The
// data was always there; this is the door to it.
package flowtelemetry

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/utils"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName = "flow_telemetry"
	RequestPort   = "request"
	ResultPort    = "result"
	ErrorPort     = "error"
)

const (
	// defaultLookbackMinutes covers the window an agent asking "what just broke"
	// cares about, without pulling a day of history it will not read.
	defaultLookbackMinutes = 60
	// maxTraces bounds the reply. A busy project can hold thousands of traces
	// per hour, and the whole list would become one message.
	maxTraces = 200
	// maxScan bounds how many traces are examined while looking for matches.
	// errorsOnly may have to read far past its results, and an unbounded scan
	// would let one hop hold the window open over a busy project.
	maxScan = 5000
	// maxPages is a backstop against a collector that keeps answering with the
	// same page: the loop would otherwise never end.
	maxPages = 100

	// otelStatsPort is the collector's statistics service, matching what the
	// operator chart exposes and what the CLI connects to.
	otelStatsPort = 2345
)

type Context any

type Settings struct {
	// OtelService and OtelPort address the collector. Defaults match what the
	// operator chart installs, so the common case needs no configuration.
	OtelService     string `json:"otelService" required:"true" title:"Collector Service" description:"Kubernetes service name of the otel-collector holding the traces."`
	OtelPort        int    `json:"otelPort" required:"true" title:"Collector Port" description:"Port of the collector's statistics service."`
	Namespace       string `json:"namespace,omitempty" title:"Namespace" description:"Namespace the collector runs in. Defaults to this module's own namespace."`
	EnableErrorPort bool   `json:"enableErrorPort" title:"Enable Error Port" description:"Emit failures on the error port instead of failing the flow."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passed through to the result unchanged."`
	Project string  `json:"project" required:"true" title:"Project" description:"Project whose traces to read — the resource name, e.g. the one list_projects reports."`
	Flow    string  `json:"flow,omitempty" title:"Flow" description:"Limit to one flow. Empty reads every flow in the project."`
	// TraceID switches from listing to detail: an agent lists to find the
	// failing run, then asks for that one to see which hop broke.
	TraceID         string `json:"traceId,omitempty" title:"Trace ID" description:"Fetch this single trace's spans instead of a list. Use an id from a previous list result."`
	LookbackMinutes int    `json:"lookbackMinutes,omitempty" title:"Lookback (minutes)" description:"How far back to look when listing. Defaults to 60."`
	ErrorsOnly      bool   `json:"errorsOnly,omitempty" title:"Errors Only" description:"List only traces that recorded at least one error."`
}

// Trace is one flow execution, summarised.
type Trace struct {
	ID         string `json:"id" title:"Trace ID"`
	Spans      int64  `json:"spans" title:"Spans" description:"Hops the run made."`
	Errors     int64  `json:"errors" title:"Errors"`
	DurationMs int64  `json:"durationMs" title:"Duration (ms)"`
	StartedAt  string `json:"startedAt" title:"Started At" description:"RFC3339."`
}

// Span is one hop within a trace: which port sent it, which port received it.
type Span struct {
	ID         string `json:"id" title:"Span ID"`
	Name       string `json:"name" title:"Name"`
	From       string `json:"from,omitempty" title:"From" description:"Source node and port."`
	To         string `json:"to,omitempty" title:"To" description:"Target node and port."`
	DurationMs int64  `json:"durationMs" title:"Duration (ms)"`
}

type Result struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Traces  []Trace `json:"traces" title:"Traces" description:"Matching runs, newest first. Empty when a single trace was requested."`
	// Spans is populated only for a single-trace request, so a flow can branch
	// on which shape it asked for without a second field to check.
	Spans       []Span `json:"spans,omitempty" title:"Spans" description:"Hops of the requested trace. Empty when listing."`
	Total       int64  `json:"total" title:"Total" description:"Traces the collector holds for the window, before any filtering."`
	ErrorTraces int    `json:"errorTraces" title:"Error Traces" description:"Traces with at least one error among those scanned."`
	// Scanned and Truncated exist so a partial answer can never be mistaken for
	// a complete one. errorsOnly reads past its results, and a caller deciding
	// "nothing failed" deserves to know whether the whole window was examined.
	Scanned   int  `json:"scanned" title:"Scanned" description:"Traces examined. Below total when the scan stopped early."`
	Truncated bool `json:"truncated" title:"Truncated" description:"True when the scan stopped before the end of the window — treat an empty result as inconclusive, and narrow the window or the flow."`
}

type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	namespace string
	nsLock    sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			OtelService: utils.DefaultOtelService,
			OtelPort:    otelStatsPort,
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Flow Telemetry",
		Info: "Reads the platform's own execution traces, so a flow can inspect how flows are running. Without a traceId it lists runs in the window (id, span count, error count, duration) — set errorsOnly to find failures. With a traceId it returns that run's hops, each naming the source and target port, which is how you find WHERE a run broke. " +
			"Needs the project's resource name. Works on any install: the traces come from the otel-collector the operator already deploys, with no Prometheus or external backend involved. " +
			"Pair it with an LLM to diagnose a failing automation, or with a cron to report what broke overnight.",
		Tags: []string{"SDK", "Observability", "Agent"},
	}
}

// OnClient supplies the namespace the collector runs in. The k8s client itself
// is unused — the collector is reached over its own service, not the API server
// — but this is the only hook that reports the namespace.
func (c *Component) OnClient(k8sClient module.K8sClient) {
	if k8sClient == nil {
		return
	}
	c.nsLock.Lock()
	c.namespace = k8sClient.GetNamespace()
	c.nsLock.Unlock()
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if strings.TrimSpace(in.OtelService) == "" {
		return fmt.Errorf("collector service is required")
	}
	if in.OtelPort <= 0 {
		return fmt.Errorf("collector port must be positive")
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

func (c *Component) getSettings() Settings {
	c.settingsLock.RLock()
	defer c.settingsLock.RUnlock()
	return c.settings
}

// resolveNamespace picks where to look for the collector: the setting wins, then
// the namespace this module runs in, then the one the operator installs into.
func (c *Component) resolveNamespace(set Settings) string {
	if set.Namespace != "" {
		return set.Namespace
	}
	c.nsLock.RLock()
	ns := c.namespace
	c.nsLock.RUnlock()
	if ns != "" {
		return ns
	}
	if env := os.Getenv("POD_NAMESPACE"); env != "" {
		return env
	}
	return "tinysystems"
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	if strings.TrimSpace(in.Project) == "" {
		return c.handleError(ctx, handler, in.Context, module.Permanent(fmt.Errorf("project is required")))
	}

	set := c.getSettings()
	ns := c.resolveNamespace(set)

	// In-cluster the collector is reachable at its service address directly, so
	// unlike the CLI there is no port-forward to broker.
	address := fmt.Sprintf("%s.%s.svc.cluster.local:%d", set.OtelService, ns, set.OtelPort)
	svc := utils.NewTraceService(utils.TraceServiceConfig{
		Client:      &utils.ConstantAddressClient{Address: address},
		OtelService: set.OtelService,
		OtelPort:    set.OtelPort,
	})
	defer svc.Close()

	if in.TraceID != "" {
		return c.detail(ctx, handler, svc, ns, in)
	}
	return c.list(ctx, handler, svc, ns, in)
}

// detail returns one run's hops — what an agent asks for once it knows which
// run failed.
func (c *Component) detail(ctx context.Context, handler module.Handler, svc *utils.TraceService, ns string, in Request) module.Result {
	data, err := svc.GetTraceByID(ctx, ns, in.Project, in.TraceID)
	if err != nil {
		// The collector being unreachable or busy is transient; a retry may well
		// succeed, and the caller loses nothing by waiting.
		return c.handleError(ctx, handler, in.Context, module.Retryable(fmt.Errorf("get trace %s: %w", in.TraceID, err)))
	}

	spans := make([]Span, 0, len(data.Spans))
	for _, s := range data.Spans {
		// Which hop a span represents lives in its attributes, not in named
		// fields — the collector stores spans generically.
		var from, to string
		for _, attr := range s.Attributes {
			switch attr.Key {
			case "from":
				from = attr.Value
			case "to":
				to = attr.Value
			}
		}
		spans = append(spans, Span{
			ID:         s.SpanID,
			Name:       s.Name,
			From:       from,
			To:         to,
			DurationMs: (s.EndTimeUnixNano - s.StartTimeUnixNano) / int64(time.Millisecond),
		})
	}

	return handler(ctx, ResultPort, Result{
		Context: in.Context,
		Traces:  []Trace{},
		Spans:   spans,
	})
}

// list summarises the runs in the window.
func (c *Component) list(ctx context.Context, handler module.Handler, svc *utils.TraceService, ns string, in Request) module.Result {
	lookback := in.LookbackMinutes
	if lookback <= 0 {
		lookback = defaultLookbackMinutes
	}
	end := time.Now()
	start := end.Add(-time.Duration(lookback) * time.Minute)

	// Page through the window rather than reading one page. The collector
	// returns traces regardless of outcome, so a failure sits wherever it
	// happened to occur — filtering a single page would report "no errors" for a
	// project whose only failure is on page two, the worst possible answer to
	// the question this component exists to answer.
	var (
		traces     = make([]Trace, 0, maxTraces)
		errorCount int
		scanned    int
		total      int64
		offset     int64
		truncated  bool
		seen       = map[string]bool{}
	)

	for page := 0; page < maxPages; page++ {
		resp, err := svc.GetTraces(ctx, ns, in.Project, in.Flow, start, end, offset)
		if err != nil {
			return c.handleError(ctx, handler, in.Context, module.Retryable(fmt.Errorf("list traces: %w", err)))
		}
		total = resp.Total
		if len(resp.Traces) == 0 {
			break
		}

		for _, t := range resp.Traces {
			// A collector that ignored the offset would otherwise return the
			// same page forever and be counted repeatedly.
			if seen[t.ID] {
				continue
			}
			seen[t.ID] = true
			scanned++

			if in.ErrorsOnly && t.Errors == 0 {
				continue
			}
			if t.Errors > 0 {
				errorCount++
			}
			if len(traces) >= maxTraces {
				continue
			}
			traces = append(traces, Trace{
				ID:         t.ID,
				Spans:      t.Spans,
				Errors:     t.Errors,
				DurationMs: t.Duration / int64(time.Millisecond),
				// Start is microseconds since the epoch, matching what the
				// collector records.
				StartedAt: time.UnixMicro(t.Start).UTC().Format(time.RFC3339),
			})
		}

		offset += int64(len(resp.Traces))
		if len(traces) >= maxTraces {
			// Enough to return. Only claim completeness if nothing is left.
			truncated = total > 0 && offset < total
			break
		}
		if scanned >= maxScan {
			truncated = true
			break
		}
		if total > 0 && offset >= total {
			break
		}
	}

	return handler(ctx, ResultPort, Result{
		Context:     in.Context,
		Traces:      traces,
		Scanned:     scanned,
		Truncated:   truncated,
		Total:       total,
		ErrorTraces: errorCount,
	})
}

func (c *Component) handleError(ctx context.Context, handler module.Handler, reqContext Context, err error) module.Result {
	if !c.getSettings().EnableErrorPort {
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, module.NewError(reqContext, err))
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: c.getSettings(),
		},
		{
			Name:          RequestPort,
			Label:         "Request",
			Position:      module.Left,
			Configuration: Request{},
		},
		{
			Name:     ResultPort,
			Label:    "Result",
			Source:   true,
			Position: module.Right,
			// Concrete examples so an edge reading $.traces[0].id or
			// $.spans[0].from is checkable when the flow is built.
			Configuration: Result{
				Traces: []Trace{{ID: "abc123", Spans: 4, Errors: 1, DurationMs: 120, StartedAt: "2026-07-30T00:00:00Z"}},
				Spans:  []Span{{ID: "s1", Name: "hop", From: "node-a:out", To: "node-b:in", DurationMs: 12}},
			},
		},
	}
	if !c.getSettings().EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name:          ErrorPort,
		Label:         "Error",
		Source:        true,
		Position:      module.Bottom,
		Configuration: module.ErrorMessage{},
	})
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ClientAware     = (*Component)(nil)
)

func init() {
	registry.Register((&Component{}).Instance())
}
