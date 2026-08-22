package embedtext

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/pkg/bundle"
	"github.com/tiny-systems/module/registry"
)

const (
	ComponentName  = "embed_text"
	RequestPort    = "request"
	ResponsePort   = "response"
	ErrorPort      = "error"
	defaultTimeout = 30 * time.Second
)

type Context any

type Settings struct {
	EnableErrorPort bool   `json:"enableErrorPort" required:"true" title:"Enable Error Port"`
	BaseURL         string `json:"baseURL" title:"TEI Base URL" description:"Override the TEI endpoint. Defaults to in-cluster discovery via the tei bundle when running inside the operator chart."`
	TimeoutSeconds  int    `json:"timeoutSeconds" minimum:"1" default:"30" title:"Timeout Seconds"`
	Truncate        bool   `json:"truncate" title:"Truncate" description:"Ask TEI to silently truncate inputs longer than the model's max sequence length instead of erroring."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context"`
	Text    string  `json:"text" required:"true" minLength:"1" title:"Text" format:"textarea" description:"Text to embed. Single string; for batches call embed_text per item until embed_batch lands."`
}

type Response struct {
	Context   Context   `json:"context,omitempty" configurable:"true" title:"Context"`
	Embedding []float32 `json:"embedding" title:"Embedding" description:"Dense vector. Length matches the model dims (384 for BAAI/bge-small-en-v1.5)."`
	Dims      int       `json:"dims" title:"Dims"`
}

type Component struct {
	settings Settings
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			TimeoutSeconds: int(defaultTimeout / time.Second),
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Embed Text",
		Info:        "Calls a HuggingFace text-embeddings-inference (TEI) endpoint and emits the dense embedding vector. With the tei bundle enabled on install the TEI service lives in-cluster at http://<release>-tei:80 and TEI_URL is wired automatically. Override the URL via settings.baseURL for external endpoints.",
		Tags:        []string{"Embeddings", "TEI", "Vectors", "RAG"},
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	c.settings = in
	return nil
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid request"))
	}
	return c.embed(ctx, handler, in)
}

type teiRequest struct {
	Inputs   string `json:"inputs"`
	Truncate bool   `json:"truncate,omitempty"`
}

func (c *Component) embed(ctx context.Context, handler module.Handler, in Request) module.Result {
	// Embedding is a pure function of the text, so every transient failure below
	// is safe to re-run — the only judgement needed is whether it *could* clear.
	baseURL := strings.TrimRight(c.resolveBaseURL(), "/")
	if baseURL == "" {
		// Misconfiguration: no amount of backoff supplies a missing URL.
		return c.fail(ctx, handler, in.Context, module.Permanent(fmt.Errorf("TEI endpoint not configured: set settings.baseURL or the TEI_URL env var")))
	}

	timeout := time.Duration(c.settings.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	payload, err := json.Marshal(teiRequest{Inputs: in.Text, Truncate: c.settings.Truncate})
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+"/embed", bytes.NewReader(payload))
	if err != nil {
		return c.fail(ctx, handler, in.Context, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// Never got a response — TEI still warming up, pod rescheduled, dial or
		// timeout. Exactly what a backoff is for.
		return c.fail(ctx, handler, in.Context, module.Retryable(fmt.Errorf("tei request: %w", err)))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return c.fail(ctx, handler, in.Context, module.Retryable(fmt.Errorf("read tei response: %w", err)))
	}

	if resp.StatusCode >= 400 {
		statusErr := fmt.Errorf("tei status %d: %s", resp.StatusCode, string(body))
		// 429 means TEI's queue is full and 5xx means it failed on its side;
		// both clear on their own. A 4xx is about this text (too long for the
		// model, malformed payload) and will fail identically forever.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			statusErr = module.Retryable(statusErr)
		}
		return c.fail(ctx, handler, in.Context, statusErr)
	}

	// TEI returns [[float, float, ...]] — a list of vectors, one per
	// input. Single-input request gives a single-element outer list.
	var parsed [][]float32
	if err := json.Unmarshal(body, &parsed); err != nil {
		return c.fail(ctx, handler, in.Context, fmt.Errorf("decode tei response: %w", err))
	}
	if len(parsed) == 0 {
		return c.fail(ctx, handler, in.Context, fmt.Errorf("tei returned empty result"))
	}

	vec := parsed[0]
	return handler(ctx, ResponsePort, Response{
		Context:   in.Context,
		Embedding: vec,
		Dims:      len(vec),
	})
}

// resolveBaseURL picks settings.baseURL first, then in-cluster
// bundle discovery. With the tei bundle enabled the operator chart
// provisions a Service at <release>-tei in the same namespace;
// bundle.URL renders that. Returns empty when RELEASE_NAME isn't
// set and no override is configured — the caller surfaces that as
// a misconfiguration error.
func (c *Component) resolveBaseURL() string {
	if c.settings.BaseURL != "" {
		return c.settings.BaseURL
	}
	return bundle.URLOr("tei", "")
}

func (c *Component) fail(ctx context.Context, handler module.Handler, reqCtx Context, err error) module.Result {
	if !c.settings.EnableErrorPort {
		// Bubble unchanged so retryability marked at the call site survives.
		return module.Fail(err)
	}
	return handler(ctx, ErrorPort, module.NewError(reqCtx, err))
}

func (c *Component) Ports() []module.Port {
	ports := []module.Port{
		{Name: v1alpha1.SettingsPort, Label: "Settings", Configuration: c.settings},
		{Name: RequestPort, Label: "Request", Configuration: Request{}, Position: module.Left},
		{Name: ResponsePort, Label: "Response", Source: true, Configuration: Response{}, Position: module.Right},
	}
	if !c.settings.EnableErrorPort {
		return ports
	}
	return append(ports, module.Port{
		Name: ErrorPort, Label: "Error", Source: true, Configuration: module.ErrorMessage{}, Position: module.Bottom,
	})
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
)

func init() {
	registry.Register(&Component{})
}
