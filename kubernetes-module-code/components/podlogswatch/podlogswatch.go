package podlogswatch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tiny-systems/kubernetes-module/pkg/k8s"
	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "pod_logs_watch"

	StartPort = "start"
	EventPort = "event"
	ErrorPort = "error"
)

// Context type alias for schema generation
type Context any

// Settings configures the watcher behavior
type Settings struct {
	EnableErrorPort bool `json:"enableErrorPort" title:"Enable Error Port" description:"Output errors to error port instead of failing"`
}

// Start initiates log watching with the given configuration
type Start struct {
	Context         Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Arbitrary context to pass through to events"`
	Namespace       string  `json:"namespace,omitempty" title:"Namespace" description:"Namespace to watch (empty = all namespaces)"`
	LabelSelector   string  `json:"labelSelector,omitempty" title:"Label Selector" description:"Filter pods by labels (e.g., app=myapp)"`
	Keywords        string  `json:"keywords" required:"true" title:"Keywords" format:"textarea" description:"Keywords to match in logs, one per line. Case-insensitive."`
	TailLines       int64   `json:"tailLines,omitempty" title:"Tail Lines" description:"Historical log lines to scan on connect (default: 10)"`
	CooldownSeconds int     `json:"cooldownSeconds,omitempty" title:"Cooldown Seconds" description:"Don't re-alert same pod+keyword within this period (0 = no cooldown)"`
}

// LogEvent is emitted when a keyword match is found in pod logs
type LogEvent struct {
	Context   Context `json:"context,omitempty" title:"Context"`
	Pod       string  `json:"pod" title:"Pod"`
	Namespace string  `json:"namespace" title:"Namespace"`
	Container string  `json:"container,omitempty" title:"Container"`
	Keyword   string  `json:"keyword" title:"Matched Keyword"`
	Line      string  `json:"line" title:"Log Line" description:"The log line containing the keyword"`
	Timestamp string  `json:"timestamp" title:"Timestamp"`
}

// Error output for error port
type Error struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Error   string  `json:"error" title:"Error"`
}

// Control shows the current watcher status
type Control struct {
	Status        string `json:"status" title:"Status" readonly:"true"`
	Namespace     string `json:"namespace,omitempty" title:"Namespace" readonly:"true"`
	LabelSelector string `json:"labelSelector,omitempty" title:"Label Selector" readonly:"true"`
	ActivePods    int    `json:"activePods" title:"Active Pods" readonly:"true"`
	MatchCount    int    `json:"matchCount" title:"Matches Found" readonly:"true"`
}

// podStream tracks a per-pod log streaming goroutine
type podStream struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Component implements the pod log watcher
type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient     client.Client
	k8sClientLock sync.RWMutex

	logsClient     *k8s.LogsClient
	logsClientLock sync.RWMutex

	control     Control
	controlLock sync.RWMutex

	cancelFunc     context.CancelFunc
	cancelFuncLock sync.Mutex

	watchDone     chan struct{}
	watchDoneLock sync.Mutex

	matchCount     int
	matchCountLock sync.Mutex

	// cooldown tracking: pod+keyword -> last emit time
	lastEmitTime     map[string]time.Time
	lastEmitTimeLock sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{
		control: Control{Status: "Not watching"},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Pod Log Watcher",
		Info:        "Watch pod logs in real-time for keyword matches. Streams logs from all pods matching namespace and label filters, emits events on match. Discovers new pods every 30s and reconnects on stream interruption.",
		Tags:        []string{"Kubernetes", "Pods", "Logs", "Watch", "Monitoring"},
	}
}

// OnClient stashes the K8s client and initializes the logs client.
func (c *Component) OnClient(k8sClient module.K8sClient) {
	if k8sClient == nil {
		return
	}
	c.k8sClientLock.Lock()
	c.k8sClient = k8sClient.GetK8sClient()
	c.k8sClientLock.Unlock()
	c.initLogsClient()
	log.Debug().Msg("pod_logs_watch: K8s client received")
}

// OnSettings stores the component settings.
func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings message")
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

// Handle dispatches the StartPort. System ports go through capabilities.
func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != StartPort {
		return module.Fail(fmt.Errorf("unknown port: %s", port))
	}
	if msg == nil {
		log.Info().Msg("pod_logs_watch: StartPort received nil, stopping")
		return module.Fail(c.stop())
	}
	in, ok := msg.(Start)
	if !ok {
		return module.Fail(fmt.Errorf("invalid start message"))
	}

	if done := c.getWatchDone(); done != nil {
		log.Info().Msg("pod_logs_watch: already running, waiting for watcher to stop")
		select {
		case <-done:
			return module.Result{}
		case <-ctx.Done():
			c.stop()
			return module.Result{}
		}
	}

	return module.Fail(c.runWatch(ctx, handler, in))
}

func (c *Component) initLogsClient() {
	c.logsClientLock.Lock()
	defer c.logsClientLock.Unlock()

	if c.logsClient != nil {
		return
	}

	logsClient, err := k8s.NewLogsClient()
	if err != nil {
		log.Error().Err(err).Msg("pod_logs_watch: failed to create logs client")
		return
	}
	c.logsClient = logsClient
}

func (c *Component) getWatchDone() chan struct{} {
	c.watchDoneLock.Lock()
	defer c.watchDoneLock.Unlock()
	return c.watchDone
}

func (c *Component) setWatchDone(done chan struct{}) {
	c.watchDoneLock.Lock()
	defer c.watchDoneLock.Unlock()
	c.watchDone = done
}

func (c *Component) setCancelFunc(fn context.CancelFunc) {
	c.cancelFuncLock.Lock()
	defer c.cancelFuncLock.Unlock()
	c.cancelFunc = fn
}

func (c *Component) stop() error {
	c.cancelFuncLock.Lock()
	defer c.cancelFuncLock.Unlock()

	if c.cancelFunc == nil {
		return nil
	}

	log.Info().Msg("pod_logs_watch: stopping watcher")
	c.cancelFunc()
	return nil
}

func (c *Component) runWatch(ctx context.Context, handler module.Handler, start Start) error {
	c.k8sClientLock.RLock()
	k8sClient := c.k8sClient
	c.k8sClientLock.RUnlock()

	if k8sClient == nil {
		c.updateControl(ctx, handler, Control{Status: "Error: K8s client not available"})
		return fmt.Errorf("K8s client not available")
	}

	c.logsClientLock.RLock()
	logsClient := c.logsClient
	c.logsClientLock.RUnlock()

	if logsClient == nil {
		c.updateControl(ctx, handler, Control{Status: "Error: Logs client not available"})
		return fmt.Errorf("Logs client not available")
	}

	keywords := parseKeywords(start.Keywords)
	if len(keywords) == 0 {
		c.updateControl(ctx, handler, Control{Status: "Error: No keywords configured"})
		return fmt.Errorf("no keywords configured")
	}

	tailLines := start.TailLines
	if tailLines <= 0 {
		tailLines = 10
	}

	// Build list options
	listOpts := &client.ListOptions{
		Namespace: start.Namespace,
	}
	if start.LabelSelector != "" {
		selector, err := metav1.ParseToLabelSelector(start.LabelSelector)
		if err != nil {
			c.updateControl(ctx, handler, Control{Status: fmt.Sprintf("Error: %v", err)})
			return fmt.Errorf("invalid label selector: %v", err)
		}
		labelSelector, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			c.updateControl(ctx, handler, Control{Status: fmt.Sprintf("Error: %v", err)})
			return fmt.Errorf("invalid label selector: %v", err)
		}
		listOpts.LabelSelector = labelSelector
	}

	// Create done channel
	done := make(chan struct{})
	c.setWatchDone(done)
	defer func() {
		c.setWatchDone(nil)
		close(done)
	}()

	// Use Background context - watch is long-running and shouldn't inherit caller's deadline
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	// Bridge: cancel watcher when parent context is done
	go func() {
		select {
		case <-ctx.Done():
			watchCancel()
		case <-watchCtx.Done():
		}
	}()

	c.setCancelFunc(watchCancel)
	defer c.setCancelFunc(nil)

	// Reset match count
	c.matchCountLock.Lock()
	c.matchCount = 0
	c.matchCountLock.Unlock()

	// Events channel - buffered for smoothing between producers and consumer
	events := make(chan LogEvent, 100)

	// Start discovery loop (manages per-pod goroutines)
	go c.discoveryLoop(watchCtx, k8sClient, logsClient, start.Context, keywords, tailLines, listOpts, events, handler)

	// Start emitter loop (reads from channel, applies cooldown, calls handler)
	go c.emitterLoop(watchCtx, handler, start, events)

	c.updateControl(ctx, handler, Control{
		Status:        "Watching",
		Namespace:     start.Namespace,
		LabelSelector: start.LabelSelector,
	})

	log.Info().
		Str("namespace", start.Namespace).
		Str("labelSelector", start.LabelSelector).
		Int("keywords", len(keywords)).
		Msg("Pod log watcher started")

	// Block until context done
	<-watchCtx.Done()

	c.updateControl(ctx, handler, Control{Status: "Stopped"})
	log.Info().Msg("Pod log watcher stopped")
	return watchCtx.Err()
}

func (c *Component) discoveryLoop(ctx context.Context, k8sClient client.Client, logsClient *k8s.LogsClient, userCtx Context, keywords []string, tailLines int64, listOpts *client.ListOptions, events chan<- LogEvent, handler module.Handler) {
	activePods := make(map[string]*podStream)
	defer func() {
		for _, ps := range activePods {
			ps.cancel()
		}
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Initial scan
	c.scanAndWatch(ctx, k8sClient, logsClient, userCtx, keywords, tailLines, listOpts, events, activePods, handler)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.scanAndWatch(ctx, k8sClient, logsClient, userCtx, keywords, tailLines, listOpts, events, activePods, handler)
		}
	}
}

func (c *Component) scanAndWatch(ctx context.Context, k8sClient client.Client, logsClient *k8s.LogsClient, userCtx Context, keywords []string, tailLines int64, listOpts *client.ListOptions, events chan<- LogEvent, activePods map[string]*podStream, handler module.Handler) {
	podList := &corev1.PodList{}
	if err := k8sClient.List(ctx, podList, listOpts); err != nil {
		log.Error().Err(err).Msg("pod_logs_watch: failed to list pods")
		return
	}

	currentPods := make(map[string]bool)

	for i := range podList.Items {
		pod := &podList.Items[i]

		// Only watch running pods
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		for _, container := range pod.Spec.Containers {
			key := pod.Namespace + "/" + pod.Name + "/" + container.Name
			currentPods[key] = true

			// Check if already watching and goroutine still alive
			if ps, exists := activePods[key]; exists {
				select {
				case <-ps.done:
					// Goroutine exited, remove and restart below
					delete(activePods, key)
				default:
					continue // Still running
				}
			}

			// Start watching this container
			podCtx, podCancel := context.WithCancel(ctx)
			doneCh := make(chan struct{})
			activePods[key] = &podStream{cancel: podCancel, done: doneCh}

			go func(ns, name, ctr string) {
				defer close(doneCh)
				c.watchPodLogs(podCtx, logsClient, ns, name, ctr, keywords, tailLines, events, userCtx)
			}(pod.Namespace, pod.Name, container.Name)
		}
	}

	// Cancel streams for pods that no longer exist
	for key, ps := range activePods {
		if !currentPods[key] {
			ps.cancel()
			delete(activePods, key)
		}
	}

	// Update control with active pod count
	c.matchCountLock.Lock()
	matchCount := c.matchCount
	c.matchCountLock.Unlock()

	c.controlLock.Lock()
	c.control.ActivePods = len(activePods)
	c.control.MatchCount = matchCount
	ctrl := c.control
	c.controlLock.Unlock()

	handler(ctx, v1alpha1.ControlPort, ctrl)
}

func (c *Component) watchPodLogs(ctx context.Context, logsClient *k8s.LogsClient, namespace, podName, container string, keywords []string, tailLines int64, events chan<- LogEvent, userCtx Context) {
	opts := &corev1.PodLogOptions{
		Follow:    true,
		TailLines: &tailLines,
	}
	if container != "" {
		opts.Container = container
	}

	stream, err := logsClient.StreamLogs(ctx, namespace, podName, opts)
	if err != nil {
		log.Debug().Err(err).Str("pod", podName).Str("container", container).Msg("pod_logs_watch: failed to open log stream")
		return
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	// Increase buffer for long log lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()
		lineLower := strings.ToLower(line)

		for _, keyword := range keywords {
			if !strings.Contains(lineLower, strings.ToLower(keyword)) {
				continue
			}

			// Truncate long lines
			displayLine := line
			if len(displayLine) > 500 {
				displayLine = displayLine[:500] + "..."
			}

			select {
			case events <- LogEvent{
				Context:   userCtx,
				Pod:       podName,
				Namespace: namespace,
				Container: container,
				Keyword:   keyword,
				Line:      displayLine,
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}:
			case <-ctx.Done():
				return
			}

			break // One match per line is enough
		}
	}

	if err := scanner.Err(); err != nil {
		log.Debug().Err(err).Str("pod", podName).Msg("pod_logs_watch: scanner error")
	}
}

func (c *Component) emitterLoop(ctx context.Context, handler module.Handler, start Start, events <-chan LogEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}

			// Apply cooldown
			if start.CooldownSeconds > 0 {
				cooldownKey := event.Namespace + "/" + event.Pod + ":" + event.Keyword
				cooldown := time.Duration(start.CooldownSeconds) * time.Second

				c.lastEmitTimeLock.RLock()
				lastEmit, exists := c.lastEmitTime[cooldownKey]
				c.lastEmitTimeLock.RUnlock()

				if exists && time.Since(lastEmit) < cooldown {
					continue
				}

				c.lastEmitTimeLock.Lock()
				if c.lastEmitTime == nil {
					c.lastEmitTime = make(map[string]time.Time)
				}
				c.lastEmitTime[cooldownKey] = time.Now()
				// Sweep stale entries to prevent unbounded growth
				for k, t := range c.lastEmitTime {
					if time.Since(t) >= cooldown {
						delete(c.lastEmitTime, k)
					}
				}
				c.lastEmitTimeLock.Unlock()
			}

			c.matchCountLock.Lock()
			c.matchCount++
			c.matchCountLock.Unlock()

			if err := handler(ctx, EventPort, event).Err(); err != nil {
				log.Error().Err(err).Str("pod", event.Pod).Msg("pod_logs_watch: failed to emit log event")
			}
		}
	}
}

func (c *Component) updateControl(ctx context.Context, handler module.Handler, ctrl Control) {
	c.controlLock.Lock()
	c.control = ctrl
	c.controlLock.Unlock()

	handler(ctx, v1alpha1.ControlPort, ctrl)
}

func (c *Component) handleError(handler module.Handler, userCtx Context, errMsg string) module.Result {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	if enableErrorPort {
		return handler(context.Background(), ErrorPort, Error{
			Context: userCtx,
			Error:   errMsg,
		})
	}
	return module.Fail(errors.New(errMsg))
}

func (c *Component) Ports() []module.Port {
	c.settingsLock.RLock()
	enableErrorPort := c.settings.EnableErrorPort
	c.settingsLock.RUnlock()

	c.controlLock.RLock()
	control := c.control
	c.controlLock.RUnlock()

	ports := []module.Port{
		{
			Name:          v1alpha1.ControlPort,
			Label:         "Control",
			Configuration: control,
		},
		{
			Name:     v1alpha1.ClientPort,
			Label:    "Client",
			Position: module.Left,
		},
		{
			Name:  v1alpha1.ReconcilePort,
			Label: "Reconcile",
		},
		{
			Name:          v1alpha1.SettingsPort,
			Label:         "Settings",
			Configuration: Settings{},
		},
		{
			Name:  StartPort,
			Label: "Start",
			Configuration: Start{
				Keywords:        "error\npanic\nOOMKilled",
				TailLines:       10,
				CooldownSeconds: 300,
			},
			Position: module.Left,
		},
		{
			Name:   EventPort,
			Label:  "Event",
			Source: true,
			Configuration: LogEvent{
				Pod:       "myapp-abc123",
				Namespace: "default",
				Container: "main",
				Keyword:   "error",
				Line:      "2024-01-01T00:00:00Z ERROR something went wrong",
				Timestamp: "2024-01-01T00:00:01Z",
			},
			Position: module.Right,
		},
	}

	if enableErrorPort {
		ports = append(ports, module.Port{
			Name:          ErrorPort,
			Label:         "Error",
			Source:        true,
			Configuration: Error{},
			Position:      module.Bottom,
		})
	}

	return ports
}

func parseKeywords(s string) []string {
	var keywords []string
	for _, line := range strings.Split(s, "\n") {
		kw := strings.TrimSpace(line)
		if kw != "" {
			keywords = append(keywords, kw)
		}
	}
	return keywords
}

var (
	_ module.Component       = (*Component)(nil)
	_ module.SettingsHandler = (*Component)(nil)
	_ module.ClientAware     = (*Component)(nil)
)
var _ module.Destroyer = (*Component)(nil)

func (c *Component) OnDestroy(_ map[string]string) {
	c.stop()
}

func init() {
	registry.Register(&Component{})
}
