// Package sandboxrun runs a script in a throwaway Kubernetes Job.
//
// The point is code an agent wrote: it cannot be trusted, and it runs inside the
// user's own cluster where the interesting data lives. Every knob set on the Job
// below exists to bound what that code can reach, and the defaults assume the
// script is hostile.
package sandboxrun

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tiny-systems/module/api/v1alpha1"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
	"github.com/tiny-systems/modules/kubernetes-module/pkg/k8s"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ComponentName = "sandbox_run"
	RequestPort   = "request"
	ResultPort    = "result"
	ErrorPort     = "error"
)

const (
	// maxTimeoutSeconds caps how long Handle will block. The run reconciler
	// re-drives a hop that has made no progress for six minutes, and a re-drive
	// runs the script a SECOND time — so the wait must finish comfortably
	// inside that window. Work needing longer does not belong in one blocking hop.
	maxTimeoutSeconds = 240
	// defaultTimeoutSeconds suits the common case: an agent reshaping data or
	// running a short script.
	defaultTimeoutSeconds = 120

	// pollInterval trades responsiveness against API chatter for short scripts.
	pollInterval = time.Second

	// ttlAfterFinished lets the Job (and its pod) linger briefly so a human can
	// inspect a failure, then disappear without a cleanup path of our own.
	ttlAfterFinished int32 = 300

	// sandboxUID is the same non-root uid the operator's own pod runs as.
	sandboxUID int64 = 65532

	// logTailLines bounds how much output is carried back into the flow. A
	// runaway script can print megabytes, which would otherwise become a message.
	logTailLines int64 = 2000
)

type Context any

type Settings struct {
	Image           string `json:"image" required:"true" title:"Image" description:"Container image the script runs in. Must contain the interpreter — e.g. python:3.12-slim, node:22-alpine, busybox."`
	Interpreter     string `json:"interpreter" required:"true" title:"Interpreter" description:"Executable that receives the script via -c, e.g. python, node, sh."`
	Namespace       string `json:"namespace,omitempty" title:"Namespace" description:"Where the Job runs. Defaults to the namespace this module is installed in. Point it at a dedicated, network-restricted namespace to bound what a script can reach."`
	TimeoutSeconds  int    `json:"timeoutSeconds" required:"true" title:"Timeout (seconds)" description:"How long to wait for the script. Also the Job's own deadline, so an overrunning script is killed rather than left behind. Capped at 240s: a hop with no progress for six minutes is re-driven, which would run the script twice."`
	CPULimit        string `json:"cpuLimit,omitempty" title:"CPU Limit" description:"Kubernetes CPU quantity, e.g. 500m. Empty means no limit."`
	MemoryLimit     string `json:"memoryLimit,omitempty" title:"Memory Limit" description:"Kubernetes memory quantity, e.g. 256Mi. Empty means no limit."`
	EnableErrorPort bool   `json:"enableErrorPort" title:"Enable Error Port" description:"Emit failures on the error port instead of failing the flow. A script that runs and exits non-zero is NOT a failure — it arrives on Result with its exit code."`
}

type Request struct {
	Context Context `json:"context,omitempty" configurable:"true" title:"Context" description:"Passed through to the result unchanged."`
	Script  string  `json:"script" required:"true" title:"Script" format:"textarea" description:"Source to execute. Passed as an argument to the interpreter, so it never touches a filesystem."`
	Image   string  `json:"image,omitempty" title:"Image" description:"Overrides the image from settings for this call."`
}

type Result struct {
	Context Context `json:"context,omitempty" title:"Context"`
	Stdout  string  `json:"stdout" title:"Output" description:"Combined output of the container, tail-limited."`
	// ExitCode is the script's own verdict. Nonzero is a normal outcome, not an
	// error: an agent asking "does this pass?" wants the code, not a failed flow.
	ExitCode int32  `json:"exitCode" title:"Exit Code" description:"Container exit status. 0 on success. -1 when the script was killed by the timeout."`
	Success  bool   `json:"success" title:"Success" description:"True when the script exited 0."`
	TimedOut bool   `json:"timedOut" title:"Timed Out" description:"True when the script hit the deadline and was killed."`
	JobName  string `json:"jobName" title:"Job Name" description:"The Job that ran it, for inspection before its TTL expires."`
}

type Component struct {
	settings     Settings
	settingsLock sync.RWMutex

	k8sClient    client.Client
	k8sNamespace string
	logsClient   *k8s.LogsClient
	clientLock   sync.RWMutex
}

func (c *Component) Instance() module.Component {
	return &Component{
		settings: Settings{
			Image:          "python:3.12-slim",
			Interpreter:    "python",
			TimeoutSeconds: defaultTimeoutSeconds,
			MemoryLimit:    "256Mi",
			CPULimit:       "500m",
		},
	}
}

func (c *Component) GetInfo() module.ComponentInfo {
	return module.ComponentInfo{
		Name:        ComponentName,
		Description: "Sandbox Run",
		Info: "Runs a script in a throwaway Kubernetes Job and returns its output and exit code. Built for code an agent wrote: the container runs as non-root with a read-only root filesystem, no service-account token, dropped capabilities, and CPU/memory limits, and is deleted after it finishes. " +
			"A script that exits non-zero is a normal Result with that exit code, not a flow failure — only infrastructure problems reach the Error port. Blocks until the script finishes, so keep it short; the timeout is capped at 240s. " +
			"The script is passed to the interpreter as an argument, so choose an image that contains it (python:3.12-slim, node:22-alpine, busybox). Nothing restricts network egress by default — run it in a namespace with a restrictive NetworkPolicy if the script must not reach the cluster.",
		Tags: []string{"Kubernetes", "Sandbox", "Agent"},
	}
}

func (c *Component) OnClient(k8sClient module.K8sClient) {
	if k8sClient == nil {
		return
	}
	c.clientLock.Lock()
	c.k8sClient = k8sClient.GetK8sClient()
	c.k8sNamespace = k8sClient.GetNamespace()
	c.clientLock.Unlock()

	// Streaming pod logs needs a client-go clientset; the controller-runtime
	// client cannot reach subresources like pods/log.
	if lc, err := k8s.NewLogsClient(); err == nil {
		c.clientLock.Lock()
		c.logsClient = lc
		c.clientLock.Unlock()
	}
}

func (c *Component) OnSettings(_ context.Context, msg any) error {
	in, ok := msg.(Settings)
	if !ok {
		return fmt.Errorf("invalid settings")
	}
	if strings.TrimSpace(in.Image) == "" {
		return fmt.Errorf("image is required")
	}
	if strings.TrimSpace(in.Interpreter) == "" {
		return fmt.Errorf("interpreter is required")
	}
	// Reject an unusable quantity here rather than when a Job is already being
	// built, so a typo surfaces as a node error instead of a runtime failure.
	if err := validateQuantity(in.CPULimit, "cpuLimit"); err != nil {
		return err
	}
	if err := validateQuantity(in.MemoryLimit, "memoryLimit"); err != nil {
		return err
	}
	c.settingsLock.Lock()
	c.settings = in
	c.settingsLock.Unlock()
	return nil
}

func validateQuantity(v, field string) error {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	if _, err := resource.ParseQuantity(v); err != nil {
		return fmt.Errorf("%s %q is not a valid Kubernetes quantity: %w", field, v, err)
	}
	return nil
}

func (c *Component) getSettings() Settings {
	c.settingsLock.RLock()
	defer c.settingsLock.RUnlock()
	return c.settings
}

func (c *Component) Handle(ctx context.Context, handler module.Handler, port string, msg any) module.Result {
	if port != RequestPort {
		return module.Fail(fmt.Errorf("port %s is not supported", port))
	}
	in, ok := msg.(Request)
	if !ok {
		return module.Fail(fmt.Errorf("invalid message"))
	}
	if strings.TrimSpace(in.Script) == "" {
		return c.handleError(ctx, handler, in.Context, module.Permanent(fmt.Errorf("script is required")))
	}

	c.clientLock.RLock()
	k8sClient, ns, logs := c.k8sClient, c.k8sNamespace, c.logsClient
	c.clientLock.RUnlock()
	if k8sClient == nil {
		return c.handleError(ctx, handler, in.Context, module.Retryable(fmt.Errorf("K8s client not available")))
	}

	set := c.getSettings()
	if set.Namespace != "" {
		ns = set.Namespace
	}
	image := set.Image
	if in.Image != "" {
		image = in.Image
	}
	timeout := set.TimeoutSeconds
	if timeout <= 0 {
		timeout = defaultTimeoutSeconds
	}
	if timeout > maxTimeoutSeconds {
		timeout = maxTimeoutSeconds
	}

	job := buildJob(ns, image, set.Interpreter, in.Script, timeout, set.CPULimit, set.MemoryLimit)
	if err := k8sClient.Create(ctx, job); err != nil {
		// The API rejecting the Job is an infrastructure problem — most often
		// missing RBAC or a bad namespace — not something the script can fix.
		return c.handleError(ctx, handler, in.Context, fmt.Errorf("create sandbox job: %w", err))
	}

	// Best-effort removal on the way out. The Job also carries a TTL, so a
	// failure here only means it lingers a few minutes longer.
	defer func() {
		policy := metav1.DeletePropagationBackground
		_ = k8sClient.Delete(context.WithoutCancel(ctx), job, &client.DeleteOptions{PropagationPolicy: &policy})
	}()

	res, err := c.await(ctx, k8sClient, logs, ns, job.Name, timeout)
	if err != nil {
		return c.handleError(ctx, handler, in.Context, err)
	}

	return handler(ctx, ResultPort, Result{
		Context:  in.Context,
		Stdout:   res.stdout,
		ExitCode: res.exitCode,
		Success:  res.exitCode == 0 && !res.timedOut,
		TimedOut: res.timedOut,
		JobName:  job.Name,
	})
}

type outcome struct {
	stdout   string
	exitCode int32
	timedOut bool
}

// await polls the Job to completion and collects what the container produced.
//
// Polling rather than watching: the wait is bounded to a few minutes and ends
// with the hop, so a watch would buy responsiveness we do not need in exchange
// for a connection to manage and leak.
func (c *Component) await(ctx context.Context, k8sClient client.Client, logs *k8s.LogsClient, ns, name string, timeoutSeconds int) (outcome, error) {
	deadline := time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	// Grace beyond the Job's own deadline: Kubernetes needs a moment to mark a
	// deadline-exceeded Job failed, and stopping first would report a timeout
	// as an infrastructure error.
	hardStop := deadline.Add(15 * time.Second)

	for {
		var job batchv1.Job
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, &job); err != nil {
			return outcome{}, module.Retryable(fmt.Errorf("read sandbox job: %w", err))
		}

		if done, timedOut := jobFinished(&job); done {
			code, stdout := c.collect(ctx, k8sClient, logs, ns, name)
			if timedOut {
				// Killed mid-run: there is no exit status, and -1 says so
				// rather than implying a clean failure.
				return outcome{stdout: stdout, exitCode: -1, timedOut: true}, nil
			}
			return outcome{stdout: stdout, exitCode: code}, nil
		}

		select {
		case <-ctx.Done():
			return outcome{}, ctx.Err()
		case <-time.After(pollInterval):
		}

		if time.Now().After(hardStop) {
			// The Job outlived its own activeDeadlineSeconds without being
			// marked failed. Report a timeout with whatever it printed — the
			// deferred delete still stops it.
			_, stdout := c.collect(ctx, k8sClient, logs, ns, name)
			return outcome{stdout: stdout, exitCode: -1, timedOut: true}, nil
		}
	}
}

// jobFinished reports whether the Job reached a terminal state, and whether it
// got there by exceeding its deadline.
func jobFinished(job *batchv1.Job) (done bool, timedOut bool) {
	for _, cond := range job.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		switch cond.Type {
		case batchv1.JobComplete:
			return true, false
		case batchv1.JobFailed:
			// DeadlineExceeded is Kubernetes' own word for "killed by
			// activeDeadlineSeconds".
			return true, cond.Reason == "DeadlineExceeded"
		}
	}
	return false, false
}

// collect reads the exit code and output of the Job's pod. Both are best
// effort: a pod evicted or garbage-collected before this runs leaves the script
// with no verdict, reported as an empty result rather than an error, because
// the run itself did happen.
func (c *Component) collect(ctx context.Context, k8sClient client.Client, logs *k8s.LogsClient, ns, jobName string) (int32, string) {
	pod, err := k8s.FindPodBySelector(ctx, k8sClient, ns, "job-name="+jobName)
	if err != nil || pod == nil {
		return 0, ""
	}

	var code int32
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil {
			code = cs.State.Terminated.ExitCode
			break
		}
	}

	var stdout string
	if logs != nil {
		tail := logTailLines
		if out, err := logs.GetLogs(ctx, ns, pod.Name, &corev1.PodLogOptions{TailLines: &tail}); err == nil {
			stdout = out
		}
	}
	return code, stdout
}

// buildJob assembles the Job. Every field below is a restriction; the container
// gets nothing it is not explicitly handed.
func buildJob(ns, image, interpreter, script string, timeoutSeconds int, cpuLimit, memLimit string) *batchv1.Job {
	var (
		backoffLimit int32 = 0 // never re-run: the script may not be idempotent
		deadline           = int64(timeoutSeconds)
		ttl                = ttlAfterFinished
		nonRoot            = true
		noPrivilege        = false
		readOnlyRoot       = true
		noSAToken          = false
		uid                = sandboxUID
	)

	limits := corev1.ResourceList{}
	if cpuLimit != "" {
		if q, err := resource.ParseQuantity(cpuLimit); err == nil {
			limits[corev1.ResourceCPU] = q
		}
	}
	if memLimit != "" {
		if q, err := resource.ParseQuantity(memLimit); err == nil {
			limits[corev1.ResourceMemory] = q
		}
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "tiny-sandbox-",
			Namespace:    ns,
			Labels:       map[string]string{"app.kubernetes.io/managed-by": "tiny-systems", "tinysystems.io/sandbox": "true"},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"tinysystems.io/sandbox": "true"},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					// Without this the script inherits the pod's service
					// account credentials and can talk to the API server as the
					// module — the single most valuable thing to deny it.
					AutomountServiceAccountToken: &noSAToken,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &nonRoot,
						RunAsUser:    &uid,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:  "sandbox",
						Image: image,
						// The script is an argument, never a file: nothing has
						// to be written, mounted, or cleaned up.
						Command: []string{interpreter, "-c", script},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivilege,
							ReadOnlyRootFilesystem:   &readOnlyRoot,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{Limits: limits},
						// A read-only root still needs somewhere to write:
						// interpreters expect a usable /tmp, and an emptyDir
						// dies with the pod.
						VolumeMounts: []corev1.VolumeMount{{Name: "tmp", MountPath: "/tmp"}},
					}},
					Volumes: []corev1.Volume{{
						Name:         "tmp",
						VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
					}},
				},
			},
		},
	}
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
			// A concrete example so an edge reading $.stdout is checkable when
			// the flow is built rather than resolving to null at runtime.
			Configuration: Result{Stdout: "output", Success: true},
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
