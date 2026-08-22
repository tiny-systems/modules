package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tiny-systems/module/cli"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"

	// Import components to register them
	_ "github.com/tiny-systems/modules/kubernetes-module/components/configmappatch"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/customresourcelist"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/daemonsetlist"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/deploymentlist"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/deploymentscale"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/deploymentupdate"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/eventwatcher"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/podcreate"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/poddelete"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/podlist"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/podlogs"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/podlogswatch"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/podstatus"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/podupdate"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/podwatcher"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/sandboxrun"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/secretget"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/servicelist"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/serviceupdate"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/statefulsetlist"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/webhookregister"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/workloadlist"
	_ "github.com/tiny-systems/modules/kubernetes-module/components/workloadrestart"
)

// RootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "tiny-system's kubernetes module",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func main() {
	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	viper.AutomaticEnv()
	if viper.GetBool("debug") {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	// Declare RBAC requirements for kubernetes-module.
	//
	// EnableKubernetesResourceAccess is deliberately OFF. It was a bundle —
	// pods, services, deployments and ingresses, every one with update, cluster
	// wide — and this module reads pods and restarts workloads; it has never
	// written a Service or an Ingress. The bundle handed it the ability to
	// repoint any Service and rewrite any Ingress in the cluster. The reads it
	// actually needs are spelled out below instead.
	//
	// No cluster-wide "*/*" read wildcard either: that granted read of every
	// object including all Secrets. resource_watch / custom_resource_list on
	// arbitrary kinds therefore are not covered here — grant a
	// resource-specific rule (ideally resourceNames-pinned) at install time
	// when a deployment genuinely needs one.
	registry.SetRequirements(module.Requirements{
		RBAC: module.RBACRequirements{
			// Cluster-wide, because these components read across namespaces:
			// pod_list, pod_watch and workload_restart are all pointed at a
			// namespace by the caller, not fixed to the module's own.
			ExtraRules: []module.RBACRule{
				// Reads that replace the disabled bundle — no write verbs.
				{
					APIGroups: []string{""},
					Resources: []string{"pods", "services"},
					Verbs:     []string{"get", "list", "watch"},
				},
				{
					APIGroups: []string{"apps"},
					Resources: []string{"deployments"},
					Verbs:     []string{"get", "list", "watch"},
				},
				{
					APIGroups: []string{"networking.k8s.io"},
					Resources: []string{"ingresses"},
					Verbs:     []string{"get", "list", "watch"},
				},
				// Deployments update — for restart/scale operations.
				{
					APIGroups: []string{"apps"},
					Resources: []string{"deployments"},
					Verbs:     []string{"update", "patch"},
				},
				// StatefulSets and DaemonSets — workload_list and
				// workload_restart treat all three kinds alike.
				{
					APIGroups: []string{"apps"},
					Resources: []string{"statefulsets", "daemonsets"},
					Verbs:     []string{"get", "list", "watch", "update", "patch"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"configmaps"},
					Verbs:     []string{"get", "create", "update", "patch"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"events"},
					Verbs:     []string{"get", "list", "watch"},
				},
				{
					APIGroups: []string{"admissionregistration.k8s.io"},
					Resources: []string{"mutatingwebhookconfigurations"},
					Verbs:     []string{"get", "create", "update", "delete"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"pods/log"},
					Verbs:     []string{"get"},
				},
				{
					APIGroups: []string{""},
					Resources: []string{"pods"},
					Verbs:     []string{"create", "delete"},
				},
				// sandbox_run's unit of work.
				{
					APIGroups: []string{"batch"},
					Resources: []string{"jobs"},
					Verbs:     []string{"create", "get", "list", "watch", "delete"},
				},
			},
			// The module's own namespace only. Reading Secrets cluster-wide to
			// reach the few in the release namespace is the single broadest
			// thing this module could ask for.
			ExtraNamespacedRules: []module.RBACRule{
				{
					APIGroups: []string{""},
					Resources: []string{"secrets"},
					Verbs:     []string{"get", "list"},
				},
			},
		},
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli.RegisterCommands(rootCmd)
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Printf("command execute error: %v\n", err)
	}
}
