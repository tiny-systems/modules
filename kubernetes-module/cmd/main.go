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
  _ "github.com/tiny-systems/modules/kubernetes-module/components/customresourcelist"
  _ "github.com/tiny-systems/modules/kubernetes-module/components/configmappatch"
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
  _ "github.com/tiny-systems/modules/kubernetes-module/components/servicelist"
  _ "github.com/tiny-systems/modules/kubernetes-module/components/serviceupdate"
  _ "github.com/tiny-systems/modules/kubernetes-module/components/statefulsetlist"
  _ "github.com/tiny-systems/modules/kubernetes-module/components/workloadlist"
  _ "github.com/tiny-systems/modules/kubernetes-module/components/secretget"
  _ "github.com/tiny-systems/modules/kubernetes-module/components/webhookregister"
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

	// Declare RBAC requirements for kubernetes-module
	registry.SetRequirements(module.Requirements{
		RBAC: module.RBACRequirements{
			// Enable base K8s resource access (pods, services, configmaps, secrets, etc.)
			EnableKubernetesResourceAccess: true,
			// Additional rules beyond the base access above. Least-privilege:
			// NO cluster-wide "*/*" read wildcard — it granted read of every
			// object incl. all Secrets. resource_watch / custom_resource_list /
			// secret_get on arbitrary resources therefore aren't covered here;
			// scope a resource-specific rule (ideally resourceNames-pinned) when
			// a deployment genuinely needs one, so the ServiceAccount never gets
			// blanket cluster read.
			ExtraRules: []module.RBACRule{
				// Deployments update - for restart/scale operations
				{
					APIGroups: []string{"apps"},
					Resources: []string{"deployments"},
					Verbs:     []string{"update", "patch"},
				},
				// StatefulSets and DaemonSets - workload_list and
				// workload_restart treat all three kinds alike, but the base
				// access flag covers only Deployments.
				{
					APIGroups: []string{"apps"},
					Resources: []string{"statefulsets", "daemonsets"},
					Verbs:     []string{"get", "list", "watch", "update", "patch"},
				},
				// ConfigMaps - for configmap_patch, which reads the ConfigMap
				// before upserting a key.
				{
					APIGroups: []string{""},
					Resources: []string{"configmaps"},
					Verbs:     []string{"get", "create", "update", "patch"},
				},
				// Secrets read - for secret_get. Lost when the cluster-wide
				// read wildcard was dropped for least privilege, which left the
				// component unable to do the one thing it exists for.
				{
					APIGroups: []string{""},
					Resources: []string{"secrets"},
					Verbs:     []string{"get", "list"},
				},
				// Events read - for event_watch, lost the same way.
				{
					APIGroups: []string{""},
					Resources: []string{"events"},
					Verbs:     []string{"get", "list", "watch"},
				},
				// MutatingWebhookConfigurations - for webhook_register component
				{
					APIGroups: []string{"admissionregistration.k8s.io"},
					Resources: []string{"mutatingwebhookconfigurations"},
					Verbs:     []string{"get", "create", "update", "delete"},
				},
				// Pod logs access
				{
					APIGroups: []string{""},
					Resources: []string{"pods/log"},
					Verbs:     []string{"get"},
				},
				// Pod create/delete — for pod_create and pod_delete. The base
				// access flag above grants only get/list/patch/update/watch, so
				// without this both components fail with a 403 the moment they
				// are used on a self-hosted install. The drift gate cannot
				// catch a gap like this: it compares the overlay against the
				// declaration, never the declaration against the API calls the
				// code actually makes.
				{
					APIGroups: []string{""},
					Resources: []string{"pods"},
					Verbs:     []string{"create", "delete"},
				},
				// Jobs — for sandbox_run, which creates one per script and
				// removes it when the script finishes.
				{
					APIGroups: []string{"batch"},
					Resources: []string{"jobs"},
					Verbs:     []string{"create", "get", "list", "watch", "delete"},
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
