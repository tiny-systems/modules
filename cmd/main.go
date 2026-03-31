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
  _ "github.com/tiny-systems/kubernetes-module/components/customresourcelist"
  _ "github.com/tiny-systems/kubernetes-module/components/configmappatch"
  _ "github.com/tiny-systems/kubernetes-module/components/daemonsetlist"
  _ "github.com/tiny-systems/kubernetes-module/components/deploymentlist"
  _ "github.com/tiny-systems/kubernetes-module/components/deploymentscale"
  _ "github.com/tiny-systems/kubernetes-module/components/deploymentupdate"
  _ "github.com/tiny-systems/kubernetes-module/components/eventwatcher"
  _ "github.com/tiny-systems/kubernetes-module/components/podcreate"
  _ "github.com/tiny-systems/kubernetes-module/components/poddelete"
  _ "github.com/tiny-systems/kubernetes-module/components/podlist"
  _ "github.com/tiny-systems/kubernetes-module/components/podlogs"
  _ "github.com/tiny-systems/kubernetes-module/components/podlogswatch"
  _ "github.com/tiny-systems/kubernetes-module/components/podstatus"
  _ "github.com/tiny-systems/kubernetes-module/components/podupdate"
  _ "github.com/tiny-systems/kubernetes-module/components/podwatcher"
  _ "github.com/tiny-systems/kubernetes-module/components/servicelist"
  _ "github.com/tiny-systems/kubernetes-module/components/serviceupdate"
  _ "github.com/tiny-systems/kubernetes-module/components/statefulsetlist"
  _ "github.com/tiny-systems/kubernetes-module/components/workloadlist"
  _ "github.com/tiny-systems/kubernetes-module/components/secretget"
  _ "github.com/tiny-systems/kubernetes-module/components/webhookregister"
  _ "github.com/tiny-systems/kubernetes-module/components/workloadrestart"
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
			// Additional rules for cluster-wide resources and CRDs
			ExtraRules: []module.RBACRule{
				// Watch all resources (wildcard) - for ResourceWatcher
				{
					APIGroups: []string{"*"},
					Resources: []string{"*"},
					Verbs:     []string{"get", "list", "watch"},
				},
				// Deployments update - for restart/scale operations
				{
					APIGroups: []string{"apps"},
					Resources: []string{"deployments"},
					Verbs:     []string{"update", "patch"},
				},
				// ConfigMaps write - for configmap_patch component
				{
					APIGroups: []string{""},
					Resources: []string{"configmaps"},
					Verbs:     []string{"create", "update", "patch"},
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
