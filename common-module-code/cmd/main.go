package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "github.com/tiny-systems/common-module/components/arrayfilter"
	_ "github.com/tiny-systems/common-module/components/arrayget"
	_ "github.com/tiny-systems/common-module/components/async"
	_ "github.com/tiny-systems/common-module/components/budgetguard"
	_ "github.com/tiny-systems/common-module/components/chat"
	_ "github.com/tiny-systems/common-module/components/collect"
	_ "github.com/tiny-systems/common-module/components/cron"
	_ "github.com/tiny-systems/common-module/components/debug"
	_ "github.com/tiny-systems/common-module/components/delay"
	_ "github.com/tiny-systems/common-module/components/display"
	_ "github.com/tiny-systems/common-module/components/flowtelemetry"
	_ "github.com/tiny-systems/common-module/components/form"
	_ "github.com/tiny-systems/common-module/components/groupby"
	_ "github.com/tiny-systems/common-module/components/inject"
	_ "github.com/tiny-systems/common-module/components/kv"
	_ "github.com/tiny-systems/common-module/components/modify"
	_ "github.com/tiny-systems/common-module/components/retry"
	_ "github.com/tiny-systems/common-module/components/router"
	_ "github.com/tiny-systems/common-module/components/runstart"
	_ "github.com/tiny-systems/common-module/components/runstatus"
	_ "github.com/tiny-systems/common-module/components/signal"
	_ "github.com/tiny-systems/common-module/components/split"
	_ "github.com/tiny-systems/common-module/components/ticker"
	"github.com/tiny-systems/module/cli"
	"os"
	"os/signal"
	"syscall"
)

// RootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "tiny-system's common module",
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli.RegisterCommands(rootCmd)
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Printf("command execute error: %v\n", err)
	}
}
