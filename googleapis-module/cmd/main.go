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
	_ "github.com/tiny-systems/googleapis-module/components/auth/exchange-code"
	_ "github.com/tiny-systems/googleapis-module/components/auth/get-url"
	_ "github.com/tiny-systems/googleapis-module/components/calendar/channel-stop"
	_ "github.com/tiny-systems/googleapis-module/components/calendar/channel-watch"
	_ "github.com/tiny-systems/googleapis-module/components/calendar/get-calendars"
	_ "github.com/tiny-systems/googleapis-module/components/calendar/get-events"
	_ "github.com/tiny-systems/googleapis-module/components/calendar/response-event"
	_ "github.com/tiny-systems/googleapis-module/components/dynamic-client"
	_ "github.com/tiny-systems/googleapis-module/components/firestore/create-doc"
	_ "github.com/tiny-systems/googleapis-module/components/firestore/delete-doc"
	_ "github.com/tiny-systems/googleapis-module/components/firestore/get-docs"
	_ "github.com/tiny-systems/googleapis-module/components/firestore/listen-collection"
	_ "github.com/tiny-systems/googleapis-module/components/firestore/update-doc"
	_ "github.com/tiny-systems/googleapis-module/components/firestore/update-doc-field"
	"github.com/tiny-systems/module/cli"
)

// RootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "tiny-system's googleapis module",
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
