package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "github.com/tiny-systems/encoding-module/components/csv/decode"
	_ "github.com/tiny-systems/encoding-module/components/csv/encode"
	_ "github.com/tiny-systems/encoding-module/components/gotemplate"
	_ "github.com/tiny-systems/encoding-module/components/json/decode"
	_ "github.com/tiny-systems/encoding-module/components/json/encode"
	_ "github.com/tiny-systems/encoding-module/components/jwt/encode"
	_ "github.com/tiny-systems/encoding-module/components/jwt/verify"
	_ "github.com/tiny-systems/encoding-module/components/textchunk"
	_ "github.com/tiny-systems/encoding-module/components/xml/encode"
	"github.com/tiny-systems/module/cli"
	"os"
	"os/signal"
	"syscall"
)

// RootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "tiny-system's encoding module",
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
