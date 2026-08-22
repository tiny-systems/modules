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
	_ "github.com/tiny-systems/storage-module/components/blobget"
	_ "github.com/tiny-systems/storage-module/components/bloblist"
	_ "github.com/tiny-systems/storage-module/components/blobpresign"
	_ "github.com/tiny-systems/storage-module/components/blobput"
)

// RootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "tiny-system's storage module — S3-compatible blob storage (put, get, list, presign)",
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
