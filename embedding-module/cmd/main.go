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
	_ "github.com/tiny-systems/embedding-module/components/embedtext"
	"github.com/tiny-systems/module/cli"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

func init() {
	// Declare the TEI bundle so installing this module also
	// provisions an in-cluster HuggingFace text-embeddings-inference
	// service. The operator chart picks up bundles.tei.enabled=true
	// and renders the curated TEI subchart at <release>-tei. The
	// embed_text component resolves that endpoint at runtime via
	// pkg/bundle.URL("tei") — no install-time env wiring needed.
	registry.SetRequirements(module.Requirements{
		Bundles: module.Bundles{
			module.Bundle{
				Name:           "tei",
				Description:    "HuggingFace text-embeddings-inference. BAAI/bge-small-en-v1.5 default (384 dims, CPU). Override bundles.tei.image.tag for a GPU image.",
				DefaultEnabled: true,
				ConnectionHint: "Auto-discovered via bundle.URL(\"tei\") — http://<release>-tei:80",
				Values: map[string]any{
					// cpu-1.9: cpu-1.5 had a model-download URL bug that
					// crashlooped TEI on first boot ("relative URL without a
					// base"). Set here so the install job's --set overrides the
					// deployed operator chart's tei.image.tag without waiting on
					// an operator-chart + platform bump.
					"image": map[string]any{"tag": "cpu-1.9"},
				},
			},
		},
	})
}

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "Tiny Systems embedding module — in-cluster text embeddings via TEI",
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

func main() {
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
