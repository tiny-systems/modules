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
	_ "github.com/tiny-systems/database-module/components/postgresexec"
	_ "github.com/tiny-systems/database-module/components/postgresquery"
	_ "github.com/tiny-systems/database-module/components/redisdedup"
	_ "github.com/tiny-systems/database-module/components/redisget"
	_ "github.com/tiny-systems/database-module/components/redisset"
	_ "github.com/tiny-systems/database-module/components/vectorsearch"
	_ "github.com/tiny-systems/database-module/components/vectorupsert"
	"github.com/tiny-systems/module/cli"
	"github.com/tiny-systems/module/module"
	"github.com/tiny-systems/module/registry"
)

func init() {
	// Declare the pgvector bundle so installing this module also
	// provisions an in-cluster pgvector Postgres (the operator chart's
	// curated subchart: pgvector/pgvector:pg16, extension pre-created
	// via initdb, 8Gi PVC). vector_upsert / vector_search resolve it via
	// bundle.PostgresDSN("pgvector") whenever their DSN is left empty —
	// the RAG/memory store works with zero config; an explicit DSN still
	// targets any external database.
	registry.SetRequirements(module.Requirements{
		Bundles: module.Bundles{
			module.Bundle{
				Name:           "pgvector",
				Description:    "In-cluster Postgres with the pgvector extension (pgvector/pgvector:pg16, 8Gi PVC). Backs vector_upsert / vector_search when the DSN is left empty.",
				DefaultEnabled: true,
				ConnectionHint: "Auto-discovered — leave the component DSN empty. Explicit DSNs still work for external databases.",
			},
		},
	})
}

var rootCmd = &cobra.Command{
	Use:   "server",
	Short: "tiny-system's database module — Postgres and Redis components",
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
