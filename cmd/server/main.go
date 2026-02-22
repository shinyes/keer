package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/shinyes/keer/internal/app"
	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/db"
	"github.com/shinyes/keer/internal/markdown"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/store"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		runServe()
		return
	}

	switch args[0] {
	case "serve":
		runServe()
	case "admin":
		if err := runAdmin(args[1:]); err != nil {
			log.Fatal(err)
		}
	case "memo":
		// Backward-compatible shorthand for: server admin memo ...
		if err := runAdmin(args); err != nil {
			log.Fatal(err)
		}
	case "help", "-h", "--help":
		printUsage()
	default:
		printUsage()
		os.Exit(2)
	}
}

func runServe() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	container, cleanup, err := app.Build(context.Background(), cfg)
	if err != nil {
		log.Fatalf("build app: %v", err)
	}
	defer cleanup() //nolint:errcheck

	log.Printf("keer backend listening on %s (storage=%s)", cfg.Addr, cfg.Storage)
	if cfg.BootstrapToken != "" {
		log.Printf("bootstrap token enabled for user=%s", cfg.BootstrapUser)
	}
	log.Fatal(container.Router.Listen(cfg.Addr))
}

func runAdmin(args []string) error {
	if len(args) < 2 {
		printUsage()
		return fmt.Errorf("invalid admin command")
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	sqliteDB, err := db.OpenSQLite(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer sqliteDB.Close() //nolint:errcheck

	if err := db.Migrate(sqliteDB); err != nil {
		return fmt.Errorf("migrate db: %w", err)
	}

	sqlStore := store.New(sqliteDB)
	md := markdown.NewService()
	memoService := service.NewMemoService(sqlStore, md)

	switch args[0] {
	case "memo":
		switch args[1] {
		case "rebuild-payload":
			count, err := memoService.RebuildAllMemoPayloads(context.Background())
			if err != nil {
				return fmt.Errorf("rebuild payload failed: %w", err)
			}
			fmt.Printf("rebuild complete, updated=%d\n", count)
			return nil
		default:
			printUsage()
			return fmt.Errorf("unknown admin subcommand: %s %s", args[0], args[1])
		}
	default:
		printUsage()
		return fmt.Errorf("unknown admin command: %s", args[0])
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run ./cmd/server")
	fmt.Println("  go run ./cmd/server serve")
	fmt.Println("  go run ./cmd/server admin memo rebuild-payload")
}
