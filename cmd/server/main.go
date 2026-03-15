package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/shinyes/keer/internal/app"
	"github.com/shinyes/keer/internal/config"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		runServe()
		return
	}

	printUsage()
	log.Fatalf("unsupported args %q, only default startup is allowed", strings.Join(args, " "))
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

	log.Printf("keer backend listening on %s (storage=%s)", container.Config.Addr, container.Config.Storage)
	log.Fatal(container.Router.Listen(container.Config.Addr))
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run ./cmd/server")
}
