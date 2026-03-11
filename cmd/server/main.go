package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shinyes/keer/internal/app"
	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/db"
	"github.com/shinyes/keer/internal/models"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/store"
)

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		runServe([]string{"--console"})
		return
	}

	printUsage()
	log.Fatalf("unsupported args %q, only default startup is allowed", strings.Join(args, " "))
}

func runServe(args []string) {
	serveFlagSet := flag.NewFlagSet("serve", flag.ContinueOnError)
	serveFlagSet.SetOutput(io.Discard)
	consoleMode := serveFlagSet.Bool("console", true, "enable runtime admin console")
	if err := serveFlagSet.Parse(args); err != nil {
		log.Fatalf("parse serve args: %v", err)
	}
	if !*consoleMode {
		log.Fatal("serve must run with --console=true")
	}

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
	if cfg.BootstrapToken != "" {
		log.Printf("bootstrap token enabled for user=%s", cfg.BootstrapUser)
	}
	if *consoleMode {
		log.Printf("runtime admin console enabled")
		go runRuntimeConsole(cfg, container.UserService)
	}
	log.Fatal(container.Router.Listen(container.Config.Addr))
}

func runAdmin(args []string) error {
	if len(args) == 0 {
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
	userService := service.NewUserService(sqlStore)
	return executeAdminCommand(context.Background(), cfg.AllowRegistration, userService, args)
}

func executeAdminCommand(ctx context.Context, allowRegistrationFallback bool, userService *service.UserService, args []string) error {
	switch args[0] {
	case "user":
		return runAdminUser(ctx, userService, args[1:])
	case "token":
		return runAdminToken(ctx, userService, args[1:])
	case "registration":
		return runAdminRegistration(ctx, userService, allowRegistrationFallback, args[1:])
	default:
		printUsage()
		return fmt.Errorf("unknown admin command: %s", args[0])
	}
}

func runRuntimeConsole(cfg config.Config, userService *service.UserService) {
	fmt.Println("Runtime Console: 输入命令，示例：user create demo demo-pass")
	fmt.Println("Runtime Console: 输入 help 查看命令，输入 exit 退出控制台（不会停止服务）")

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("keer> ")
		lineRaw, readErr := reader.ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			fmt.Printf("console read error: %v\n", readErr)
			return
		}
		line := strings.TrimSpace(lineRaw)
		if errors.Is(readErr, io.EOF) && line == "" {
			return
		}
		if line == "" {
			if errors.Is(readErr, io.EOF) {
				return
			}
			continue
		}

		parsed, parseErr := parseCommandLine(line)
		if parseErr != nil {
			fmt.Printf("parse command error: %v\n", parseErr)
			continue
		}
		if len(parsed) == 0 {
			continue
		}

		switch strings.ToLower(parsed[0]) {
		case "help":
			printRuntimeConsoleUsage()
			continue
		case "exit", "quit":
			fmt.Println("runtime console closed")
			return
		case "admin":
			parsed = parsed[1:]
			if len(parsed) == 0 {
				printRuntimeConsoleUsage()
				continue
			}
		}

		if err := executeAdminCommand(context.Background(), cfg.AllowRegistration, userService, parsed); err != nil {
			fmt.Printf("command failed: %v\n", err)
		}
		if errors.Is(readErr, io.EOF) {
			return
		}
	}
}

func runAdminUser(ctx context.Context, userService *service.UserService, args []string) error {
	if len(args) < 3 || args[0] != "create" {
		printUsage()
		return fmt.Errorf("usage: admin user create <username> <password> [display_name] [role]")
	}

	username := strings.TrimSpace(args[1])
	password := strings.TrimSpace(args[2])
	displayName := ""
	if len(args) >= 4 {
		displayName = strings.TrimSpace(args[3])
	}
	role := "USER"
	if len(args) >= 5 {
		role = strings.TrimSpace(args[4])
	}

	admin := &models.User{Role: "ADMIN"}
	user, err := userService.CreateUser(ctx, admin, service.CreateUserInput{
		Username:    username,
		DisplayName: displayName,
		Password:    password,
		Role:        role,
	}, true)
	if err != nil {
		return fmt.Errorf("create user failed: %w", err)
	}
	fmt.Printf("user created: id=%d username=%s role=%s\n", user.ID, user.Username, user.Role)
	return nil
}

func runAdminToken(ctx context.Context, userService *service.UserService, args []string) error {
	if len(args) == 0 {
		printUsage()
		return fmt.Errorf("usage: admin token <create|list|revoke> ...")
	}
	switch args[0] {
	case "create":
		return runAdminTokenCreate(ctx, userService, args[1:])
	case "list":
		return runAdminTokenList(ctx, userService, args[1:])
	case "revoke":
		return runAdminTokenRevoke(ctx, userService, args[1:])
	default:
		printUsage()
		return fmt.Errorf("unknown token subcommand: %s", args[0])
	}
}

func runAdminTokenCreate(ctx context.Context, userService *service.UserService, args []string) error {
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("usage: token create <username_or_id> [description] [--ttl 7d|24h] (default ttl: 7d)")
	}

	identifier := strings.TrimSpace(args[0])
	flagSet := flag.NewFlagSet("admin token create", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	descriptionFlag := flagSet.String("description", "", "token description")
	ttlFlag := flagSet.String("ttl", "", "token ttl, e.g. 24h")
	if err := flagSet.Parse(args[1:]); err != nil {
		return fmt.Errorf("parse token args failed: %w", err)
	}

	description := strings.TrimSpace(*descriptionFlag)
	if description == "" && len(flagSet.Args()) > 0 {
		description = strings.TrimSpace(strings.Join(flagSet.Args(), " "))
	}
	if descriptionFlag != nil && strings.TrimSpace(*descriptionFlag) != "" && len(flagSet.Args()) > 0 {
		return fmt.Errorf("description already set by --description, remove extra positional text")
	}

	ttlRaw := strings.TrimSpace(*ttlFlag)
	expiresAt, err := resolveTokenExpiresAt(ttlRaw, time.Now().UTC())
	if err != nil {
		return err
	}

	user, token, err := userService.CreateAccessTokenForUserWithExpiry(ctx, identifier, description, expiresAt)
	if err != nil {
		if errors.Is(err, service.ErrTokenAlreadyExists) {
			return fmt.Errorf("create token failed: token collision, please retry")
		}
		if errors.Is(err, service.ErrInvalidTokenExpiry) {
			return fmt.Errorf("create token failed: token expiry must be in the future")
		}
		return fmt.Errorf("create token failed: %w", err)
	}
	fmt.Printf("token created: user=%s(%d)\n", user.Username, user.ID)
	fmt.Printf("accessToken=%s\n", token)
	if expiresAt != nil {
		fmt.Printf("expiresAt=%s\n", expiresAt.UTC().Format(time.RFC3339))
	}
	return nil
}

func resolveTokenExpiresAt(ttlRaw string, now time.Time) (*time.Time, error) {
	ttlRaw = strings.TrimSpace(ttlRaw)
	if ttlRaw == "" {
		ttlRaw = "7d"
	}
	ttl, err := parseTTL(ttlRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid --ttl %q: %w", ttlRaw, err)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("--ttl must be greater than 0")
	}
	v := now.UTC().Add(ttl)
	return &v, nil
}

func runAdminTokenList(ctx context.Context, userService *service.UserService, args []string) error {
	identifier, includeAll, err := parseTokenListArgs(args)
	if err != nil {
		printUsage()
		return err
	}
	user, tokens, err := userService.ListAccessTokensForUser(ctx, identifier)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("user not found: %s", identifier)
		}
		return fmt.Errorf("list tokens failed: %w", err)
	}

	filtered := tokens
	if !includeAll {
		filtered = make([]models.PersonalAccessToken, 0, len(tokens))
		for _, token := range tokens {
			if token.RevokedAt == nil {
				filtered = append(filtered, token)
			}
		}
	}

	scope := "active"
	if includeAll {
		scope = "all"
	}
	fmt.Printf("tokens for user=%s(%d), count=%d, scope=%s\n", user.Username, user.ID, len(filtered), scope)
	fmt.Println("id\tprefix\tcreatedAt\texpiresAt\trevokedAt\tlastUsedAt\tdescription")
	for _, token := range filtered {
		fmt.Printf(
			"%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			token.ID,
			token.TokenPrefix,
			token.CreatedAt.UTC().Format(time.RFC3339),
			formatOptionalTime(token.ExpiresAt),
			formatOptionalTime(token.RevokedAt),
			formatOptionalTime(token.LastUsedAt),
			strings.TrimSpace(token.Description),
		)
	}
	return nil
}

func parseTokenListArgs(args []string) (string, bool, error) {
	if len(args) == 0 {
		return "", false, fmt.Errorf("usage: token list <username_or_id> [--all]")
	}

	includeAll := false
	identifier := ""
	for _, arg := range args {
		value := strings.TrimSpace(arg)
		if value == "" {
			continue
		}
		if value == "--all" {
			includeAll = true
			continue
		}
		if strings.HasPrefix(value, "--") {
			return "", false, fmt.Errorf("unknown option: %s", value)
		}
		if identifier == "" {
			identifier = value
			continue
		}
		return "", false, fmt.Errorf("unexpected argument: %s", value)
	}
	if identifier == "" {
		return "", false, fmt.Errorf("usage: token list <username_or_id> [--all]")
	}
	return identifier, includeAll, nil
}

func runAdminTokenRevoke(ctx context.Context, userService *service.UserService, args []string) error {
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("usage: admin token revoke <token_id>")
	}
	tokenID, err := strconv.ParseInt(strings.TrimSpace(args[0]), 10, 64)
	if err != nil || tokenID <= 0 {
		return fmt.Errorf("invalid token_id: %s", args[0])
	}

	token, err := userService.RevokeAccessTokenByID(ctx, tokenID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("token not found: %d", tokenID)
		}
		if errors.Is(err, service.ErrTokenAlreadyRevoked) {
			fmt.Printf("token already revoked: id=%d revokedAt=%s\n", tokenID, formatOptionalTime(token.RevokedAt))
			return nil
		}
		return fmt.Errorf("revoke token failed: %w", err)
	}
	fmt.Printf("token revoked: id=%d user_id=%d revokedAt=%s\n", token.ID, token.UserID, formatOptionalTime(token.RevokedAt))
	return nil
}

func runAdminRegistration(ctx context.Context, userService *service.UserService, fallback bool, args []string) error {
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("usage: admin registration <status|enable|disable>")
	}
	switch args[0] {
	case "status":
		allow, err := userService.ResolveAllowRegistration(ctx, fallback)
		if err != nil {
			return fmt.Errorf("read registration setting failed: %w", err)
		}
		fmt.Printf("allow_registration=%t\n", allow)
		return nil
	case "enable":
		if err := userService.SetAllowRegistration(ctx, true); err != nil {
			return fmt.Errorf("enable registration failed: %w", err)
		}
		fmt.Println("allow_registration=true")
		return nil
	case "disable":
		if err := userService.SetAllowRegistration(ctx, false); err != nil {
			return fmt.Errorf("disable registration failed: %w", err)
		}
		fmt.Println("allow_registration=false")
		return nil
	default:
		printUsage()
		return fmt.Errorf("unknown registration subcommand: %s", args[0])
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  go run ./cmd/server")
	fmt.Println("Note: no subcommands are allowed. Runtime console is always enabled.")
	fmt.Println("Note: use runtime console commands for admin operations.")
}

func printRuntimeConsoleUsage() {
	fmt.Println("Runtime Console Commands:")
	fmt.Println("  user create <username> <password> [display_name] [role]")
	fmt.Println("  token create <username_or_id> [description] [--ttl 7d|24h]  # default ttl=7d")
	fmt.Println("  token list <username_or_id> [--all]")
	fmt.Println("  token revoke <token_id>")
	fmt.Println("  registration status|enable|disable")
	fmt.Println("  help")
	fmt.Println("  exit")
}

func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func parseTTL(raw string) (time.Duration, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return 0, fmt.Errorf("empty ttl")
	}

	if d, err := time.ParseDuration(normalized); err == nil {
		return d, nil
	}

	for _, suffix := range []string{"days", "day", "d"} {
		if !strings.HasSuffix(normalized, suffix) {
			continue
		}
		dayPart := strings.TrimSpace(strings.TrimSuffix(normalized, suffix))
		if dayPart == "" {
			return 0, fmt.Errorf("invalid day ttl")
		}
		days, err := strconv.ParseFloat(dayPart, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid day ttl")
		}
		if days <= 0 {
			return 0, fmt.Errorf("day ttl must be greater than 0")
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}

	return 0, fmt.Errorf("unsupported ttl format")
}

func parseCommandLine(input string) ([]string, error) {
	var args []string
	var current strings.Builder
	var quote rune

	for _, r := range input {
		switch r {
		case '\'', '"':
			if quote == 0 {
				// Treat quotes as wrappers only at token start.
				// Mid-token quotes like cyk'slife are parsed as literals.
				if current.Len() == 0 {
					quote = r
					continue
				}
				current.WriteRune(r)
				continue
			}
			if quote == r {
				quote = 0
				continue
			}
			current.WriteRune(r)
		case ' ', '\t':
			if quote != 0 {
				current.WriteRune(r)
				continue
			}
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args, nil
}
