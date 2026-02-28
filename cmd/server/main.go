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
		go runRuntimeConsole(cfg, container.UserService, container.StorageService)
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
	storageService := service.NewStorageSettingsService(sqlStore)
	return executeAdminCommand(context.Background(), cfg.AllowRegistration, userService, storageService, args, os.Stdin)
}

func executeAdminCommand(ctx context.Context, allowRegistrationFallback bool, userService *service.UserService, storageService *service.StorageSettingsService, args []string, interactiveInput io.Reader) error {
	switch args[0] {
	case "user":
		return runAdminUser(ctx, userService, args[1:])
	case "token":
		return runAdminToken(ctx, userService, args[1:])
	case "registration":
		return runAdminRegistration(ctx, userService, allowRegistrationFallback, args[1:])
	case "storage":
		return runAdminStorage(ctx, storageService, args[1:], interactiveInput)
	default:
		printUsage()
		return fmt.Errorf("unknown admin command: %s", args[0])
	}
}

func runRuntimeConsole(cfg config.Config, userService *service.UserService, storageService *service.StorageSettingsService) {
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

		if err := executeAdminCommand(context.Background(), cfg.AllowRegistration, userService, storageService, parsed, reader); err != nil {
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

func runAdminStorage(ctx context.Context, storageService *service.StorageSettingsService, args []string, interactiveInput io.Reader) error {
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("usage: admin storage <status|set-local|set-s3|wizard>")
	}

	switch args[0] {
	case "status":
		resolved, err := storageService.Resolve(ctx)
		if err != nil {
			return fmt.Errorf("read storage setting failed: %w", err)
		}
		fmt.Printf("storage_backend=%s\n", resolved.Backend)
		if resolved.Backend == config.StorageBackendS3 {
			fmt.Printf("storage_s3_endpoint=%s\n", resolved.S3.Endpoint)
			fmt.Printf("storage_s3_region=%s\n", resolved.S3.Region)
			fmt.Printf("storage_s3_bucket=%s\n", resolved.S3.Bucket)
			fmt.Printf("storage_s3_access_key_id=%s\n", maskSecret(resolved.S3.AccessKeyID))
			fmt.Printf("storage_s3_access_key_secret=%s\n", maskSecret(resolved.S3.AccessSecret))
			fmt.Printf("storage_s3_use_path_style=%t\n", resolved.S3.UsePathStyle)
		}
		return nil
	case "set-local":
		if err := storageService.SetLocal(ctx); err != nil {
			return fmt.Errorf("set storage backend local failed: %w", err)
		}
		fmt.Println("storage_backend=local")
		fmt.Println("note: restart server to apply storage backend change")
		return nil
	case "set-s3":
		return runAdminStorageSetS3(ctx, storageService, args[1:], interactiveInput)
	case "wizard":
		return runAdminStorageWizard(ctx, storageService, interactiveInput)
	default:
		printUsage()
		return fmt.Errorf("unknown storage subcommand: %s", args[0])
	}
}

func runAdminStorageSetS3(ctx context.Context, storageService *service.StorageSettingsService, args []string, interactiveInput io.Reader) error {
	flagSet := flag.NewFlagSet("admin storage set-s3", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	endpoint := flagSet.String("endpoint", "", "S3 endpoint")
	region := flagSet.String("region", "", "S3 region")
	bucket := flagSet.String("bucket", "", "S3 bucket")
	accessKeyID := flagSet.String("access-key-id", "", "S3 access key id")
	accessKeySecret := flagSet.String("access-key-secret", "", "S3 access key secret")
	usePathStyleRaw := flagSet.String("use-path-style", "", "S3 use path style (true/false), default true")
	interactiveMode := flagSet.Bool("interactive", false, "interactive prompt for S3 settings")
	if err := flagSet.Parse(args); err != nil {
		return fmt.Errorf("parse storage args failed: %w", err)
	}
	if len(flagSet.Args()) > 0 {
		return fmt.Errorf("unexpected positional args: %s", strings.Join(flagSet.Args(), " "))
	}

	usePathStyle := true
	usePathStyleSet := false
	if strings.TrimSpace(*usePathStyleRaw) != "" {
		parsed, ok := parseBoolInput(*usePathStyleRaw)
		if !ok {
			return fmt.Errorf("invalid --use-path-style %q, expected true/false", *usePathStyleRaw)
		}
		usePathStyle = parsed
		usePathStyleSet = true
	}

	seed := config.S3Config{
		Endpoint:     strings.TrimSpace(*endpoint),
		Region:       strings.TrimSpace(*region),
		Bucket:       strings.TrimSpace(*bucket),
		AccessKeyID:  strings.TrimSpace(*accessKeyID),
		AccessSecret: strings.TrimSpace(*accessKeySecret),
		UsePathStyle: usePathStyle,
	}

	if *interactiveMode {
		return runAdminStorageSetS3Interactive(ctx, storageService, seed, usePathStyleSet, interactiveInput)
	}

	if err := storageService.SetS3(ctx, seed); err != nil {
		return fmt.Errorf("set storage backend s3 failed: %w", err)
	}

	fmt.Println("storage_backend=s3")
	fmt.Println("note: restart server to apply storage backend change")
	return nil
}

func runAdminStorageWizard(ctx context.Context, storageService *service.StorageSettingsService, interactiveInput io.Reader) error {
	return runAdminStorageSetS3Interactive(ctx, storageService, config.S3Config{}, false, interactiveInput)
}

func runAdminStorageSetS3Interactive(ctx context.Context, storageService *service.StorageSettingsService, seed config.S3Config, usePathStyleSeeded bool, interactiveInput io.Reader) error {
	if interactiveInput == nil {
		return fmt.Errorf("interactive input is not available")
	}

	defaults := config.S3Config{
		Region:       "auto",
		UsePathStyle: true,
	}
	if resolved, err := storageService.Resolve(ctx); err == nil && resolved.Backend == config.StorageBackendS3 {
		defaults = resolved.S3
	}

	if seed.Endpoint != "" {
		defaults.Endpoint = seed.Endpoint
	}
	if seed.Region != "" {
		defaults.Region = seed.Region
	}
	if seed.Bucket != "" {
		defaults.Bucket = seed.Bucket
	}
	if seed.AccessKeyID != "" {
		defaults.AccessKeyID = seed.AccessKeyID
	}
	if seed.AccessSecret != "" {
		defaults.AccessSecret = seed.AccessSecret
	}
	if usePathStyleSeeded {
		defaults.UsePathStyle = seed.UsePathStyle
	}

	fmt.Println("S3 configuration wizard (values will be saved into database)")
	cfg, err := collectInteractiveS3Config(interactiveInput, os.Stdout, defaults)
	if err != nil {
		return fmt.Errorf("interactive input failed: %w", err)
	}
	if err := storageService.SetS3(ctx, cfg); err != nil {
		return fmt.Errorf("set storage backend s3 failed: %w", err)
	}
	fmt.Println("storage_backend=s3")
	fmt.Println("note: restart server to apply storage backend change")
	return nil
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
	fmt.Println("  storage status|set-local|set-s3 ...|wizard")
	fmt.Println("  help")
	fmt.Println("  exit")
}

func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func maskSecret(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "-"
	}
	if len(raw) <= 4 {
		return "****"
	}
	return raw[:2] + "****" + raw[len(raw)-2:]
}

func collectInteractiveS3Config(input io.Reader, output io.Writer, defaults config.S3Config) (config.S3Config, error) {
	if input == nil {
		return config.S3Config{}, fmt.Errorf("interactive input is required")
	}
	if output == nil {
		output = io.Discard
	}
	reader := bufio.NewReader(input)

	endpoint, err := promptRequiredString(reader, output, "S3 endpoint", defaults.Endpoint)
	if err != nil {
		return config.S3Config{}, err
	}
	region, err := promptRequiredString(reader, output, "S3 region", defaults.Region)
	if err != nil {
		return config.S3Config{}, err
	}
	bucket, err := promptRequiredString(reader, output, "S3 bucket", defaults.Bucket)
	if err != nil {
		return config.S3Config{}, err
	}
	accessKeyID, err := promptRequiredString(reader, output, "S3 access key id", defaults.AccessKeyID)
	if err != nil {
		return config.S3Config{}, err
	}
	accessSecret, err := promptSecretString(reader, output, defaults.AccessSecret)
	if err != nil {
		return config.S3Config{}, err
	}
	usePathStyle, err := promptBoolString(reader, output, "S3 use path style", defaults.UsePathStyle)
	if err != nil {
		return config.S3Config{}, err
	}

	return config.S3Config{
		Endpoint:     endpoint,
		Region:       region,
		Bucket:       bucket,
		AccessKeyID:  accessKeyID,
		AccessSecret: accessSecret,
		UsePathStyle: usePathStyle,
	}, nil
}

func promptRequiredString(reader *bufio.Reader, output io.Writer, label string, defaultValue string) (string, error) {
	for {
		if strings.TrimSpace(defaultValue) == "" {
			fmt.Fprintf(output, "%s: ", label)
		} else {
			fmt.Fprintf(output, "%s [%s]: ", label, defaultValue)
		}

		raw, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
		value := strings.TrimSpace(raw)
		if value == "" {
			value = strings.TrimSpace(defaultValue)
		}
		if value != "" {
			return value, nil
		}

		fmt.Fprintln(output, "value cannot be empty")
		if errors.Is(err, io.EOF) {
			return "", fmt.Errorf("%s cannot be empty", label)
		}
	}
}

func promptSecretString(reader *bufio.Reader, output io.Writer, currentValue string) (string, error) {
	hasCurrent := strings.TrimSpace(currentValue) != ""
	for {
		if hasCurrent {
			fmt.Fprint(output, "S3 access key secret [leave empty to keep current]: ")
		} else {
			fmt.Fprint(output, "S3 access key secret: ")
		}

		raw, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
		value := strings.TrimSpace(raw)
		if value != "" {
			return value, nil
		}
		if hasCurrent {
			return strings.TrimSpace(currentValue), nil
		}

		fmt.Fprintln(output, "value cannot be empty")
		if errors.Is(err, io.EOF) {
			return "", fmt.Errorf("S3 access key secret cannot be empty")
		}
	}
}

func promptBoolString(reader *bufio.Reader, output io.Writer, label string, defaultValue bool) (bool, error) {
	for {
		fmt.Fprintf(output, "%s [true/false] (%t): ", label, defaultValue)
		raw, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return false, err
		}
		value := strings.TrimSpace(raw)
		if value == "" {
			return defaultValue, nil
		}
		parsed, ok := parseBoolInput(value)
		if ok {
			return parsed, nil
		}

		fmt.Fprintln(output, "invalid value, expected true/false")
		if errors.Is(err, io.EOF) {
			return false, fmt.Errorf("%s expects true or false", label)
		}
	}
}

func parseBoolInput(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "t", "true", "y", "yes", "on":
		return true, true
	case "0", "f", "false", "n", "no", "off":
		return false, true
	default:
		return false, false
	}
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
