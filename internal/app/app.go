package app

import (
	"context"
	"fmt"

	"github.com/gofiber/fiber/v2"

	"github.com/shinyes/keer/internal/config"
	"github.com/shinyes/keer/internal/db"
	httpserver "github.com/shinyes/keer/internal/http"
	"github.com/shinyes/keer/internal/service"
	"github.com/shinyes/keer/internal/storage"
	"github.com/shinyes/keer/internal/store"
)

type Container struct {
	Config            config.Config
	Store             *store.SQLStore
	UserService       *service.UserService
	StorageService    *service.StorageSettingsService
	MemoService       *service.MemoService
	GroupService      *service.GroupService
	AttachmentService *service.AttachmentService
	Router            *fiber.App
}

func Build(ctx context.Context, cfg config.Config) (*Container, func() error, error) {
	sqliteDB, err := db.OpenSQLite(cfg.DBPath)
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() error {
		return sqliteDB.Close()
	}

	if err := db.Migrate(sqliteDB); err != nil {
		_ = cleanup()
		return nil, nil, err
	}

	sqlStore := store.New(sqliteDB)
	userService := service.NewUserService(sqlStore)
	storageService := service.NewStorageSettingsService(sqlStore)
	resolvedStorage, err := storageService.Resolve(ctx)
	if err != nil {
		_ = cleanup()
		return nil, nil, fmt.Errorf("resolve storage settings: %w", err)
	}
	cfg.Storage = resolvedStorage.Backend
	cfg.S3 = resolvedStorage.S3
	if err := userService.EnsureBootstrap(ctx, cfg.BootstrapUser, cfg.BootstrapToken); err != nil {
		_ = cleanup()
		return nil, nil, fmt.Errorf("bootstrap setup: %w", err)
	}

	memoService := service.NewMemoService(sqlStore)
	groupService := service.NewGroupService(sqlStore)

	var fileStorage storage.Store
	switch cfg.Storage {
	case config.StorageBackendLocal:
		localStore, err := storage.NewLocalStore(cfg.UploadsDir)
		if err != nil {
			_ = cleanup()
			return nil, nil, err
		}
		fileStorage = localStore
	case config.StorageBackendS3:
		s3Store, err := storage.NewS3Store(ctx, cfg.S3)
		if err != nil {
			_ = cleanup()
			return nil, nil, err
		}
		fileStorage = s3Store
	default:
		_ = cleanup()
		return nil, nil, fmt.Errorf("unsupported storage backend %s", cfg.Storage)
	}

	attachmentService := service.NewAttachmentService(sqlStore, fileStorage)
	userService.SetAvatarStorage(fileStorage)
	_ = attachmentService.CleanupExpiredUploadSessions(ctx)
	router := httpserver.NewRouter(cfg, userService, memoService, groupService, attachmentService)

	return &Container{
		Config:            cfg,
		Store:             sqlStore,
		UserService:       userService,
		StorageService:    storageService,
		MemoService:       memoService,
		GroupService:      groupService,
		AttachmentService: attachmentService,
		Router:            router,
	}, cleanup, nil
}
