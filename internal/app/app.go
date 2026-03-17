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
	if err := sqlStore.PromoteUsersToAdminByUsername(ctx, cfg.AdminUsers); err != nil {
		_ = cleanup()
		return nil, nil, err
	}
	userService := service.NewUserService(sqlStore)
	if err := userService.ConfigureAuth(cfg.JWTSecret, cfg.AccessTokenTTL, cfg.RefreshTokenTTL); err != nil {
		_ = cleanup()
		return nil, nil, err
	}

	memoService := service.NewMemoService(sqlStore)
	groupService := service.NewGroupService(sqlStore)

	localStore, err := storage.NewLocalStore(cfg.UploadsDir)
	if err != nil {
		_ = cleanup()
		return nil, nil, err
	}
	stores := []storage.Store{localStore}
	if cfg.Storage == config.StorageBackendS3 {
		s3Store, err := storage.NewS3Store(ctx, cfg.S3)
		if err != nil {
			_ = cleanup()
			return nil, nil, err
		}
		stores = append(stores, s3Store)
	} else if cfg.S3.Endpoint != "" && cfg.S3.Region != "" && cfg.S3.Bucket != "" && cfg.S3.AccessKeyID != "" && cfg.S3.AccessSecret != "" {
		s3Store, err := storage.NewS3Store(ctx, cfg.S3)
		if err != nil {
			_ = cleanup()
			return nil, nil, err
		}
		stores = append(stores, s3Store)
	}
	storageRouter := storage.NewRouter(storage.NormalizeType(string(cfg.Storage)), stores...)
	if storageRouter.DefaultStore() == nil {
		_ = cleanup()
		return nil, nil, fmt.Errorf("unsupported storage backend %s", cfg.Storage)
	}

	attachmentService := service.NewAttachmentService(sqlStore, storageRouter)
	userService.SetAvatarStorageRouter(storageRouter)
	_ = attachmentService.CleanupExpiredUploadSessions(ctx)
	router := httpserver.NewRouter(cfg, sqlStore, userService, memoService, groupService, attachmentService)

	return &Container{
		Config:            cfg,
		Store:             sqlStore,
		UserService:       userService,
		MemoService:       memoService,
		GroupService:      groupService,
		AttachmentService: attachmentService,
		Router:            router,
	}, cleanup, nil
}
