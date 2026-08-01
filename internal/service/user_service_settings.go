package service

import (
	"context"
	"fmt"
	"github.com/shinyes/keer/internal/models"
	"golang.org/x/crypto/bcrypt"
	"strings"
)

func (s *UserService) GetUserEncryptionKey(ctx context.Context, userID int64) (models.UserEncryptionKey, error) {
	return s.store.GetUserEncryptionKeyByUserID(ctx, userID)
}

func (s *UserService) UpsertUserEncryptionKey(
	ctx context.Context,
	userID int64,
	input UpsertUserEncryptionKeyInput,
) (models.UserEncryptionKey, error) {
	encryptionKey, err := normalizeUserEncryptionKeyInput(userID, input)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}
	return s.store.UpsertUserEncryptionKey(ctx, encryptionKey)
}

func normalizeUserEncryptionKeyInput(
	userID int64,
	input UpsertUserEncryptionKeyInput,
) (models.UserEncryptionKey, error) {
	input.KDFAlgorithm = strings.TrimSpace(input.KDFAlgorithm)
	input.KDFSalt = strings.TrimSpace(input.KDFSalt)
	input.WrapAlgorithm = strings.TrimSpace(input.WrapAlgorithm)
	input.WrappedAccountKey = strings.TrimSpace(input.WrappedAccountKey)
	input.SharingPublicKey = strings.TrimSpace(input.SharingPublicKey)
	input.WrappedSharingPrivateKey = strings.TrimSpace(input.WrappedSharingPrivateKey)
	input.Algorithms = strings.TrimSpace(input.Algorithms)
	if input.KeyVersion <= 0 {
		input.KeyVersion = 1
	}
	if userID <= 0 ||
		input.Version != accountMasterKeyRecoveryBundleVersion ||
		input.KDFAlgorithm != accountMasterKeyRecoveryKDFAlgorithm ||
		input.KDFSalt == "" ||
		input.KDFTimeCost <= 0 ||
		input.KDFMemoryKiB <= 0 ||
		input.KDFParallelism <= 0 ||
		input.WrapAlgorithm != accountMasterKeyRecoveryWrapAlgorithm ||
		input.WrappedAccountKey == "" {
		return models.UserEncryptionKey{}, ErrInvalidEncryptionKey
	}
	if (input.SharingPublicKey == "") != (input.WrappedSharingPrivateKey == "") {
		return models.UserEncryptionKey{}, ErrInvalidEncryptionKey
	}

	return models.UserEncryptionKey{
		UserID:                   userID,
		Version:                  input.Version,
		KDFAlgorithm:             input.KDFAlgorithm,
		KDFSalt:                  input.KDFSalt,
		KDFTimeCost:              input.KDFTimeCost,
		KDFMemoryKiB:             input.KDFMemoryKiB,
		KDFParallelism:           input.KDFParallelism,
		WrapAlgorithm:            input.WrapAlgorithm,
		WrappedAccountKey:        input.WrappedAccountKey,
		SharingPublicKey:         input.SharingPublicKey,
		WrappedSharingPrivateKey: input.WrappedSharingPrivateKey,
		KeyVersion:               input.KeyVersion,
		Algorithms:               input.Algorithms,
	}, nil
}

const (
	accountMasterKeyRecoveryBundleVersion = 2
	accountMasterKeyRecoveryKDFAlgorithm  = "ARGON2ID"
	accountMasterKeyRecoveryWrapAlgorithm = "AES_GCM"
)

func (s *UserService) ChangePassword(
	ctx context.Context,
	userID int64,
	currentPassword string,
	newPassword string,
	input UpsertUserEncryptionKeyInput,
) (models.UserEncryptionKey, error) {
	currentPassword = strings.TrimSpace(currentPassword)
	newPassword = strings.TrimSpace(newPassword)
	if userID <= 0 || currentPassword == "" {
		return models.UserEncryptionKey{}, ErrInvalidCurrentPassword
	}
	if newPassword == "" {
		return models.UserEncryptionKey{}, ErrInvalidPassword
	}

	user, err := s.store.GetUserByID(ctx, userID)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}
	if user.PasswordHash == "" {
		return models.UserEncryptionKey{}, ErrInvalidCurrentPassword
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(currentPassword)); err != nil {
		return models.UserEncryptionKey{}, ErrInvalidCurrentPassword
	}

	encryptionKey, err := normalizeUserEncryptionKeyInput(userID, input)
	if err != nil {
		return models.UserEncryptionKey{}, err
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(newPassword), bcrypt.DefaultCost)
	if err != nil {
		return models.UserEncryptionKey{}, fmt.Errorf("hash password: %w", err)
	}

	return s.store.UpdateUserPasswordHashAndEncryptionKey(ctx, userID, string(passwordHash), encryptionKey)
}

func (s *UserService) GetUserGeneralSettings(ctx context.Context, userID int64) (models.UserGeneralSettings, error) {
	return s.store.GetUserGeneralSettings(ctx, userID)
}

func (s *UserService) UpdateUserGeneralSettings(
	ctx context.Context,
	userID int64,
	input UpdateUserGeneralSettingsInput,
) (models.UserGeneralSettings, error) {
	settings, err := normalizeUserGeneralSettingsInput(userID, input)
	if err != nil {
		return models.UserGeneralSettings{}, err
	}
	return s.store.UpdateUserGeneralSettings(ctx, settings)
}

func normalizeUserGeneralSettingsInput(
	userID int64,
	input UpdateUserGeneralSettingsInput,
) (models.UserGeneralSettings, error) {
	if userID <= 0 {
		return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
	}

	visibility := models.Visibility(strings.TrimSpace(input.MemoVisibility))
	gesture := models.MemoEditGesture(strings.TrimSpace(input.MemoEditGesture))
	if !visibility.IsValid() || !gesture.IsValid() {
		return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
	}

	columns := make([]models.MemoColumnConfig, 0, len(input.MemoColumns))
	seenIDs := make(map[string]struct{}, len(input.MemoColumns))
	for _, column := range input.MemoColumns {
		id := strings.TrimSpace(column.ID)
		name := strings.TrimSpace(column.Name)
		if id == "" || name == "" {
			return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
		}
		if _, exists := seenIDs[id]; exists {
			return models.UserGeneralSettings{}, ErrInvalidGeneralSetting
		}
		seenIDs[id] = struct{}{}
		columns = append(columns, models.MemoColumnConfig{
			ID:              id,
			Name:            name,
			RequiredTags:    normalizeMemoColumnTags(column.RequiredTags),
			VisibleInDrawer: column.VisibleInDrawer,
			PinnedMemoNames: normalizeMemoColumnMemoNames(column.PinnedMemoNames),
		})
	}

	return models.UserGeneralSettings{
		UserID:               userID,
		MemoVisibility:       visibility,
		MemoEditGesture:      gesture,
		MemoColumns:          columns,
		ExploreDrawerEntries: normalizeExploreDrawerEntries(input.ExploreDrawerEntries),
	}, nil
}

func normalizeMemoColumnTags(tags []string) []string {
	normalized := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, raw := range tags {
		tag := strings.TrimSpace(raw)
		if tag == "" {
			continue
		}
		if _, exists := seen[tag]; exists {
			continue
		}
		seen[tag] = struct{}{}
		normalized = append(normalized, tag)
	}
	return normalized
}

func normalizeMemoColumnMemoNames(names []string) []string {
	normalized := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

func normalizeExploreDrawerEntries(entries []models.ExploreDrawerEntryConfig) []models.ExploreDrawerEntryConfig {
	normalized := make([]models.ExploreDrawerEntryConfig, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		entryID := strings.TrimSpace(entry.EntryID)
		if entryID == "" {
			continue
		}
		if _, exists := seen[entryID]; exists {
			continue
		}
		seen[entryID] = struct{}{}
		normalized = append(normalized, models.ExploreDrawerEntryConfig{
			EntryID:          entryID,
			VisibleInExplore: entry.VisibleInExplore,
		})
	}
	return normalized
}
