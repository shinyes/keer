package service

import (
	"bytes"
	"context"
	"image"
	"image/jpeg"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/shinyes/keer/internal/models"

	_ "image/gif"
	_ "image/png"
)

const (
	thumbnailMaxDimension  = 640
	thumbnailJPEGQuality   = 80
	thumbnailMaxSourceSize = 40 * 1024 * 1024
	thumbnailUploadMaxSize = 8 * 1024 * 1024
	thumbnailContentType   = "image/jpeg"
)

var thumbnailImageExtensions = map[string]struct{}{
	".jpg":  {},
	".jpeg": {},
	".png":  {},
	".gif":  {},
	".bmp":  {},
	".webp": {},
	".heic": {},
	".heif": {},
	".avif": {},
}

func thumbnailStorageKey(storageKey string) string {
	storageKey = strings.TrimSpace(storageKey)
	if storageKey == "" {
		return ""
	}
	return storageKey + ".thumb.jpg"
}

func shouldGenerateThumbnail(contentType string, filename string) bool {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(contentType)), "image/") {
		return true
	}
	_, ok := thumbnailImageExtensions[strings.ToLower(filepath.Ext(strings.TrimSpace(filename)))]
	return ok
}

func buildThumbnailFilename(filename string) string {
	trimmed := strings.TrimSpace(filename)
	ext := filepath.Ext(trimmed)
	base := strings.TrimSuffix(trimmed, ext)
	if strings.TrimSpace(base) == "" {
		base = "image"
	}
	return base + "_thumb.jpg"
}

func (s *AttachmentService) copyThumbnailMetadataFromExisting(
	ctx context.Context,
	attachmentID int64,
	existing models.Attachment,
) {
	if existing.ThumbnailStorageKey == "" || existing.ThumbnailSize <= 0 {
		return
	}
	thumbnailFilename := existing.ThumbnailFilename
	if strings.TrimSpace(thumbnailFilename) == "" {
		thumbnailFilename = buildThumbnailFilename(existing.Filename)
	}
	thumbnailType := existing.ThumbnailType
	if strings.TrimSpace(thumbnailType) == "" {
		thumbnailType = thumbnailContentType
	}
	thumbnailStorageType := existing.ThumbnailStorageType
	if strings.TrimSpace(thumbnailStorageType) == "" {
		thumbnailStorageType = existing.StorageType
	}
	_ = s.store.UpdateAttachmentThumbnail(
		ctx,
		attachmentID,
		thumbnailFilename,
		thumbnailType,
		existing.ThumbnailSize,
		thumbnailStorageType,
		existing.ThumbnailStorageKey,
	)
}

func (s *AttachmentService) ensureThumbnailFromBytes(
	ctx context.Context,
	attachment models.Attachment,
	contentType string,
	filename string,
	data []byte,
) {
	if !shouldGenerateThumbnail(contentType, filename) {
		return
	}
	if len(data) == 0 || len(data) > thumbnailMaxSourceSize {
		return
	}
	thumbnailData, err := buildThumbnailJPEG(bytes.NewReader(data))
	if err != nil || len(thumbnailData) == 0 {
		return
	}
	thumbnailKey := thumbnailStorageKey(attachment.StorageKey)
	if thumbnailKey == "" {
		return
	}
	thumbnailSize, err := s.storage.Put(ctx, thumbnailKey, thumbnailContentType, thumbnailData)
	if err != nil || thumbnailSize <= 0 {
		return
	}
	_ = s.store.UpdateAttachmentThumbnail(
		ctx,
		attachment.ID,
		buildThumbnailFilename(filename),
		thumbnailContentType,
		thumbnailSize,
		storageTypeName(s.storage),
		thumbnailKey,
	)
}

func (s *AttachmentService) ensureThumbnailFromFile(
	ctx context.Context,
	attachment models.Attachment,
	contentType string,
	filename string,
	path string,
) {
	if !shouldGenerateThumbnail(contentType, filename) {
		return
	}
	stat, err := os.Stat(path)
	if err != nil {
		return
	}
	if stat.Size() <= 0 || stat.Size() > thumbnailMaxSourceSize {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	thumbnailData, err := buildThumbnailJPEG(f)
	if err != nil || len(thumbnailData) == 0 {
		return
	}
	thumbnailKey := thumbnailStorageKey(attachment.StorageKey)
	if thumbnailKey == "" {
		return
	}
	thumbnailSize, err := s.storage.Put(ctx, thumbnailKey, thumbnailContentType, thumbnailData)
	if err != nil || thumbnailSize <= 0 {
		return
	}
	_ = s.store.UpdateAttachmentThumbnail(
		ctx,
		attachment.ID,
		buildThumbnailFilename(filename),
		thumbnailContentType,
		thumbnailSize,
		storageTypeName(s.storage),
		thumbnailKey,
	)
}

func (s *AttachmentService) ensureThumbnailFromUploadSession(
	ctx context.Context,
	attachment models.Attachment,
	contentType string,
	filename string,
	path string,
) {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return
	}
	stat, err := os.Stat(trimmedPath)
	if err != nil {
		return
	}
	if stat.Size() <= 0 || stat.Size() > thumbnailUploadMaxSize {
		return
	}
	thumbnailType := strings.TrimSpace(contentType)
	if thumbnailType == "" {
		thumbnailType = thumbnailContentType
	}
	thumbnailFilename := sanitizeFilename(filename)
	if thumbnailFilename == "" {
		thumbnailFilename = buildThumbnailFilename(attachment.Filename)
	}
	f, err := os.Open(trimmedPath)
	if err != nil {
		return
	}
	defer f.Close()

	thumbnailKey := thumbnailStorageKey(attachment.StorageKey)
	if thumbnailKey == "" {
		return
	}
	thumbnailSize, err := s.storage.PutStream(ctx, thumbnailKey, thumbnailType, f, stat.Size())
	if err != nil || thumbnailSize <= 0 {
		return
	}
	_ = s.store.UpdateAttachmentThumbnail(
		ctx,
		attachment.ID,
		thumbnailFilename,
		thumbnailType,
		thumbnailSize,
		storageTypeName(s.storage),
		thumbnailKey,
	)
}

func buildThumbnailJPEG(reader io.Reader) ([]byte, error) {
	src, _, err := image.Decode(reader)
	if err != nil {
		return nil, err
	}

	resized := resizeImageNearest(src, thumbnailMaxDimension, thumbnailMaxDimension)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, resized, &jpeg.Options{Quality: thumbnailJPEGQuality}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func resizeImageNearest(src image.Image, maxWidth int, maxHeight int) image.Image {
	bounds := src.Bounds()
	srcWidth := bounds.Dx()
	srcHeight := bounds.Dy()
	if srcWidth <= 0 || srcHeight <= 0 {
		return src
	}
	if srcWidth <= maxWidth && srcHeight <= maxHeight {
		return src
	}

	scale := math.Min(
		float64(maxWidth)/float64(srcWidth),
		float64(maxHeight)/float64(srcHeight),
	)
	dstWidth := int(math.Max(1, math.Round(float64(srcWidth)*scale)))
	dstHeight := int(math.Max(1, math.Round(float64(srcHeight)*scale)))

	dst := image.NewRGBA(image.Rect(0, 0, dstWidth, dstHeight))
	for y := 0; y < dstHeight; y++ {
		srcY := bounds.Min.Y + y*srcHeight/dstHeight
		for x := 0; x < dstWidth; x++ {
			srcX := bounds.Min.X + x*srcWidth/dstWidth
			dst.Set(x, y, src.At(srcX, srcY))
		}
	}
	return dst
}
