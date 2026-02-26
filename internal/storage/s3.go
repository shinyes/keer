package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/shinyes/keer/internal/config"
)

var ErrS3MultipartUnsupported = errors.New("s3 multipart upload unsupported")

type S3UploadedPart struct {
	PartNumber int32
	ETag       string
	Size       int64
}

type S3Store struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	bucket        string
}

func NewS3Store(ctx context.Context, cfg config.S3Config) (*S3Store, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.AccessSecret, "")),
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = cfg.UsePathStyle
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})

	return &S3Store{
		client:        client,
		presignClient: s3.NewPresignClient(client),
		bucket:        cfg.Bucket,
	}, nil
}

func (s *S3Store) Put(ctx context.Context, key string, contentType string, data []byte) (int64, error) {
	return s.PutStream(ctx, key, contentType, bytes.NewReader(data), int64(len(data)))
}

func (s *S3Store) PutStream(ctx context.Context, key string, contentType string, reader io.Reader, size int64) (int64, error) {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
		Body:        reader,
	}
	if size >= 0 {
		input.ContentLength = aws.Int64(size)
	}

	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        input.Bucket,
		Key:           input.Key,
		ContentType:   input.ContentType,
		Body:          input.Body,
		ContentLength: input.ContentLength,
	})
	if err != nil {
		return 0, fmt.Errorf("put s3 object: %w", err)
	}
	return size, nil
}

func (s *S3Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get s3 object: %w", err)
	}
	return obj.Body, nil
}

func (s *S3Store) OpenRange(ctx context.Context, key string, start int64, end int64) (io.ReadCloser, error) {
	if start < 0 {
		return nil, fmt.Errorf("invalid range start")
	}
	if end >= 0 && end < start {
		return nil, fmt.Errorf("invalid range end")
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	if start > 0 || end >= 0 {
		if end >= 0 {
			input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, end))
		} else {
			input.Range = aws.String(fmt.Sprintf("bytes=%d-", start))
		}
	}

	obj, err := s.client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get s3 object with range: %w", err)
	}
	return obj.Body, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete s3 object: %w", err)
	}
	return nil
}

func (s *S3Store) HeadSize(ctx context.Context, key string) (int64, error) {
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("head s3 object: %w", err)
	}
	if output.ContentLength == nil {
		return 0, nil
	}
	return *output.ContentLength, nil
}

func (s *S3Store) PresignPutObjectURL(ctx context.Context, key string, contentType string, expires time.Duration) (string, error) {
	if expires <= 0 {
		expires = 15 * time.Minute
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}
	req, err := s.presignClient.PresignPutObject(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign put s3 object: %w", err)
	}
	return req.URL, nil
}

func (s *S3Store) PresignGetObjectURL(ctx context.Context, key string, expires time.Duration) (string, error) {
	if expires <= 0 {
		expires = 5 * time.Minute
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	req, err := s.presignClient.PresignGetObject(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign get s3 object: %w", err)
	}
	return req.URL, nil
}

func (s *S3Store) CreateMultipartUpload(ctx context.Context, key string, contentType string) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		ContentType: aws.String(contentType),
	}
	output, err := s.client.CreateMultipartUpload(ctx, input)
	if err != nil {
		if isMultipartUnsupportedError(err) {
			return "", ErrS3MultipartUnsupported
		}
		return "", fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := strings.TrimSpace(aws.ToString(output.UploadId))
	if uploadID == "" {
		return "", fmt.Errorf("create multipart upload: missing upload id")
	}
	return uploadID, nil
}

func (s *S3Store) PresignUploadPartURL(
	ctx context.Context,
	key string,
	uploadID string,
	partNumber int32,
	expires time.Duration,
) (string, error) {
	if partNumber <= 0 {
		return "", fmt.Errorf("invalid multipart part number")
	}
	if strings.TrimSpace(uploadID) == "" {
		return "", fmt.Errorf("missing multipart upload id")
	}
	if expires <= 0 {
		expires = 15 * time.Minute
	}
	input := &s3.UploadPartInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNumber),
	}
	req, err := s.presignClient.PresignUploadPart(ctx, input, func(options *s3.PresignOptions) {
		options.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign multipart upload part: %w", err)
	}
	return req.URL, nil
}

func (s *S3Store) ListMultipartUploadedParts(ctx context.Context, key string, uploadID string) ([]S3UploadedPart, error) {
	if strings.TrimSpace(uploadID) == "" {
		return nil, fmt.Errorf("missing multipart upload id")
	}
	paginator := s3.NewListPartsPaginator(s.client, &s3.ListPartsInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})

	parts := make([]S3UploadedPart, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list multipart uploaded parts: %w", err)
		}
		for _, part := range page.Parts {
			size := int64(0)
			if part.Size != nil {
				size = *part.Size
			}
			parts = append(parts, S3UploadedPart{
				PartNumber: aws.ToInt32(part.PartNumber),
				ETag:       strings.TrimSpace(aws.ToString(part.ETag)),
				Size:       size,
			})
		}
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	return parts, nil
}

func (s *S3Store) CompleteMultipartUpload(
	ctx context.Context,
	key string,
	uploadID string,
	parts []S3UploadedPart,
) error {
	if strings.TrimSpace(uploadID) == "" {
		return fmt.Errorf("missing multipart upload id")
	}
	if len(parts) == 0 {
		return fmt.Errorf("missing multipart uploaded parts")
	}
	completedParts := make([]types.CompletedPart, 0, len(parts))
	for _, part := range parts {
		if part.PartNumber <= 0 || strings.TrimSpace(part.ETag) == "" {
			return fmt.Errorf("invalid multipart uploaded part")
		}
		etag := strings.TrimSpace(part.ETag)
		partNumber := part.PartNumber
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       aws.String(etag),
			PartNumber: aws.Int32(partNumber),
		})
	}

	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

func (s *S3Store) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	if strings.TrimSpace(uploadID) == "" {
		return nil
	}
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	if err != nil {
		return fmt.Errorf("abort multipart upload: %w", err)
	}
	return nil
}

func isMultipartUnsupportedError(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := strings.ToLower(strings.TrimSpace(apiErr.ErrorCode()))
		if strings.Contains(code, "notimplemented") ||
			strings.Contains(code, "unsupported") ||
			strings.Contains(code, "not_supported") {
			return true
		}
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, "not implemented") || strings.Contains(msg, "unsupported")
}
