package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/shinyes/keer/internal/config"
)

type S3Store struct {
	client *s3.Client
	bucket string
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
		client: client,
		bucket: cfg.Bucket,
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
