// Package objstore is the operator's minimal S3 client for conversation GC
// (issue #114 decision 5): it deletes (and probes) conversation transcript
// objects whose brainstorm batch has fully closed. It mirrors the wrapper's
// storage client shape (one client targets AWS S3 and Ceph RGW / MinIO via a
// configurable endpoint + path-style toggle) but exposes only the GC surface.
// Credentials come from the AWS default chain (the operator Deployment mounts
// AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY from the S3 secret, or IRSA on AWS).
package objstore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

// Config is the S3 connection config, sourced from the operator config (the same
// s3* values the wrapper pods get).
type Config struct {
	Endpoint       string
	Bucket         string
	Region         string
	KeyPrefix      string
	ForcePathStyle bool
}

// Enabled reports whether conversation GC is configured (a bucket is set).
func (c Config) Enabled() bool { return c.Bucket != "" }

// Client is the S3-backed conversation-GC client.
type Client struct {
	s3     *s3.Client
	bucket string
	prefix string
}

// New builds an S3 client from cfg using the AWS default credential chain.
// Construction makes no network call. Returns an error when cfg has no bucket.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if !cfg.Enabled() {
		return nil, errors.New("objstore: no bucket configured")
	}
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("objstore: load aws config: %w", err)
	}
	s3c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.ForcePathStyle
	})
	return &Client{s3: s3c, bucket: cfg.Bucket, prefix: cfg.KeyPrefix}, nil
}

func (c *Client) fullKey(key string) string {
	p := strings.Trim(c.prefix, "/")
	k := strings.TrimLeft(key, "/")
	if p == "" {
		return k
	}
	return p + "/" + k
}

// Exists reports whether key is present. A 404 maps to (false, nil).
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	if _, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.fullKey(key)),
	}); err != nil {
		var nf *s3types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NotFound", "NoSuchKey", "404":
				return false, nil
			}
		}
		return false, fmt.Errorf("objstore exists %s: %w", key, err)
	}
	return true, nil
}

// Delete removes the object at key (idempotent; deleting a missing key is fine).
func (c *Client) Delete(ctx context.Context, key string) error {
	if _, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(c.fullKey(key)),
	}); err != nil {
		return fmt.Errorf("objstore delete %s: %w", key, err)
	}
	return nil
}
