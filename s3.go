package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"strconv"
)

type s3Connection struct {
	client *s3.Client
	name   string
}

func connectS3(ctx context.Context, name string) (*s3Connection, error) {
	c, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration, %w", err)
	}
	client := s3.NewFromConfig(c)
	return &s3Connection{client, name}, nil
}

func (s *s3Connection) head(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.name),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Connection) put(ctx context.Context, i int64, buffer []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(strconv.FormatInt(i, 10)),
		Body:   bytes.NewReader(buffer),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Connection) delete(ctx context.Context, i int64) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(strconv.FormatInt(i, 10)),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *s3Connection) deleteAll(ctx context.Context) error {
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.name),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects, %w", err)
		}
		var objects []types.ObjectIdentifier
		for _, obj := range page.Contents {
			objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
		}
		if len(objects) > 0 {
			_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(s.name),
				Delete: &types.Delete{
					Objects: objects,
					Quiet:   aws.Bool(true),
				},
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}
