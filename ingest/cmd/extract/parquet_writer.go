package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/greenearth/ingest/internal/common"
	"github.com/parquet-go/parquet-go"
)

type parquetWriterState[T any] struct {
	writer               *parquet.GenericWriter[T]
	recordsInCurrentFile int64
	lastRecordTimestamp  string

	gcsClient    *storage.Client
	gcsWriter    *storage.Writer
	localFile    *os.File
	tempFilePath string
	isGCS        bool
	gcsBucket    string
	gcsPrefix    string
}

func openParquetWriterForPosts(
	ctx context.Context,
	basePath string,
	isGCS bool,
	gcsClient *storage.Client,
	gcsBucket, gcsPrefix, indexName string,
	logger *common.IngestLogger,
) (*parquetWriterState[common.ExtractPost], error) {
	state := &parquetWriterState[common.ExtractPost]{
		isGCS:     isGCS,
		gcsClient: gcsClient,
		gcsBucket: gcsBucket,
		gcsPrefix: gcsPrefix,
	}

	if isGCS {
		filename := fmt.Sprintf("tmp_bsky_posts_%d.parquet", os.Getpid())
		fullPath := gcsPrefix + filename

		logger.Info("Opening GCS parquet writer: gs://%s/%s", gcsBucket, fullPath)

		obj := gcsClient.Bucket(gcsBucket).Object(fullPath)
		state.gcsWriter = obj.NewWriter(ctx)
		state.writer = parquet.NewGenericWriter[common.ExtractPost](state.gcsWriter)
		state.tempFilePath = fullPath
	} else {
		tmpFile, err := os.CreateTemp(basePath, "bsky_posts_*.parquet.tmp")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}

		logger.Info("Opening local parquet writer: %s", tmpFile.Name())

		state.localFile = tmpFile
		state.tempFilePath = tmpFile.Name()
		state.writer = parquet.NewGenericWriter[common.ExtractPost](tmpFile)
	}

	return state, nil
}

func openParquetWriterForLikes(
	ctx context.Context,
	basePath string,
	isGCS bool,
	gcsClient *storage.Client,
	gcsBucket, gcsPrefix, indexName string,
	logger *common.IngestLogger,
) (*parquetWriterState[common.ExtractLike], error) {
	state := &parquetWriterState[common.ExtractLike]{
		isGCS:     isGCS,
		gcsClient: gcsClient,
		gcsBucket: gcsBucket,
		gcsPrefix: gcsPrefix,
	}

	if isGCS {
		filename := fmt.Sprintf("tmp_bsky_likes_%d.parquet", os.Getpid())
		fullPath := gcsPrefix + filename

		logger.Info("Opening GCS parquet writer: gs://%s/%s", gcsBucket, fullPath)

		obj := gcsClient.Bucket(gcsBucket).Object(fullPath)
		state.gcsWriter = obj.NewWriter(ctx)
		state.writer = parquet.NewGenericWriter[common.ExtractLike](state.gcsWriter)
		state.tempFilePath = fullPath
	} else {
		tmpFile, err := os.CreateTemp(basePath, "bsky_likes_*.parquet.tmp")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}

		logger.Info("Opening local parquet writer: %s", tmpFile.Name())

		state.localFile = tmpFile
		state.tempFilePath = tmpFile.Name()
		state.writer = parquet.NewGenericWriter[common.ExtractLike](tmpFile)
	}

	return state, nil
}

func (s *parquetWriterState[T]) writeChunk(chunk []T, logger *common.IngestLogger) error {
	if len(chunk) == 0 {
		return nil
	}

	n, err := s.writer.Write(chunk)
	if err != nil {
		return fmt.Errorf("failed to write %d records: %w", len(chunk), err)
	}

	s.recordsInCurrentFile += int64(n)
	logger.Info("Wrote chunk of %d records (total in file: %d)", n, s.recordsInCurrentFile)

	return nil
}

func (s *parquetWriterState[T]) close(
	ctx context.Context,
	basePath string,
	indexName string,
	logger *common.IngestLogger,
) error {
	if s.writer == nil {
		return nil
	}

	if err := s.writer.Close(); err != nil {
		s.cleanup(logger)
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	if s.isGCS {
		if err := s.gcsWriter.Close(); err != nil {
			return fmt.Errorf("failed to finalize GCS upload: %w", err)
		}

		finalFilename := generateFilename(indexName, s.lastRecordTimestamp, logger)
		finalPath := s.gcsPrefix + finalFilename

		srcObj := s.gcsClient.Bucket(s.gcsBucket).Object(s.tempFilePath)
		dstObj := s.gcsClient.Bucket(s.gcsBucket).Object(finalPath)

		if _, err := dstObj.CopierFrom(srcObj).Run(ctx); err != nil {
			return fmt.Errorf("failed to rename GCS object: %w", err)
		}

		if err := srcObj.Delete(ctx); err != nil {
			logger.Error("Failed to delete temp GCS object %s: %v", s.tempFilePath, err)
		}

		logger.Info("Successfully wrote %d records to gs://%s/%s", s.recordsInCurrentFile, s.gcsBucket, finalPath)
	} else {
		if err := s.localFile.Close(); err != nil {
			logger.Error("Failed to close file handle: %v", err)
		}

		finalFilename := generateFilename(indexName, s.lastRecordTimestamp, logger)
		finalPath := filepath.Join(basePath, finalFilename)

		if err := os.Rename(s.tempFilePath, finalPath); err != nil {
			s.cleanup(logger)
			return fmt.Errorf("failed to rename file: %w", err)
		}

		logger.Info("Successfully wrote %d records to %s", s.recordsInCurrentFile, finalPath)
	}

	return nil
}

func (s *parquetWriterState[T]) cleanup(logger *common.IngestLogger) {
	if s.localFile != nil {
		if err := s.localFile.Close(); err != nil {
			logger.Error("Failed to close file handle: %v", err)
		}
		if s.tempFilePath != "" {
			if err := os.Remove(s.tempFilePath); err != nil {
				logger.Error("Failed to remove temp file %s: %v", s.tempFilePath, err)
			}
		}
	}

	if s.gcsWriter != nil {
		if err := s.gcsWriter.Close(); err != nil {
			logger.Error("failed to close GCS writer: %v", err)
		}
	}
}
