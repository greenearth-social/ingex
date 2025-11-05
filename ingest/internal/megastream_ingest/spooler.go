package megastream_ingest

import (
	"archive/zip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/greenearth/ingest/internal/common"

	_ "modernc.org/sqlite"
)

type SQLiteRow struct {
	AtURI          string
	DID            string
	RawPost        string
	Inferences     string
	SourceFilename string
}

type Spooler interface {
	Start(ctx context.Context) error
	GetRowChannel() <-chan SQLiteRow
	Stop() error
}

type baseSpooler struct {
	rowChan      chan SQLiteRow
	stateManager *common.StateManager
	logger       *common.IngestLogger
	mode         string
	interval     time.Duration
}

type LocalSpooler struct {
	*baseSpooler
	directory string
}

type S3Spooler struct {
	*baseSpooler
	bucket    string
	prefix    string
	s3Client  *s3.Client
	region    string
	awsConfig aws.Config
}

func NewLocalSpooler(directory string, mode string, interval time.Duration, stateManager *common.StateManager, logger *common.IngestLogger) *LocalSpooler {
	return &LocalSpooler{
		baseSpooler: &baseSpooler{
			rowChan:      make(chan SQLiteRow, 1000),
			stateManager: stateManager,
			logger:       logger,
			mode:         mode,
			interval:     interval,
		},
		directory: directory,
	}
}

func NewS3Spooler(bucket, prefix, region, accessKey, secretKey string, mode string, interval time.Duration, stateManager *common.StateManager, logger *common.IngestLogger) (*S3Spooler, error) {
	var cfg aws.Config
	var err error

	if accessKey != "" && secretKey != "" {
		cfg, err = config.LoadDefaultConfig(
			context.Background(),
			config.WithRegion(region),
			config.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretKey,
				}, nil
			})),
		)
	} else {
		cfg, err = config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Spooler{
		baseSpooler: &baseSpooler{
			rowChan:      make(chan SQLiteRow, 1000),
			stateManager: stateManager,
			logger:       logger,
			mode:         mode,
			interval:     interval,
		},
		bucket:    bucket,
		prefix:    prefix,
		s3Client:  client,
		region:    region,
		awsConfig: cfg,
	}, nil
}

func (ls *LocalSpooler) Start(ctx context.Context) error {
	ls.logger.Info("Starting local spooler in %s mode (directory: %s)", ls.mode, ls.directory)

	go func() {
		defer close(ls.rowChan)

		for {
			files, err := ls.discoverFiles()
			if err != nil {
				ls.logger.Error("Failed to discover files: %v", err)
			} else {
				ls.processFiles(ctx, files)
			}

			if ls.mode == "once" {
				ls.logger.Info("Single run complete, exiting spooler")
				return
			}

			select {
			case <-ctx.Done():
				ls.logger.Info("Context cancelled, stopping spooler")
				return
			case <-time.After(ls.interval):
			}
		}
	}()

	return nil
}

func (ls *LocalSpooler) GetRowChannel() <-chan SQLiteRow {
	return ls.rowChan
}

func (ls *LocalSpooler) Stop() error {
	ls.logger.Info("Stopping local spooler")
	return nil
}

func (ls *LocalSpooler) discoverFiles() ([]string, error) {
	entries, err := os.ReadDir(ls.directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	cursor := ls.stateManager.GetCursor()
	var cursorTimeUs int64
	if cursor != nil {
		cursorTimeUs = cursor.LastTimeUs
		ls.logger.Debug("Using cursor for file filtering: %d", cursorTimeUs)
	} else {
		cursorTimeUs = time.Now().UnixMicro()
		ls.logger.Debug("No cursor found, filtering from current time: %d", cursorTimeUs)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".db.zip") {
			continue
		}

		fileTimeUs, err := common.ParseMegastreamFilenameTimestamp(entry.Name())
		if err != nil {
			ls.logger.Error("Skipping file with invalid filename format: %s (%v)", entry.Name(), err)
			continue
		}

		if fileTimeUs <= cursorTimeUs {
			ls.logger.Debug("Skipping file before or at cursor: %s (file_time: %d, cursor: %d)", entry.Name(), fileTimeUs, cursorTimeUs)
			continue
		}

		files = append(files, entry.Name())
	}

	sort.Strings(files)
	ls.logger.Info("Discovered %d unprocessed files", len(files))
	return files, nil
}

func (ls *LocalSpooler) processFiles(ctx context.Context, files []string) {
	for _, filename := range files {
		select {
		case <-ctx.Done():
			ls.logger.Info("Context cancelled during file processing")
			return
		default:
		}

		filePath := filepath.Join(ls.directory, filename)
		ls.logger.Info("Processing file: %s", filename)

		if err := ls.processFile(ctx, filePath, filename); err != nil {
			ls.logger.Error("Failed to process file %s: %v", filename, err)
		} else {
			fileTimeUs, err := common.ParseMegastreamFilenameTimestamp(filename)
			if err != nil {
				ls.logger.Error("Failed to parse filename timestamp for cursor update: %s (%v)", filename, err)
				continue
			}

			if err := ls.stateManager.UpdateCursor(fileTimeUs); err != nil {
				ls.logger.Error("Failed to update cursor for file %s: %v", filename, err)
			} else {
				ls.logger.Debug("Updated cursor to %d after processing file: %s", fileTimeUs, filename)
			}
		}
	}
}

func (ls *LocalSpooler) processFile(ctx context.Context, filePath, filename string) error {
	tmpDir, err := os.MkdirTemp("", "ingest-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath, err := unzipFile(filePath, tmpDir)
	if err != nil {
		return fmt.Errorf("failed to unzip file: %w", err)
	}

	if err := processDatabase(ctx, dbPath, filename, ls.rowChan, ls.logger); err != nil {
		return fmt.Errorf("failed to process database: %w", err)
	}

	if err := os.Remove(filePath); err != nil {
		ls.logger.Error("Failed to remove zip file %s: %v", filePath, err)
	} else {
		ls.logger.Debug("Cleaned up zip file: %s", filePath)
	}

	return nil
}

func (ss *S3Spooler) Start(ctx context.Context) error {
	ss.logger.Info("Starting S3 spooler in %s mode (bucket: %s, prefix: %s)", ss.mode, ss.bucket, ss.prefix)

	go func() {
		defer close(ss.rowChan)

		for {
			files, err := ss.discoverFiles(ctx)
			if err != nil {
				ss.logger.Error("Failed to discover files: %v", err)
			} else {
				ss.processFiles(ctx, files)
			}

			if ss.mode == "once" {
				ss.logger.Info("Single run complete, exiting spooler")
				return
			}

			select {
			case <-ctx.Done():
				ss.logger.Info("Context cancelled, stopping spooler")
				return
			case <-time.After(ss.interval):
			}
		}
	}()

	return nil
}

func (ss *S3Spooler) GetRowChannel() <-chan SQLiteRow {
	return ss.rowChan
}

func (ss *S3Spooler) Stop() error {
	ss.logger.Info("Stopping S3 spooler")
	return nil
}

func (ss *S3Spooler) discoverFiles(ctx context.Context) ([]string, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:       aws.String(ss.bucket),
		Prefix:       aws.String(ss.prefix),
		RequestPayer: "requester",
	}

	result, err := ss.s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 objects: %w", err)
	}

	cursor := ss.stateManager.GetCursor()
	var cursorTimeUs int64
	if cursor != nil {
		cursorTimeUs = cursor.LastTimeUs
		ss.logger.Debug("Using cursor for file filtering: %d", cursorTimeUs)
	} else {
		cursorTimeUs = time.Now().UnixMicro()
		ss.logger.Debug("No cursor found, filtering from current time: %d", cursorTimeUs)
	}

	var files []string
	for _, obj := range result.Contents {
		key := *obj.Key
		filename := filepath.Base(key)

		if !strings.HasSuffix(filename, ".db.zip") {
			continue
		}

		fileTimeUs, err := common.ParseMegastreamFilenameTimestamp(filename)
		if err != nil {
			ss.logger.Error("Skipping file with invalid filename format: %s (%v)", filename, err)
			continue
		}

		if fileTimeUs <= cursorTimeUs {
			ss.logger.Debug("Skipping file before or at cursor: %s (file_time: %d, cursor: %d)", filename, fileTimeUs, cursorTimeUs)
			continue
		}

		files = append(files, key)
	}

	sort.Strings(files)
	ss.logger.Info("Discovered %d unprocessed files in S3", len(files))
	return files, nil
}

func (ss *S3Spooler) processFiles(ctx context.Context, keys []string) {
	for _, key := range keys {
		select {
		case <-ctx.Done():
			ss.logger.Info("Context cancelled during file processing")
			return
		default:
		}

		filename := filepath.Base(key)
		ss.logger.Info("Processing S3 file: %s", key)

		if err := ss.processFile(ctx, key, filename); err != nil {
			ss.logger.Error("Failed to process S3 file %s: %v", key, err)
		} else {
			fileTimeUs, err := common.ParseMegastreamFilenameTimestamp(filename)
			if err != nil {
				ss.logger.Error("Failed to parse filename timestamp for cursor update: %s (%v)", filename, err)
				continue
			}

			if err := ss.stateManager.UpdateCursor(fileTimeUs); err != nil {
				ss.logger.Error("Failed to update cursor for file %s: %v", filename, err)
			} else {
				ss.logger.Debug("Updated cursor to %d after processing file: %s", fileTimeUs, filename)
			}
		}
	}
}

func (ss *S3Spooler) processFile(ctx context.Context, key, filename string) error {
	tmpDir, err := os.MkdirTemp("", "ingest-s3-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	zipPath := filepath.Join(tmpDir, filename)
	if err := ss.downloadFile(ctx, key, zipPath); err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	dbPath, err := unzipFile(zipPath, tmpDir)
	if err != nil {
		return fmt.Errorf("failed to unzip file: %w", err)
	}

	if err := processDatabase(ctx, dbPath, filename, ss.rowChan, ss.logger); err != nil {
		return fmt.Errorf("failed to process database: %w", err)
	}

	return nil
}

func (ss *S3Spooler) downloadFile(ctx context.Context, key, destPath string) error {
	input := &s3.GetObjectInput{
		Bucket:       aws.String(ss.bucket),
		Key:          aws.String(key),
		RequestPayer: "requester",
	}

	result, err := ss.s3Client.GetObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get S3 object: %w", err)
	}
	defer result.Body.Close()

	outFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, result.Body); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	ss.logger.Debug("Downloaded S3 file to: %s", destPath)
	return nil
}

func unzipFile(zipPath, destDir string) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", fmt.Errorf("failed to open zip file: %w", err)
	}
	defer r.Close()

	if len(r.File) == 0 {
		return "", fmt.Errorf("zip file is empty")
	}

	var dbPath string
	for _, f := range r.File {
		if strings.HasSuffix(f.Name, ".db") {
			fpath := filepath.Join(destDir, filepath.Base(f.Name))

			rc, err := f.Open()
			if err != nil {
				return "", fmt.Errorf("failed to open file in zip: %w", err)
			}

			outFile, err := os.Create(fpath)
			if err != nil {
				rc.Close()
				return "", fmt.Errorf("failed to create output file: %w", err)
			}

			_, err = io.Copy(outFile, rc)
			outFile.Close()
			rc.Close()

			if err != nil {
				return "", fmt.Errorf("failed to extract file: %w", err)
			}

			dbPath = fpath
			break
		}
	}

	if dbPath == "" {
		return "", fmt.Errorf("no .db file found in zip archive")
	}

	return dbPath, nil
}

func processDatabase(ctx context.Context, dbPath, filename string, rowChan chan<- SQLiteRow, logger *common.IngestLogger) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open SQLite database: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, `
		SELECT at_uri, did, raw_post, inferences
		FROM enriched_posts
	`)
	if err != nil {
		return fmt.Errorf("failed to query enriched_posts: %w", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during database processing")
		default:
		}

		var atURI, did, rawPost, inferences string
		if err := rows.Scan(&atURI, &did, &rawPost, &inferences); err != nil {
			logger.Error("Failed to scan row from %s: %v", filename, err)
			continue
		}

		rowChan <- SQLiteRow{
			AtURI:          atURI,
			DID:            did,
			RawPost:        rawPost,
			Inferences:     inferences,
			SourceFilename: filename,
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	logger.Info("Queued %d rows from %s", rowCount, filename)
	return nil
}
