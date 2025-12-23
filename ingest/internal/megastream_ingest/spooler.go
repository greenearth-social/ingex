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

// SQLiteRow represents a row of data extracted from a SQLite database
type SQLiteRow struct {
	AtURI          string
	DID            string
	RawPost        string
	Inferences     string
	SourceFilename string
}

// Spooler defines the interface for data source processors that extract SQLiteRow data
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

// LocalSpooler processes SQLite database files from a local directory
type LocalSpooler struct {
	*baseSpooler
	directory string
}

// S3Spooler processes SQLite database files from an Amazon S3 bucket
type S3Spooler struct {
	*baseSpooler
	bucket    string
	prefix    string
	s3Client  *s3.Client
	region    string
	awsConfig aws.Config
}

// NewLocalSpooler creates a new LocalSpooler for processing files from a local directory
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

// NewS3Spooler creates a new S3Spooler for processing files from an Amazon S3 bucket
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

// Start begins processing files in the local directory
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

// GetRowChannel returns the channel that receives SQLiteRow data
func (ls *LocalSpooler) GetRowChannel() <-chan SQLiteRow {
	return ls.rowChan
}

// Stop gracefully stops the LocalSpooler
func (ls *LocalSpooler) Stop() error {
	ls.logger.Info("Stopping local spooler")
	return nil
}

func (ls *LocalSpooler) discoverFiles() ([]string, error) {
	entries, err := os.ReadDir(ls.directory)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Cursor is guaranteed to be set by StateManager
	cursor := ls.stateManager.GetCursor()
	cursorTimeUs := cursor.LastTimeUs
	ls.logger.Debug("Using cursor for file filtering: %d", cursorTimeUs)

	var files []string
	var skippedCount int
	var oldestSkipped, newestSkipped string
	var oldestSkippedTime, newestSkippedTime int64

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
			skippedCount++
			if oldestSkipped == "" || fileTimeUs < oldestSkippedTime {
				oldestSkipped = entry.Name()
				oldestSkippedTime = fileTimeUs
			}
			if newestSkipped == "" || fileTimeUs > newestSkippedTime {
				newestSkipped = entry.Name()
				newestSkippedTime = fileTimeUs
			}
			continue
		}

		files = append(files, entry.Name())
	}

	sort.Strings(files)
	if skippedCount > 0 {
		ls.logger.Info("Skipped %d files before cursor (oldest: %s, newest: %s)", skippedCount, oldestSkipped, newestSkipped)
	}
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
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			ls.logger.Error("Failed to clean up temp directory: %v", err)
		}
	}()

	// Check if file is actually zipped or just a raw SQLite database
	var dbPath string
	if isZipFile(filePath) {
		ls.logger.Debug("File is zipped, extracting %s", filePath)
		dbPath, err = unzipFile(filePath, tmpDir)
		if err != nil {
			return fmt.Errorf("failed to unzip file: %w", err)
		}
		ls.logger.Debug("Successfully unzipped to %s", dbPath)
	} else {
		// File is not zipped, use it directly
		ls.logger.Debug("File is not zipped, using directly: %s", filePath)
		dbPath = filePath
	}

	if err := processDatabase(ctx, dbPath, filename, ls.rowChan, ls.logger); err != nil {
		return fmt.Errorf("failed to process database: %w", err)
	}

	if err := os.Remove(filePath); err != nil {
		ls.logger.Error("Failed to remove processed file %s: %v", filePath, err)
	} else {
		ls.logger.Debug("Cleaned up processed file: %s", filePath)
	}

	return nil
}

// Start begins processing files in the S3 bucket
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

// GetRowChannel returns the channel that receives SQLiteRow data
func (ss *S3Spooler) GetRowChannel() <-chan SQLiteRow {
	return ss.rowChan
}

// Stop gracefully stops the S3Spooler
func (ss *S3Spooler) Stop() error {
	ss.logger.Info("Stopping S3 spooler")
	return nil
}

func (ss *S3Spooler) discoverFiles(ctx context.Context) ([]string, error) {
	// Cursor is guaranteed to be set by StateManager
	cursor := ss.stateManager.GetCursor()
	cursorTimeUs := cursor.LastTimeUs
	ss.logger.Debug("Using cursor for file filtering: %d", cursorTimeUs)

	// Convert cursor timestamp to filename for StartAfter optimization
	startAfterFilename := common.TimestampToMegastreamFilename(cursorTimeUs)
	startAfterKey := ss.prefix + startAfterFilename

	input := &s3.ListObjectsV2Input{
		Bucket:       aws.String(ss.bucket),
		Prefix:       aws.String(ss.prefix),
		StartAfter:   aws.String(startAfterKey),
		RequestPayer: "requester",
	}

	// Paginate through all S3 results
	var allObjects []string
	pageCount := 0
	totalObjects := 0

	for {
		result, err := ss.s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list S3 objects: %w", err)
		}

		pageCount++
		totalObjects += len(result.Contents)

		for _, obj := range result.Contents {
			allObjects = append(allObjects, *obj.Key)
		}

		if !*result.IsTruncated {
			break
		}

		input.ContinuationToken = result.NextContinuationToken
		input.StartAfter = nil // Only use StartAfter on first request
	}

	ss.logger.Info("Retrieved %d objects from S3 across %d page(s)", totalObjects, pageCount)

	// Filter files based on timestamp
	var files []string
	var skippedCount int
	var oldestSkipped, newestSkipped string
	var oldestSkippedTime, newestSkippedTime int64

	for _, key := range allObjects {
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
			skippedCount++
			if oldestSkipped == "" || fileTimeUs < oldestSkippedTime {
				oldestSkipped = filename
				oldestSkippedTime = fileTimeUs
			}
			if newestSkipped == "" || fileTimeUs > newestSkippedTime {
				newestSkipped = filename
				newestSkippedTime = fileTimeUs
			}
			continue
		}

		files = append(files, key)
	}

	sort.Strings(files)
	if skippedCount > 0 {
		ss.logger.Info("Skipped %d files before cursor (oldest: %s, newest: %s)", skippedCount, oldestSkipped, newestSkipped)
	}
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

			// TODO: Move state update to after Elasticsearch indexing is confirmed.
			// mechanism from main thread back to spooler (e.g., via separate ack channel).
			// https://github.com/greenearth-social/ingex/issues/44
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
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			ss.logger.Error("Failed to clean up temp directory: %v", err)
		}
	}()

	zipPath := filepath.Join(tmpDir, filename)
	ss.logger.Debug("Will download %s to %s", key, zipPath)
	if err := ss.downloadFile(ctx, key, zipPath); err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}

	// Check if file is actually zipped or just a raw SQLite database
	var dbPath string
	if isZipFile(zipPath) {
		ss.logger.Debug("File is zipped, extracting %s", zipPath)
		dbPath, err = unzipFile(zipPath, tmpDir)
		if err != nil {
			return fmt.Errorf("failed to unzip file: %w", err)
		}
		ss.logger.Debug("Successfully unzipped to %s", dbPath)
	} else {
		// File is not zipped, use it directly
		ss.logger.Debug("File is not zipped, using directly: %s", zipPath)
		dbPath = zipPath
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
	defer func() {
		if err := result.Body.Close(); err != nil {
			ss.logger.Error("Failed to close S3 response body: %v", err)
		}
	}()

	outFile, err := os.Create(destPath) // nolint:gosec // G304: File path is from earlier disk read, not user input
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}

	bytesWritten, err := io.Copy(outFile, result.Body)
	if err != nil {
		_ = outFile.Close()
		return fmt.Errorf("failed to write file: %w", err)
	}

	if err := outFile.Close(); err != nil {
		return fmt.Errorf("failed to close output file: %w", err)
	}

	// Verify file size matches what we downloaded
	fileInfo, err := os.Stat(destPath)
	if err != nil {
		return fmt.Errorf("failed to stat downloaded file: %w", err)
	}
	if fileInfo.Size() != bytesWritten {
		return fmt.Errorf("file size mismatch: wrote %d bytes but file is %d bytes", bytesWritten, fileInfo.Size())
	}

	// Check file signature to verify it's a valid zip file
	f, err := os.Open(destPath) // nolint:gosec // G304: destPath is created internally, not from user input
	if err != nil {
		return fmt.Errorf("failed to open file for signature check: %w", err)
	}
	header := make([]byte, 4)
	n, _ := f.Read(header)
	_ = f.Close() // Best-effort close for read-only file check

	ss.logger.Debug("Downloaded S3 file to: %s (%d bytes, signature: %x)", destPath, bytesWritten, header[:n])

	return nil
}

// isZipFile checks if a file is a valid ZIP file by examining its signature
func isZipFile(path string) bool {
	f, err := os.Open(path) // nolint:gosec // G304: path is created internally, not from user input
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }() // Best-effort close for read-only file check

	header := make([]byte, 2)
	n, err := f.Read(header)
	if err != nil || n < 2 {
		return false
	}

	// ZIP files start with PK (0x504b)
	return n >= 2 && header[0] == 0x50 && header[1] == 0x4b // nolint:gosec // G602: bounds already checked above
}

func unzipFile(zipPath, destDir string) (string, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", fmt.Errorf("failed to open zip file: %w", err)
	}
	defer func() {
		_ = r.Close() // Ignore error in cleanup
	}()

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

			outFile, err := os.Create(fpath) // nolint:gosec // G304: File path is from zipfile
			if err != nil {
				if closeErr := rc.Close(); closeErr != nil {
					return "", fmt.Errorf("failed to close reader and create output file: %v, %w", closeErr, err)
				}
				return "", fmt.Errorf("failed to create output file: %w", err)
			}

			_, err = io.Copy(outFile, rc) // nolint:gosec // G110: We trust the source of the zip file
			if closeErr := outFile.Close(); closeErr != nil {
				_ = rc.Close() // Best effort close, ignore error as we're already handling an error
				return "", fmt.Errorf("failed to close output file: %w", closeErr)
			}
			if closeErr := rc.Close(); closeErr != nil {
				return "", fmt.Errorf("failed to close reader: %w", closeErr)
			}

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
	defer func() {
		if err := db.Close(); err != nil {
			logger.Error("Failed to close database: %v", err)
		}
	}()

	rows, err := db.QueryContext(ctx, `
		SELECT at_uri, did, raw_post, inferences
		FROM enriched_posts
	`)
	if err != nil {
		return fmt.Errorf("failed to query enriched_posts: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			logger.Error("Failed to close rows: %v", err)
		}
	}()

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
