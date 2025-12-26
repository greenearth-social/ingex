# Test Data

This directory contains sample data files for development and testing purposes. By storing files here, developers can work with realistic data without needing S3 credentials or access to the production data sources.

## Megastream Archive Files

Megastream data is stored as timestamped ZIP files containing SQLite databases with BlueSky post data. Each archive file follows the naming convention:

```text
megastream_YYYYMMDD_HHMMSS.zip
```

For example: `megastream_20241217_143022.zip`

**Note:** Despite the `.zip` extension, some files may actually be uncompressed SQLite database files rather than ZIP archives. The ingestion service handles both formats automatically.

### Downloading a Sample Archive from S3

To download a sample archive file for local development:

1. **Configure AWS credentials** (if not already set up):

   ```bash
   export GE_AWS_S3_ACCESS_KEY="your-access-key"
   export GE_AWS_S3_SECRET_KEY="your-secret-key"
   export GE_AWS_REGION="us-east-1"
   ```

   **Troubleshooting:**

   ```bash
   # Verify which AWS account/credentials are being used
   aws sts get-caller-identity

   # Check if you can access the bucket
   aws s3 ls s3://graze-mega-02/mega/ --region us-east-1 --request-payer requester
   ```

### Alternative: Get a File from Someone with Access

If you don't have S3 permissions, ask a team member who does to download and share a sample file with you. Once you have the file locally, you can skip the S3 steps above.

1. **List available files** to find a recent one:

   ```bash
   aws s3 ls s3://graze-mega-02/mega/ --recursive --region us-east-1 --request-payer requester | tail -20
   ```

2. **Download a specific file**:

   ```bash
   aws s3 cp s3://graze-mega-02/mega/mega_jetstream_20251217_231140.db.zip ./test_data/megastream/ --region us-east-1 --request-payer requester
   ```

3. **Choose a small/recent file**: Look for files that are a reasonable size (a few MB to tens of MB) and relatively recent to ensure they contain valid data in the current schema.

### Using the Sample Archive

Once you have a sample archive in this directory, you can use it with the `megastream_ingest` service:

```bash
# Set the local directory as the source
export GE_LOCAL_SQLITE_DB_PATH="./test_data"

# Run the ingest service in "once" mode
./bin/megastream_ingest --source=local --mode=once
```
