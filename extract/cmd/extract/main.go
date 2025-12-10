package main

import (
	"fmt"
	"os"

	//	"github.com/xitongsys/parquet-go/parquet"
	//	"github.com/xitongsys/parquet-go/writer"
	"github.com/parquet-go/parquet-go"

	"extract/internal/common"
)

func main() {

	// TODO Parse command line flags
	// dryRun := flag.Bool("dry-run", false, "Run in dry-run mode (no writes to Elasticsearch)")

	// Load configuration
	config := common.LoadConfig()
	logger := common.NewLogger(config.LoggingEnabled)

	// Validate Elasticsearch configuration
	if config.ElasticsearchURL == "" {
		logger.Error("ELASTICSEARCH_URL environment variable is required")
		os.Exit(1)
	}

	// get connection to ES

	// TODO: validate destination selection

	// set up connection to output parquet file

	// fetch-and-write-loop:
	runPostsExport(config, logger, false, true)

	// close & clean up

}

// for now this is little more than a cast, but later may involve more complex transformations
func esPosts2ParquetPosts(esPosts []common.Hit) ([]common.ExtractPost, error) {
	extractPosts := make([]common.ExtractPost, len(esPosts))

	for i, hit := range esPosts {
		// The PostData in the Hit.Source has most of the fields.
		// We can cast it and then fill in the missing EsID.
		extractPosts[i].AtURI = hit.Source.AtURI
		extractPosts[i].AuthorDID = hit.Source.AuthorDID
		extractPosts[i].Content = hit.Source.Content
		extractPosts[i].CreatedAt = hit.Source.CreatedAt
		extractPosts[i].ThreadRootPost = hit.Source.ThreadRootPost
		extractPosts[i].ThreadParentPost = hit.Source.ThreadParentPost
		extractPosts[i].QuotePost = hit.Source.QuotePost
		extractPosts[i].Embeddings = hit.Source.Embeddings
		extractPosts[i].IndexedAt = hit.Source.IndexedAt
		extractPosts[i].EsID = hit.Source.EsID
		// post := common.ExtractPost(hit.Source)
		//post.EsID = hit.ID
	}
	return extractPosts, nil
}

func runPostsExport(config *common.Config, logger *common.IngestLogger, dryRun, skipTLSVerify bool) {

	logger.Info("Starting export to Parquet (dryRun: %v)", dryRun)

	// Initialize Elasticsearch client

	esConfig := common.ElasticsearchConfig{
		URL:           config.ElasticsearchURL,
		APIKey:        config.ElasticsearchAPIKey,
		SkipTLSVerify: skipTLSVerify,
	}

	esClient, err := common.NewElasticsearchClient(esConfig, logger)
	if err != nil {
		logger.Error("%v", err)
		os.Exit(1)
	} else {
		logger.Info("Connected to ES")
	}

	response, err := common.FetchPosts(esClient, logger, "", "")
	if err != nil {
		logger.Error("Error fetching posts: %v", err)
		os.Exit(1)
	}

	// process result setinto desired format
	// TODO - if I can find a clean way to
	pqPosts, err := esPosts2ParquetPosts(response.Hits.Hits)
	if err != nil {
		logger.Error("Failed to convert ES posts to Parquet posts: %v", err)
	}

	// pull records (arrays/batches of posts) until complete; send each batch to the parquet writer

	var fileno int = 1
	for len(pqPosts) > 0 {
		// write the posts to parquet
		filename := fmt.Sprintf("%s%d%s", "../outfile", fileno, ".parquet")
		logger.Info("output filename is: %s", filename)
		fileno++
		if err := parquet.WriteFile(filename, pqPosts); err != nil {
			logger.Error("Failed to write parquet file: %v", err)
		} else {
			logger.Info("wrote %d records to parquet file", len(pqPosts))
		}

		// TODO right now parquetfile size = fetch size; should decouple those so that we could,
		// for example, fetch 5k at a time but write to parquet files of max 100k size
		// if we do that, we switch parquet write primitives (to placing queryset into a table, not
		// directly write it to a file), and here we would check for desired (max) parquet file size
		// reached and if so, write to & close current file and start a new table/file

		// fetch next batch from ES with updated "after" clause
		lastPost := pqPosts[len(pqPosts)-1]
		response, err := common.FetchPosts(esClient, logger, lastPost.CreatedAt, lastPost.IndexedAt)
		if err != nil {
			logger.Error("Error fetching posts: %v", err)
			os.Exit(1)
		}

		// process result set into desired format
		pqPosts, err = esPosts2ParquetPosts(response.Hits.Hits)
		if err != nil {
			logger.Error("Failed to convert ES posts to Parquet posts: %v", err)
		}
		// TODO remove this! temporary to avoid looping since the after clause isn't made yet
		//pqPosts = nil
	}

	// TODO finish up -- close parquet file, etc
	logger.Info("Export completed")
}

func unused() {
	/* JUNK below here for testing parquet writing

	// write to parquet (via https://github.com/parquet-go/parquet-go)

	type Post struct {
		Did string
		Cid string
	}
	posts := []Post{
		{Did: "did1", Cid: "cid1"},
		{Did: "did2", Cid: "cid2"},
		{Did: "did3", Cid: "cid3"},
	}
	if err := parquet.WriteFile("../outfile.parquet", posts); err != nil {
		logger.Error("Failed again to write parquet file: %v", err)
	}
	// Closing the writer is necessary to flush buffers and write the file footer.
	//if err := writer.Close(); err != nil {
	//	logger.Error("Failed to close parquet writer: %v", err)
	//}

		// write to parquet (via https://github.com/xitongsys/parquet-go/blob/master/example/writer.go)
		f, err := os.Create("../example.parquet")
		if err != nil {
			logger.Error("Failed to create file: %v", err)
		}

		// dummy data
		type Post struct {
			did string `parquet:"name=did, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
			cid string `parquet:"name=cid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
		}

		var posts = []Post{
			{did: "did1", cid: "cid1"},
			{did: "did2", cid: "cid2"},
			{did: "did3", cid: "cid3"},
		}

		pw, err := writer.NewParquetWriterFromWriter(f, new(Post), 4)
		if err != nil {
			logger.Error("Failed to create writer: %v", err)
		}
		pw.RowGroupSize = 128 * 1024 * 1024 //128M
		pw.CompressionType = parquet.CompressionCodec_SNAPPY

			//apost := Post{did: "did1", cid: "cid1"}
			//if err = pw.Write(apost); err != nil {
			//	logger.Error("Write error: %v", err)
			//}
		for i := 0; i < 3; i++ {
			logger.Info("post: %v %v", posts[i].did, posts[i].cid)
			if err = pw.Write(posts[i]); err != nil {
				logger.Error("Write error: %v", err)
			}
		}

		if err = pw.WriteStop(); err != nil {
			logger.Error("WriteStop error: %v", err)
			return
		}	*/

}

func unused_typed_client() {
	/* attempt here to use the typed client, but going back to the regular client for now
	esClient, err := common.NewElasticsearchTypedClient(esConfig, logger)
	if err != nil {
		logger.Error("%v", err)
		os.Exit(1)
	} else {
		logger.Info("Connected to ES")
	}

	// TODO form the proper desired query
	query := `{ "query": { "match_all": {} } }`

	//var ctx = context.Background() // TODO is this the right context to use here?

	// Execute the search query
	res, err := esClient.Search().
	Index("posts").
		esClient.Search.WithBody(strings.NewReader(query)))

	//.WithIndex("posts").WithBody(strings.NewReader(query)).Do(ctx)

	if err != nil {
		logger.Error("Elasticsearch search query failed: %v", err)
		os.Exit(1)
	} else {
		logger.Info("Elasticsearch search query succeeded")
		logger.Info("ES response: %v", res)
		logger.Info("ES response body: %v", res.Body)
	}

	var body Message
	err := json.Unmarshal(res.Hits.Hits, body)
	// add "after" clause to the query for pagination
	*/
}
