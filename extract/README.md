# Extract Service

Go-based data extraction service that pulls records from Elasticsearch to send to offline storage.

## Overview

Initial use case for this extraction is to provide training data sets for model development. Targeting a pre-existing file format in parquet files, and will ship them up to a parking area on S3.

## Features

The first export is of posts.

Additional exports to be implemented of likes & other data as needed.

## Known Issues

So many! But an overview:

* Untested on anything but small samples on local
* As a temporary strategy, copied files from ingest/internal/common as needed and added content to them (although generally avoided modifying existing code); need to pick a sharing strategy & then split/merge
* See TODOs throughout the code; lots of features not yet implemented

