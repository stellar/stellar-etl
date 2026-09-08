# Developing

Welcome to stellar-etl. These instructions cover building, running, and testing
the ETL locally.

For what stellar-etl is and a reference for every export command, read the
[README](README.md). If you plan to open a pull request, also read the
[contributing guidelines](CONTRIBUTING.md).

## Table of Contents

- [Requirements](#requirements)
- [Get the code](#get-the-code)
- [Building locally](#building-locally)
- [Running locally](#running-locally)
- [Dependencies](#dependencies)
- [Running tests](#running-tests)
- [Linting](#linting)
- [Logging](#logging)
- [Adding a new command](#adding-a-new-command)
- [Tips and notes](#tips-and-notes)

## Requirements

To check out, build, and run stellar-etl you need:

- [Git](https://git-scm.com/downloads)
- [Go](https://golang.org/dl) at the version pinned in [go.mod](go.mod)
- [Docker](https://www.docker.com/get-started) and Docker Compose, to build the
  image and to run the integration tests

Some workflows need extra tooling:

- The [gcloud CLI](https://cloud.google.com/sdk/docs/install), for reading
  TxMeta files from a GCS datastore and for running the integration tests.
- [Stellar Core](https://github.com/stellar/stellar-core/blob/master/INSTALL.md)
  v20.0.0 or later, only if you pass `--captive-core` instead of reading from a
  datastore.

Make sure your Go bin directory is on your `PATH`:

```sh
export PATH=$PATH:$(go env GOPATH)/bin
```

## Get the code

Check the code out anywhere; a `GOPATH` is not required.

```sh
git clone https://github.com/stellar/stellar-etl
cd stellar-etl
```

## Building locally

Build the binary:

```sh
go build
```

Build the Docker image. The `docker-build` target tags the image with the
9-character short SHA of `HEAD` as well as `latest`:

```sh
make docker-build
```

Override the tag with `ETLHASH` if you need a specific one:

```sh
ETLHASH=stellar/stellar-etl:my-tag make docker-build
```

## Running locally

Run a command straight from the local build:

```sh
./stellar-etl export_ledgers --start-ledger 1000 --end-ledger 500000 \
  --output exported_ledgers.txt
```

Or run it inside the image you just built:

```sh
docker run --platform linux/amd64 -it stellar/stellar-etl:latest /bin/bash
root@71890b878fca:/etl/data# stellar-etl export_ledgers --start-ledger 1000 \
  --end-ledger 500000 --output exported_ledgers.txt
```

### GCP credentials

The export commands read compressed `LedgerCloseMetaBatch` XDR files from a GCS
datastore, so they need application default credentials:

```sh
gcloud auth login
gcloud config set project <your gcp project>
gcloud auth application-default login
```

To pass those credentials into a container, mount them and point
`GOOGLE_APPLICATION_CREDENTIALS` at the mount:

```sh
docker run --platform linux/amd64 -it \
  -e GOOGLE_APPLICATION_CREDENTIALS=/.config/gcp/credentials.json \
  -v "$HOME/.config/gcloud/application_default_credentials.json":/.config/gcp/credentials.json:ro \
  stellar/stellar-etl:latest /bin/bash
```

## Dependencies

stellar-etl uses [Go modules](https://go.dev/ref/mod). Add a dependency by
importing it and running any Go command, or pin a version explicitly:

```sh
go get <importpath>@<version>
```

Run `go mod tidy` before opening a pull request so `go.mod` and `go.sum` stay
tidy.

> _*Note:*_ The `internal` directory builds separately in CI
> (`cd internal && go build ./...`), so a dependency change that only compiles
> from the repository root can still fail the `internal` job.

## Running tests

### Unit tests

Unit tests cover the transform layer and the helpers in `internal`, and need no
credentials:

```sh
# All transform tests
go test -v -cover ./internal/transform

# A single test
go test -v -run ^TestTransformAsset$ ./internal/transform
```

### Integration tests

The tests in `cmd` run each CLI command end to end against real TxMeta files in
GCS, so they run inside Docker Compose and need GCP credentials:

```sh
# All integration tests
make int-test

# All integration tests, regenerating golden files
make int-test-update
```

Both targets wrap `docker-compose`, mounting your application default
credentials and the `testdata` directory into the container:

```sh
docker-compose build
docker-compose run \
  -v $(HOME)/.config/gcloud/application_default_credentials.json:/usr/credential.json:ro \
  -v $(PWD)/testdata:/usr/src/etl/testdata \
  -e GOOGLE_APPLICATION_CREDENTIALS=/usr/credential.json \
  integration-tests \
  go test -v ./cmd -timeout 30m
```

Run a single integration test by passing `-run` through to `go test`:

```sh
docker-compose run \
  -v $(HOME)/.config/gcloud/application_default_credentials.json:/usr/credential.json:ro \
  -v $(PWD)/testdata:/usr/src/etl/testdata \
  -e GOOGLE_APPLICATION_CREDENTIALS=/usr/credential.json \
  integration-tests \
  go test -v -run ^TestExportAssets$ ./cmd -timeout 30m
```

### Golden files

The integration tests compare command output against golden files in
`testdata/`. When you intentionally change an export's output, regenerate them
with `make int-test-update` (or `-args -update=true`) and **review the diff** —
the golden files are the reference for what lands in BigQuery, so an unexpected
change there is a real finding, not noise.

> _*Note:*_ Golden files for commands that legitimately return no rows are a
> single newline, and `.pre-commit-config.yaml` excludes them from the
> `end-of-file-fixer` and `trailing-whitespace` hooks.

### Coverage

CI runs `./cmd` and `./internal/transform` together and fails if total coverage
drops below **55%**.

## Linting

Install the hooks once, then let them run on every commit:

```sh
pre-commit install
```

Run them over the whole repository the way CI does:

```sh
make lint
```

This runs [golangci-lint](https://golangci-lint.run/) (`gofmt`, `goimports`,
`importas`, `misspell`) plus `prettier` over JSON, YAML, and Markdown files and
a handful of hygiene checks. CI only lints the files a pull request touches.

## Logging

Commands log through `utils.EtlLogger`, a thin wrapper around
[`support/log`](https://github.com/stellar/go-stellar-sdk/tree/master/support/log)
declared in `internal/utils/logger.go`. `cmd/root.go` creates the single
`cmdLogger` that every command uses.

The important behavior is `LogError`, which branches on the logger's
`StrictExport` field:

- `StrictExport = true` (the `--strict-export` flag, which
  `utils.AddCommonFlags` defaults to `true`) → `Fatal`: the command logs the
  error and exits non-zero.
- `StrictExport = false` → `Error`: the command logs the error, **keeps going,
  and still exits 0**.

So a run with `--strict-export=false` can drop rows and still look green. When
you add a new code path, use `cmdLogger.LogError` rather than `Error` or
`Fatal` directly so it honors that flag, and check logs for `level=error` when a
run's row counts look low.

## Adding a new command

To add a new export, add these files:

- `cmd/export_new_data_structure.go`
  - Generate the skeleton with `cobra add {command}`.
  - This file parses flags, creates output files, gets the transformed data from
    the input package, and exports it.
- `cmd/export_new_data_structure_test.go`
  - `runCLI` does most of the heavy lifting; the test supplies the command
    arguments and the expected output.
  - Test data goes in `testdata/new_data_structure/`.
- `internal/input/new_data_structure.go`
  - Extracts the new data structure from wherever it lives: the history
    archives, the bucket list, a captive core instance, or a datastore.
  - Captive core work needs to happen in the background: one set of methods
    exports batches of data onto a channel, another reads from the channel and
    transforms the data for export.
- `internal/transform/new_data_structure.go`
  - Transforms the extracted data into a shape suitable for BigQuery. The struct
    definition belongs in `internal/transform/schema.go`.

A good number of common helpers already exist in `internal/utils`.

> _*Note:*_ A struct in `schema.go` is not enough on its own. A new export also
> needs its Parquet counterpart in `internal/transform/schema_parquet.go` and a
> conversion in `internal/transform/parquet_converter.go`, plus a matching
> schema JSON in
> [stellar-etl-airflow](https://github.com/stellar/stellar-etl-airflow) before
> the data can land in BigQuery.

## Tips and notes

- Every command accepts `-h`, which prints its usage and flags.
- Commands read from mainnet by default; `--testnet` and `--futurenet` switch
  networks. A command can only read from one network per run, and setting both
  flags falls back to testnet.
- Prefer processing large ledger ranges over many small ones. With
  `--captive-core`, each run pays the Stellar Core catch-up cost, which grows as
  the network does.
- Recommended pod resources for running captive core in Kubernetes:
  `{cpu: 3.5, memory: 20Gi, ephemeral-storage: 12Gi}`.
- `--batch-size` for `export_ledger_entry_changes` defaults to 64 ledgers, which
  lines up with the network's checkpoint ledgers. Keep batch sizes as multiples
  of 64.
