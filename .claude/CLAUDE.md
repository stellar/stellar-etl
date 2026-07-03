# CLAUDE.md

## Critical rules — read these first

- **Run lint, unit tests, and integration tests before considering a task complete**: `golangci-lint run`, `go test -v -cover ./internal/transform`, `make int-test`.
- **Run pre-commit before committing or creating a PR.**
- Use the GCS datastore and its default settings for `LedgerCloseMetaBatch` files.
- `uint32` fields must be converted to `int64` in all `ToParquet()` implementations (parquet-go restriction).
- Never commit secrets, API keys, or credentials. Prefer small focused changes; keep PRs scoped to the request.

## Skills — load the matching one BEFORE starting

Skills live in the data-platform monorepo checkout under `.claude/skills/` — `../.claude/skills/` when this repo sits inside that checkout. They are readable as plain files from anywhere; they load as skills when the session starts at the monorepo root.

| Task | Skill |
|---|---|
| Build, test, or debug a failing test in this repo | `stellar-etl-build-and-test` |
| Add or change an export command / output field | `stellar-etl-export-change` |
| Propagate a field across etl → BQ schema → dbt → airflow | `etl-schema-change` |
| Protocol upgrade work (new XDR, new ledger entry types) | `stellar-etl-protocol-upgrade` |
| Shipping a change that Airflow must consume (image bump) | `cross-repo-release` |

## What This Project Does

Stellar-ETL is a Go CLI that extracts data from the Stellar blockchain and exports it as newline-delimited JSON or Parquet files for ingestion into BigQuery. Reads from a GCS-hosted datastore of `LedgerCloseMetaBatch` XDR binary files.

## Build & Development Commands

```sh
# Build the binary
go build

# Run all unit tests
go test -v -cover ./internal/transform

# Run a single unit test
go test -v -run ^TestTransformLedger$ ./internal/transform

# Run integration tests (requires Docker and GCP credentials)
make int-test

# Run integration tests and update golden files
make int-test-update

# Run a single integration test
docker-compose build
docker-compose run \
  -v $(HOME)/.config/gcloud/application_default_credentials.json:/usr/credential.json:ro \
  -v $(PWD)/testdata:/usr/src/etl/testdata \
  -e GOOGLE_APPLICATION_CREDENTIALS=/usr/credential.json \
  integration-tests \
  go test -v -run ^TestExportLedgers$ ./cmd -timeout 30m

# Build Docker image
make docker-build

# Run linter (gofmt + goimports + importas + misspell)
golangci-lint run
```

## Code Architecture

### Package Structure

```
cmd/                     # Cobra CLI commands (one file per export command)
internal/
  input/                 # Data extraction layer — reads from GCS datastore
  transform/             # Transformation layer — converts XDR types to output structs
    schema.go            # All JSON output struct definitions (BigQuery-aligned)
    schema_parquet.go    # All Parquet output struct definitions
    parquet_converter.go # SchemaParquet interface + ToParquet() implementations
  toid/                  # Transaction Object ID calculation utilities
  utils/                 # Shared flag parsing, environment config, logger, helpers
```

### Data Flow

Every export command follows the same pipeline:

1. **Extract** (`internal/input/`): reads `LedgerCloseMeta` XDR from a GCS datastore, produces batched raw data
2. **Transform** (`internal/transform/`): converts raw XDR types to flat output structs defined in `schema.go`
3. **Write** (`cmd/command_utils.go`): `ExportEntry` writes newline-delimited JSON; `WriteParquet` writes Parquet using the `SchemaParquet` interface

### Adding a New Export Command

Four files are required:

1. `cmd/export_<name>.go` — Cobra command, flag parsing, orchestration
2. `cmd/export_<name>_test.go` — integration test with golden files in `testdata/<name>/`
3. `internal/input/<name>.go` — extraction logic (channel-based for streaming)
4. `internal/transform/<name>.go` — transformation logic; add the output struct to `schema.go` (and `schema_parquet.go` + `parquet_converter.go` if Parquet output is needed)

### Output Format

- JSON files: newline-delimited, written via `ExportEntry` in `cmd/command_utils.go`
- Parquet files: written via `WriteParquet`; each schema struct must implement `SchemaParquet` (`ToParquet() interface{}`)
- Filenames follow the pattern `{start}-{end-1}-{export_type}.{txt|parquet}`
