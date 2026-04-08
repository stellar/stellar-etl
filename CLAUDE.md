# stellar-etl

Go CLI tool that extracts and transforms data from the Stellar blockchain network. Exports ledgers, transactions, operations, effects, trades, assets, contract events, token transfers, and ledger entry changes in JSON and Parquet formats.

## Tech Stack

- **Language**: Go 1.25 (module: `github.com/stellar/stellar-etl/v2`)
- **CLI**: Cobra + Viper (config: `$HOME/.stellar-etl.yaml` or `--config`)
- **Stellar SDK**: `go-stellar-sdk` for blockchain data access
- **Output**: JSON (line-delimited) and Apache Parquet via `parquet-go`
- **Cloud**: Google Cloud Storage for uploads
- **Logging**: Logrus (structured JSON)
- **Testing**: Testify for assertions, golden files for integration tests
- **Linting**: golangci-lint v2.4 (importas, misspell, gofmt, goimports)
- **CI**: GitHub Actions (lint, unit tests, integration tests, CodeQL, Docker build, release)

## Project Structure

```
cmd/                         # Cobra CLI commands
  root.go                    # Root command, config init (Viper)
  export_*.go                # One file per export entity (10 commands)
  export_*_test.go           # Integration tests (golden file comparison)
  command_utils.go           # Shared: file I/O, JSON/Parquet export, upload
  test_utils.go              # CLI test harness (CliTest struct, RunCLITest)
  upload_to_gcs.go           # GCS upload implementation

internal/
  input/                     # Data fetching from ledger backends
    *.go                     # Get{Entity}() → []{Entity}TransformInput
  transform/                 # Data transformation (46 files + 22 test files)
    *.go                     # Transform{Entity}() → {Entity}Output
    parquet_converter.go     # SchemaParquet interface + all ToParquet() impls
  utils/
    main.go                  # Flag helpers, env config, backend factory (~1050 lines)
    logger.go                # EtlLogger with strict/lenient error modes
  toid/                      # Stellar transaction object ID encoding

docker/                      # Dockerfile, Dockerfile.test, stellar-core configs
testdata/                    # Golden files organized by entity type
```

## Build & Test Commands

```bash
# Build
go build -v -o ./stellar-etl ./...

# Unit tests
go test -v -cover ./internal/...                          # All internal packages
go test -v -run ^TestTransformAsset$ ./internal/transform  # Single test

# Integration tests (Docker, needs GCP creds)
make int-test                # Runs golden file comparison (30min timeout)
make int-test-update         # Regenerate golden files with -update=true

# Lint
make lint                    # pre-commit: golangci-lint + file checks + prettier

# Docker
make docker-build            # Multi-stage build → stellar/stellar-etl:<hash>
make docker-push             # Push to DockerHub
```

Integration tests require GCP credentials at `$HOME/.config/gcloud/application_default_credentials.json`. CI enforces 55% coverage minimum.

## Key Interfaces

- `SchemaParquet` at `internal/transform/parquet_converter.go:15` — all output structs implement `ToParquet()`
- `CloudStorage` at `cmd/command_utils.go:15` — abstraction for cloud uploads
- Ledger backend (captive-core vs GCS datastore) selected at `internal/utils/main.go:1011`

## Key Types

- `CommonFlagValues` at `internal/utils/main.go:443` — parsed common CLI flags
- `EnvironmentDetails` at `internal/utils/main.go:876` — network-specific config (passphrase, archive URLs)
- `EtlLogger` at `internal/utils/logger.go:5` — logger with `StrictExport` toggle

## Flag Registration Helpers

All defined in `internal/utils/main.go`:

- `AddCommonFlags()` :232 — network, strict-export, parquet, captive-core
- `AddArchiveFlags()` :250 — start/end ledger, limit, output path
- `AddCloudStorageFlags()` :258 — GCS bucket, credentials, provider
- `AddCoreFlags()` :267 — captive-core binary, config, batch size
- `AddExportTypeFlags()` :280 — entity type selection for ledger_entry_changes

## Networks

Controlled by `--testnet` or `--futurenet` flags (default: mainnet). Resolved at `internal/utils/main.go:886`.

## Release Process

Branch naming drives semantic versioning on merge to master:

- `major/*` — bump major
- `minor/*` — bump minor
- anything else — bump patch

## Additional Documentation

- [Architectural Patterns](.claude/docs/architectural_patterns.md) — command pipeline, error handling, testing patterns, and conventions for adding new export commands
