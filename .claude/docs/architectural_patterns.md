# Architectural Patterns — stellar-etl

## 1. Export Command Pipeline

Every `cmd/export_*.go` file follows an identical five-phase pattern. Use `cmd/export_operations.go:17-67` as the canonical reference.

**Phase 1 — Parse flags**

```
commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
startNum, path, parquetPath, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
env := utils.GetEnvironmentDetails(commonArgs)
```

**Phase 2 — Fetch data** via `input.Get{Entity}()` returning `[]{Entity}TransformInput`

**Phase 3 — Transform loop**: iterate over inputs, call `transform.Transform{Entity}()`, track failures with `numFailures` counter, export each record via `ExportEntry()` (`cmd/command_utils.go:55`)

**Phase 4 — Finalize**: close output file, call `PrintTransformStats()` (`cmd/command_utils.go:90`)

**Phase 5 — Optional upload/parquet**: `MaybeUpload()` (`cmd/command_utils.go:123`), `WriteParquet()` (`cmd/command_utils.go:162`)

**Registration** happens in each command's `init()` function — see `cmd/export_operations.go:70-75` for the pattern of `rootCmd.AddCommand()` + flag group additions + `MarkFlagRequired("end-ledger")`.

## 2. Transform Input Structs

Each entity defines a typed input struct bundling all data needed for transformation:

- `input/transactions.go:16-20` — `LedgerTransformInput` (Transaction, LedgerHistory, LedgerCloseMeta)
- `input/operations.go:15-21` — `OperationTransformInput` (Operation, OperationIndex, Transaction, LedgerSeqNum, LedgerCloseMeta)

Convention: `Get{Entity}()` fetches from ledger backend and returns `[]{Entity}TransformInput`. Transform functions accept the struct's fields as individual parameters.

## 3. Error Handling: Strict vs Lenient

Controlled by the `--strict-export` flag, stored in `EtlLogger.StrictExport` (`internal/utils/logger.go:7`).

- **Lenient (default)**: `LogError()` calls `l.Error()` — logs and continues (`logger.go:21`)
- **Strict**: `LogError()` calls `l.Fatal()` — stops execution (`logger.go:19`)

Transform errors are always caught in the loop, counted via `numFailures`, and reported by `PrintTransformStats()` as JSON: `{attempted_transforms, failed_transforms, successful_transforms}` (`cmd/command_utils.go:90-103`).

Convention: never call `cmdLogger.Fatal()` directly for transform errors. Always use `cmdLogger.LogError()` to respect strict mode. Use `Fatal()` only for unrecoverable setup errors (file I/O, backend creation).

## 4. SchemaParquet Interface

Defined at `internal/transform/parquet_converter.go:15-17`. Every `*Output` struct implements `ToParquet()` returning a `*OutputParquet` variant.

The Parquet variant exists because `uint32` must be converted to `int64` due to parquet-go limitations (see comment at `parquet_converter.go:5-7`).

Files follow the pattern: entity logic in `transform/{entity}.go`, its `ToParquet()` in `parquet_converter.go`, and Parquet struct definition alongside the output struct.

## 5. Ledger Backend Abstraction

Factory at `internal/utils/main.go:1011` (`CreateLedgerBackend`) selects between:

- **GCS Datastore** (production) — reads pre-exported ledger data from cloud storage
- **Captive Core** (legacy/testing) — runs a local Stellar Core instance

All `input.Get{Entity}()` functions call `CreateLedgerBackend()` and work identically regardless of backend. The choice is driven by the `--use-captive-core` flag.

## 6. Configuration Hierarchy

Managed by Viper in `cmd/root.go:52-75`:

1. CLI flags (highest priority)
2. Config file (`--config` or `$HOME/.stellar-etl.yaml`)
3. Environment variables (`viper.AutomaticEnv()` at `root.go:69`)
4. Flag defaults

Network selection (`internal/utils/main.go:886-915`): `--testnet` and `--futurenet` flags select the environment; default is mainnet. Each environment defines its own passphrase and archive URLs.

## 7. Constructor Factory Pattern

Dependencies are created via `New{Type}()` functions:

- `internal/utils/logger.go:10` — `NewEtlLogger()`
- `cmd/upload_to_gcs.go` — `newGCS()`

The global logger (`cmd/root.go:16`) is the sole exception to explicit dependency passing.

## 8. Golden File Testing

Integration tests use snapshot comparison via the harness in `cmd/test_utils.go`:

- `CliTest` struct (`test_utils.go:22-28`): defines `Name`, `Args`, `Golden` file, `WantErr`, `SortForComparison`
- `RunCLITest()` (`test_utils.go:39-126`): executes the binary, reads output, compares to golden file
- `GotTestDir()` (`test_utils.go:171`): returns `testdata/got/<TestName>/<filename>` for test output
- Golden files live in `testdata/<entity>/*.golden`
- Update with: `make int-test-update` (passes `-update=true`)

Unit tests in `internal/transform/` use table-driven patterns with Testify assertions. No mocking framework — tests use inline fixture data.

## 9. Adding a New Export Command

Checklist:

1. **Input struct**: Create `internal/input/{entity}.go` with `{Entity}TransformInput` struct and `Get{Entity}()` function
2. **Transform**: Create `internal/transform/{entity}.go` with `{Entity}Output`, `{Entity}OutputParquet` structs and `Transform{Entity}()` function
3. **Parquet**: Add `ToParquet()` method in `internal/transform/parquet_converter.go`
4. **Command**: Create `cmd/export_{entity}.go` following the five-phase pipeline from pattern #1
5. **Tests**: Create `cmd/export_{entity}_test.go` with `CliTest` cases and golden files in `testdata/{entity}/`
6. **Unit tests**: Create `internal/transform/{entity}_test.go` with table-driven tests

Use `cmd/export_operations.go` and `internal/input/operations.go` as templates.
