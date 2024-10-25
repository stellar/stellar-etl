## v2.0.0

* Release/CI enhancements (#247) @laysabit
* Update readme (#245) @chowbao
* Create codeql.yml (#243) @kanwalpreetd
* Update to latest stellar/go change with updated ledgerexporter zstd (#244) @chowbao
* Update to use BufferedStorageBackend to read txmeta files (#242) @chowbao
* Add contract code fees (#241) @chowbao
* Add fee fields; formatting for warnings (#240) @chowbao
* adding ledgerbackend datastore txmeta as a data source (#235) @chowbao
* Feature/change workdir (#238) @edualvess
* Add `ledger_key_hash` to `history_operations` (#237) @lucaszanotelli
* Bump google.golang.org/protobuf from 1.32.0 to 1.33.0 (#232) @dependabot
* Fix operation trace code bug (#236) @chowbao
* Fix fee charged calculation (#234) @chowbao
* Release drafter and publisher (#233) @laysabit
* Update soroban fees (#231) @chowbao
* Add case for null operationResultTr for operation_trace_code (#230) @chowbao
* Enable diagnostic events (#229) @chowbao
* Log Transaction Codes and Operation Traces (#228) @sydneynotthecity

## v1.0.0

* Update stellar-core version by @chowbao in https://github.com/stellar/stellar-etl/pull/216
* doc : commented functions in cmd and internal/input folder   by @laysabit in https://github.com/stellar/stellar-etl/pull/217
* Add user agent to captive core config by @chowbao in https://github.com/stellar/stellar-etl/pull/219
* Add export all history command by @chowbao in https://github.com/stellar/stellar-etl/pull/220
* Add OperationTypeManageSellOffer operation type to history_assets export by @cayod in https://github.com/stellar/stellar-etl/pull/214
* Export_assets fix by @cayod in https://github.com/stellar/stellar-etl/pull/222
* Update core image for testnet reset by @chowbao in https://github.com/stellar/stellar-etl/pull/223
* use NewLedgerChangeReaderFromLedgerCloseMeta by @sfsf9797 in https://github.com/stellar/stellar-etl/pull/221
* Fix contract data balance holder by @chowbao in https://github.com/stellar/stellar-etl/pull/225
* Revert "Fix contract data balance holder" by @chowbao in https://github.com/stellar/stellar-etl/pull/226
* Add common interface to write exported files by @chowbao in https://github.com/stellar/stellar-etl/pull/215
* Fix soroban hashes to hexstring by @chowbao in https://github.com/stellar/stellar-etl/pull/227