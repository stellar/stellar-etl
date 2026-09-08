# **Stellar ETL**

[![Apache 2.0 licensed](https://img.shields.io/badge/license-apache%202.0-blue.svg)](LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/stellar/stellar-etl)

The Stellar-ETL is a data pipeline that allows users to extract data from the history of the Stellar network.

## **Documentation**

| Document                           | Contents                                                                 |
| ---------------------------------- | ------------------------------------------------------------------------ |
| This README                        | Installing the ETL and a reference for every command it exposes          |
| [DEVELOPING.md](DEVELOPING.md)     | Building, running, and testing the ETL locally, and adding a new command |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Branch naming, pull request etiquette, release mechanics, and Go style   |

For background on the Stellar network and the data this pipeline extracts, see
the [Stellar developer documentation](https://developers.stellar.org/). For the
BigQuery datasets built on top of this ETL, see
[Hubble](https://developers.stellar.org/docs/data/analytics/hubble).

## **Table of Contents**

- [Install](#install)
- [Command Reference](#command-reference)
  - [Export Commands](#export-commands)
    - [export_ledgers](#export_ledgers)
    - [export_transactions](#export_transactions)
    - [export_operations](#export_operations)
    - [export_effects](#export_effects)
    - [export_assets](#export_assets)
    - [export_trades](#export_trades)
    - [export_diagnostic_events](#export_diagnostic_events)
    - [export_ledger_entry_changes](#export_ledger_entry_changes)
  - [Utility Commands](#utility-commands)
    - [get_ledger_range_from_times](#get_ledger_range_from_times)
- [Schemas](#schemas)

<br>

---

# Install

## **Docker**

1. Download the latest version of docker [Docker](https://www.docker.com/get-started)
2. Pull the latest stellar-etl Docker image: `docker pull stellar/stellar-etl:latest`
3. Run the Docker images with the desired stellar-etl command: `docker run stellar/stellar-etl:latest stellar-etl [etl-command] [etl-command arguments]`

## **Manual Installation**

1. Install Golang at the version pinned in [go.mod](go.mod) or later: https://golang.org/dl/
2. Ensure that your Go bin has been added to the PATH env variable: `export PATH=$PATH:$(go env GOPATH)/bin`
3. If using captive-core, download and install Stellar-Core v20.0.0 or later: https://github.com/stellar/stellar-core/blob/master/INSTALL.md
4. Run `go install github.com/stellar/stellar-etl/v2@latest` to install the ETL
5. Run export commands to export information about the legder

## **Building from source**

To build, run, and test the ETL locally, see [DEVELOPING.md](DEVELOPING.md).

<br>

---

# **Command Reference**

- [Export Commands](#export-commands)
  - [export_ledgers](#export_ledgers)
  - [export_transactions](#export_transactions)
  - [export_operations](#export_operations)
  - [export_effects](#export_effects)
  - [export_assets](#export_assets)
  - [export_trades](#export_trades)
  - [export_diagnostic_events](#export_diagnostic_events)
  - [export_ledger_entry_changes](#export_ledger_entry_changes)
- [Utility Commands](#utility-commands)
  - [get_ledger_range_from_times](#get_ledger_range_from_times)

Every command accepts a `-h` parameter, which provides a help screen containing information about the command, its usage, and its flags.

Commands have the option to read from testnet with the `--testnet` flag, from futurenet with the `--futurenet` flag, and defaults to reading from mainnet without any flags.

> _*NOTE:*_ Adding both flags will default to testnet. Each stellar-etl command can only run from one network at a time.

<br>

---

## **Export Commands**

These commands export information using the [Galexie](https://developers.stellar.org/docs/data/indexers/build-your-own/galexie) output files within a specified datastore (currently [datastore](https://github.com/stellar/go-stellar-sdk/tree/master/support/datastore) only supports GCS). This allows users to provide a start and end ledger range. The commands in this category export a list of everything that occurred within the provided range. All of the ranges are inclusive.

> _*NOTE:*_ The datastore must contain the expected compressed LedgerCloseMetaBatch XDR binary files as exported from [Galexie](https://developers.stellar.org/docs/data/indexers/build-your-own/galexie).

#### Common Flags

| Flag           | Description                                                                                   | Default                 |
| -------------- | --------------------------------------------------------------------------------------------- | ----------------------- |
| start-ledger   | The ledger sequence number for the beginning of the export period. Defaults to genesis ledger | 2                       |
| end-ledger     | The ledger sequence number for the end of the export range                                    | 0                       |
| strict-export  | If set, transform errors will be fatal                                                        | true                    |
| testnet        | If set, will connect to Testnet instead of Pubnet                                             | false                   |
| futurenet      | If set, will connect to Futurenet instead of Pubnet                                           | false                   |
| extra-fields   | Additional fields to append to output jsons. Used for appending metadata                      | ---                     |
| captive-core   | If set, run captive core to retrieve data. Otherwise use TxMeta file datastore                | false                   |
| datastore-path | Datastore bucket path to read txmeta files from                                               | ledger-exporter/ledgers |
| buffer-size    | Buffer size sets the max limit for the number of txmeta files that can be held in memory      | 1000                    |
| num-workers    | Number of workers to spawn that read txmeta files from the datastore                          | 5                       |
| retry-limit    | Datastore GetLedger retry limit                                                               | 3                       |
| retry-wait     | Time in seconds to wait for GetLedger retry                                                   | 5                       |

> _*NOTE:*_ Using captive-core requires a Stellar Core instance that is v20.0.0 or later. The commands use the Core instance to retrieve information about changes from the ledger. More information about the Stellar ledger information can be found [here](https://developers.stellar.org/network/horizon/api-reference/resources).
> <br> As the Stellar network grows, the Stellar Core instance has to catch up on an increasingly large amount of information. This catch-up process can add some overhead to the commands in this category. In order to avoid this overhead, run prefer processing larger ranges instead of many small ones, or use unbounded mode.
> <br><br> Recommended resources for running captive-core within a KubernetesPod:
>
> ```
> {cpu: 3.5, memory: 20Gi, ephemeral-storage: 12Gi}
> ```

<br>

---

### **export_ledgers**

```bash
> stellar-etl export_ledgers --start-ledger 1000 \
--end-ledger 500000 --output exported_ledgers.txt
```

This command exports ledgers within the provided range.

<br>

---

### **export_transactions**

```bash
> stellar-etl export_transactions --start-ledger 1000 \
--end-ledger 500000 --output exported_transactions.txt
```

This command exports transactions within the provided range.

<br>

---

### **export_operations**

```bash
> stellar-etl export_operations --start-ledger 1000 \
--end-ledger 500000 --output exported_operations.txt
```

This command exports operations within the provided range.

<br>

---

### **export_effects**

```bash
> stellar-etl export_effects --start-ledger 1000 \
--end-ledger 500000 --output exported_effects.txt
```

This command exports effects within the provided range.

<br>

---

### **export_assets**

```bash
> stellar-etl export_assets \
--start-ledger 1000 \
--end-ledger 500000 --output exported_assets.txt
```

Exports the assets that are created from payment operations over a specified ledger range.

<br>

---

### **export_trades**

```bash
> stellar-etl export_trades \
--start-ledger 1000 \
--end-ledger 500000 --output exported_trades.txt
```

Exports trade data within the specified range to an output file

<br>

---

### **export_diagnostic_events**

```bash
> stellar-etl export_diagnostic_events \
--start-ledger 1000 \
--end-ledger 500000 --output export_diagnostic_events.txt
```

Exports diagnostic events data within the specified range to an output file

<br>

---

### **export_ledger_entry_changes**

```bash
> stellar-etl export_ledger_entry_changes --start-ledger 1000 \
--end-ledger 500000 --output exported_changes_folder/
```

This command exports ledger changes within the provided ledger range. Flags can filter which ledger entry types are exported. If no data type flags are set, then by default all types are exported. If any are set, it is assumed that the others should not be exported. If a data type flag is set to false, and no other data type flags are set to true, all types are exported.

```bash
# This command exports all data types
> stellar-etl export_ledger_entry_changes --export-accounts=false --start-ledger 1000 \
--end-ledger 500000 --output exported_changes_folder/
```

Changes are exported in batches of a size defined by the `--batch-size` flag. By default, the batch-size parameter is set to 64 ledgers, which corresponds to a five minute period of time. This batch size is convenient because checkpoint ledgers are created every 64 ledgers. Checkpoint ledgers act as anchoring points for the nodes on the network, so it is beneficial to export in multiples of 64.

This command has two modes: bounded and unbounded.

#### **Bounded**

If both a start and end ledger are provided, then the command runs in a bounded mode. This means that once all the ledgers in the range are processed and exported, the command shuts down.

#### **Unbounded (Currently Unsupported)**

If only a start ledger is provided, then the command runs in an unbounded fashion starting from the provided ledger. In this mode, stellar-etl will block and wait for the next sequentially written ledger file in the datastore. Since the changes are continually exported in batches, this process can be continually run in the background in order to avoid the overhead of closing and starting new stellar-etl instances.

The following are the ledger entry type flags that can be used to export data:

- export-accounts
- export-trustlines
- export-offers
- export-pools
- export-balances
- export-contract-code
- export-contract-data
- export-config-settings
- export-ttl

<br>

---

## **Utility Commands**

These commands aid in the usage of [Export Commands](#export-commands).

### **get_ledger_range_from_times**

```bash
> stellar-etl get_ledger_range_from_times \
--start-time 2019-09-13T23:00:00+00:00 \
--end-time 2019-09-14T13:35:10+00:00 --output exported_range.txt
```

This command takes in a start and end time and converts it to a ledger range. The ledger range that is returned will be the smallest possible ledger range that completely covers the provided time period.

<br>

---

# Schemas

See https://github.com/stellar/stellar-etl/blob/master/internal/transform/schema.go for the schemas of the data structures that are outputted by the ETL.

<br>

---

# Extensions

To add a new export command, see
[Adding a new command](DEVELOPING.md#adding-a-new-command).
