# **Stellar ETL**

The Stellar-ETL is a data pipeline that allows users to extract data from the history of the Stellar network.

## ** Before creating a branch **

Pay attention, it is very important to know if your modification to this repository is a release (breaking changes), a feature (functionalities) or a patch(to fix bugs). With that information, create your branch name like this:

- `release/<branch-name>`
- `feature/<branch-name>`
- `patch/<branch-name>`

If branch is already made, just rename it _before passing the pull request_.

## **Table of Contents**

- [Exporting the Ledger Chain](#exporting-the-ledger-chain)
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
- [Extensions](#extensions)
  - [Adding New Commands](#adding-new-commands)

<br>

---

# **Exporting the Ledger Chain**

## **Docker**

1. Download the latest version of docker [Docker](https://www.docker.com/get-started)
2. Pull the latest stellar-etl Docker image: `docker pull stellar/stellar-etl:latest`
3. Run the Docker images with the desired stellar-etl command: `docker run stellar/stellar-etl:latest stellar-etl [etl-command] [etl-command arguments]`

## **Manual Installation**

1. Install Golang v1.22.1 or later: https://golang.org/dl/
2. Ensure that your Go bin has been added to the PATH env variable: `export PATH=$PATH:$(go env GOPATH)/bin`
3. If using captive-core, download and install Stellar-Core v20.0.0 or later: https://github.com/stellar/stellar-core/blob/master/INSTALL.md
4. Run `go install github.com/stellar/stellar-etl@latest` to install the ETL
5. Run export commands to export information about the legder

## **Manual build for local development**

1. Clone this repo `git clone https://github.com/stellar/stellar-etl`
2. Build stellar-etl with `go build`
3. Build the docker image locally with `make docker-build`
4. Run the docker container in interactive mode to run export commands.
```sh
$ docker run --platform linux/amd64 -it stellar/stellar-etl:latest /bin/bash
```
5. Run export commands to export information about the legder
Example command to export ledger data
```sh
root@71890b878fca:/etl/data# stellar-etl export_ledgers --start-ledger 1000 --end-ledger 500000 --output exported_ledgers.txt
```

> _*Note:*_ If using the GCS datastore, you can run the following to set GCP credentials to use in your shell

```
gcloud auth login
gcloud config set project dev-hubble
gcloud auth application-default login
```

Add following to docker run command to pass gcloud credentials to docker container

```
-e GOOGLE_APPLICATION_CREDENTIALS=/.config/gcp/credentials.json -v "$HOME/.config/gcloud/application_default_credentials.json":/.config/gcp/credentials.json:ro
```

> _*Note:*_ Instructions for installing gcloud can be found [here](https://cloud.google.com/sdk/docs/install-sdk)

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

These commands export information using the [Ledger Exporter](https://github.com/stellar/go/blob/master/exp/services/ledgerexporter/README.md) output files within a specified datastore (currently [datastore](https://github.com/stellar/go/tree/master/support/datastore) only supports GCS). This allows users to provide a start and end ledger range. The commands in this category export a list of everything that occurred within the provided range. All of the ranges are inclusive.

> _*NOTE:*_ The datastore must contain the expected compressed LedgerCloseMetaBatch XDR binary files as exported from [Ledger Exporter](https://github.com/stellar/go/blob/master/exp/services/ledgerexporter/README.md#exported-files).

#### Common Flags

| Flag           | Description                                                                                   | Default                 |
| -------------- | --------------------------------------------------------------------------------------------- | ----------------------- |
| start-ledger   | The ledger sequence number for the beginning of the export period. Defaults to genesis ledger | 2                       |
| end-ledger     | The ledger sequence number for the end of the export range                                    | 0                       |
| strict-export  | If set, transform errors will be fatal                                                        | false                   |
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

This command exports ledger changes within the provided ledger range. Flags can filter which ledger entry types are exported. If no data type flags are set, then by default all types are exported. If any are set, it is assumed that the others should not be exported.

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

This section covers some possible extensions or further work that can be done.

## **Adding New Commands**

In general, in order to add new commands, you need to add these files:

- `export_new_data_structure.go` in the `cmd` folder
  - This file can be generated with cobra by calling: `cobra add {command}`
  - This file will parse flags, create output files, get the transformed data from the input package, and then export the data.
- `export_new_data_structure_test.go` in the `cmd` folder
  - This file will contain some tests for the newly added command. The `runCLI` function does most of the heavy lifting. All the tests need is the command arguments to test and the desired output.
  - Test data should be stored in the `testdata/new_data_structure` folder
- `new_data_structure.go` in the `internal/input` folder
  - This file will contain the methods needed to extract the new data structure from wherever it is located. This may be the history archives, the bucket list, a captive core instance, or a datastore.
  - If working with captive core, the methods need to work in the background. There should be methods that export batches of data and send them to a channel. There should be other methods that read from the channel and transform the data so it can be exported.
- `new_data_structure.go` in the `internal/transform` folder
  - This file will contain the methods needed to transform the extracted data into a form that is suitable for BigQuery.
  - The struct definition for the transformed object should be stored in `schemas.go` in the `internal/transform` folder.

A good number of common methods are already written and stored in the `util` package.
