
# Stellar ETL
The Stellar-ETL is a data pipeline that allows users to extract data from the history of the Stellar network.

## Table of Contents

- [Stellar ETL](#stellar-etl)
  - [Table of Contents](#table-of-contents)
  - [Exporting the Ledger Chain](#exporting-the-ledger-chain)
   - [Command Reference](#command-reference)
		- [Bucket List Commands](#bucket-list-commands)
	       - [export_accounts](#export_accounts)
	       - [export_offers](#export_offers)
	       - [export_trustlines](#export_trustlines)
		- [History Archive Commands](#history-archive-commands)
		   - [export_ledgers](#export_ledgers)
		   - [export_transactions](#export_transactions)
		   - [export_operations](#export_operations)
		- [Stellar Core Commands](#stellar-core-commands)
		   - [export_ledger_entry_changes](#export_ledger_entry_changes)
		- [Utility Commands](#utility-commands)
		   - [get_ledger_range_from_times](#get_ledger_range_from_times) 
		   - [export_orderbooks](#export_orderbooks)
    - [Schemas](#schemas)
    - [Extensions](#extensions)
	    - [Adding New Commands](#adding-new-commands)




## Exporting the Ledger Chain

### Docker
1. Download the latest version of docker [Docker](https://www.docker.com/get-started)
2. Pull the stellar-etl Docker image: `docker pull stellar/stellar-etl`
3. Run the Docker images with the desired stellar-etl command: `docker run stellar/stellar-etl stellar-etl [etl-command] [etl-command arguments]`

### Manual Installation
1. Install Golang v1.16.5 or later: https://golang.org/dl/

2. Ensure that your Go bin has been added to the PATH env variable: `export PATH=$PATH:$(go env GOPATH)/bin`
3. Download and install Stellar-Core v13.2.0 or later: https://github.com/stellar/stellar-core/blob/master/INSTALL.md

4. Run `go get github.com/stellar/stellar-etl` to install the ETL

5. Run export commands to export information about the legder

## Command Reference
- [Bucket List Commands](#bucket-list-commands)
   - [export_accounts](#export_accounts)
   - [export_offers](#export_offers)
   - [export_trustlines](#export_trustlines)
- [History Archive Commands](#history-archive-commands)
   - [export_ledgers](#export_ledgers)
   - [export_transactions](#export_transactions)
   - [export_operations](#export_operations)
 - [Stellar Core Commands](#stellar-core-commands)
   - [export_ledger_entry_changes](#export_ledger_entry_changes)
   - [export_orderbooks](#export_orderbooks)
 - [Utility Commands](#utility-commands)
   - [get_ledger_range_from_times](#get_ledger_range_from_times) 

Every command accepts a `-h` parameter, which provides a help screen containing information about the command, its usage, and its flags.

### Bucket List Commands

These commands use the bucket list in order to ingest large amounts of data from the history of the stellar ledger. If you are trying to read large amounts of information in order to catch up to the current state of the ledger, these commands provide a good way to catchup quickly. However, they don't allow for custom start-ledger values. For updating within a user-defined range, see the Stellar Core commands.

#### export_accounts

```bash
> stellar-etl export_accounts --end-ledger 500000 --output exported_accounts.txt
```

This command exports accounts, starting from the genesis ledger and ending at the ledger determined by `end-ledger`. This command exports the point-in-time state of accounts, meaning that the exported data represents the account information as it was at `end-ledger`.

#### export_offers

```bash
> stellar-etl export_offers --end-ledger 500000 --output exported_offers.txt
```

This command exports offers, starting from the genesis ledger and ending at the ledger determined by `end-ledger`. This command exports the point-in-time state of offers on Stellar's decentralized exchange as it was at `end-ledger`.

#### export_trustlines

```bash
> stellar-etl export_trustlines --end-ledger 500000 --output exported_trustlines.txt
```

This command exports trustlines, starting from the genesis ledger and ending at the ledger determined by `end-ledger`. This command exports the point-in-time state of trustlines, meaning that the exported data represents the trustline information as it was at `end-ledger`.

### History Archive Commands

These commands export information using the history archives. This allows users to provide a start and end ledger range. The commands in this category export a list of everything that occurred within the provided range. All of the ranges are inclusive.
#### export_ledgers

```bash
> stellar-etl export_ledgers --start-ledger 1000 \
--end-ledger 500000 --output exported_ledgers.txt
```

This command exports ledgers within the provided range. 

### Stellar Core Commands

These commands require a Stellar Core instance that is v15.0.0 or later. The commands use the Core instance to retrieve information about changes from the ledger. These changes can be in the form of accounts, offers, or trustlines.

As the Stellar network grows, the Stellar Core instance has to catch up on an increasingly large amount of information. This catch-up process can add some overhead to the commands in this category. In order to avoid this overhead, run prefer processing larger ranges instead of many small ones, or use unbounded mode.
#### export_ledger_entry_changes

```bash
> stellar-etl export_ledger_entry_changes --start-ledger 1000 \
--end-ledger 500000 --output exported_changes_folder/
```

This command exports ledger changes within the provided ledger range. There are three data type flags that control which types of changes are exported. If no data type flags are set, then by default all three types are exported. If any are set, it is assumed that the others should not be exported. 

Changes are exported in batches of a size defined by the `batch-size` flag. By default, the batch-size parameter is set to 64 ledgers, which corresponds to a five minute period of time. This batch size is convenient because checkpoint ledgers are created every 64 ledgers. Checkpoint ledgers act as anchoring points for the nodes on the network, so it is beneficial to export in multiples of 64.

This command has two modes: bounded and unbounded.

##### Bounded
 If both a start and end ledger are provided, then the command runs in a bounded mode. This means that once all the ledgers in the range are processed and exported, the command shuts down.
 
##### Unbounded
If only a start ledger is provided, then the command runs in an unbounded fashion starting from the provided ledger. In this mode, the Stellar Core connects to the Stellar network and processes new changes as they occur on the network. Since the changes are continually exported in batches, this process can be continually run in the background in order to avoid the overhead of closing and starting new Stellar Core instances.

#### export_orderbooks

```bash
> stellar-etl export_orderbooks --start-ledger 1000 \
--end-ledger 500000 --output exported_orderbooks_folder/
```

This command exports orderbooks within the provided ledger range. Since exporting complete orderbooks at every single ledger would require an excessive amount of storage space, the output is normalized. Each batch that is exported contains multiple files, namely: `dimAccounts.txt`, `dimOffers.txt`, `dimMarkets.txt`, and `factEvents.txt`. The dim files relate a data structure to an ID. `dimMarkets`, for example, contains the buying and selling assets of a market, as well as the ID for that market. That ID is used in other places as a replacement for the full market information. This normalization process saves  a significant amount of space (roughly 90% in our benchmarks). The `factEvents` file connects ledger numbers to the offer IDs that were present at that ledger.

Orderbooks are exported in batches of a size defined by the `batch-size` flag. By default, the batch-size parameter is set to 64 ledgers, which corresponds to a five minute period of time. This batch size is convenient because checkpoint ledgers are created every 64 ledgers. Checkpoint ledgers act as anchoring points in that once they are available, so are the previous 63 nodes. It is beneficial to export in multiples of 64.

This command has two modes: bounded and unbounded.

##### Bounded
 If both a start and end ledger are provided, then the command runs in a bounded mode. This means that once all the ledgers in the range are processed and exported, the command shuts down.
 
##### Unbounded
If only a start ledger is provided, then the command runs in an unbounded fashion starting from the provided ledger. In this mode, the Stellar Core connects to the Stellar network and processes new orderbooks as they occur on the network. Since the changes are continually exported in batches, this process can be continually run in the background in order to avoid the overhead of closing and starting new Stellar Core instances.

#### export_transactions

```bash
> stellar-etl export_transactions --start-ledger 1000 \
--end-ledger 500000 --output exported_transactions.txt
```

This command exports transactions within the provided range.

#### export_operations

```bash
> stellar-etl export_operations --start-ledger 1000 \
--end-ledger 500000 --output exported_operations.txt
```

This command exports operations within the provided range.

#### export_effects

```bash
> stellar-etl export_operations --start-ledger 1000 \
--end-ledger 500000 --output exported_effects.txt
```

This command exports effects within the provided range.


### Utility Commands
#### get_ledger_range_from_times
```bash
> stellar-etl get_ledger_range_from_times \
--start-time 2019-09-13T23:00:00+00:00 \
--end-time 2019-09-14T13:35:10+00:00 --output exported_range.txt
```

This command exports takes in a start and end time and converts it to a ledger range. The ledger range that is returned will be the smallest possible ledger range that completely covers the provided time period. 


## Schemas

See https://github.com/stellar/stellar-etl/blob/master/internal/transform/schema.go for the schemas of the data structures that are outputted by the ETL.

## Extensions
This section covers some possible extensions or further work that can be done.

### Adding New Commands
In general, in order to add new commands, you need to add these files:

 - `export_new_data_structure.go` in the `cmd` folder
	 - This file can be generated with cobra by calling: `cobra add {command}`
	 - This file will parse flags, create output files, get the transformed data from the input package, and then export the data.
 - `export_new_data_structure_test.go` in the `cmd` folder
	 - This file will contain some tests for the newly added command. The `runCLI` function does most of the heavy lifting. All the tests need is the command arguments to test and the desired output.
	 - Test data should be stored in the `testate/new_data_structure` folder
 - `new_data_structure.go` in the `internal/input` folder
	 - This file will contain the methods needed to extract the new data structure from wherever it is located. This may be the history archives, the bucket list, or a captive core instance. 
	 - This file should extract the data and transform it, and return the transformed data.
	 - If working with captive core, the methods need to work in the background. There should be methods that export batches of data and send them to a channel. There should be other methods that read from the channel and transform the data so it can be exported.
- `new_data_structure.go` in the `internal/transform` folder
	- This file will contain the methods needed to transform the extracted data into a form that is suitable for BigQuery.
	- The struct definition for the transformed object should be stored in `schemas.go` in the `internal/transform` folder.

A good number of common methods are already written and stored in the `util` package.

