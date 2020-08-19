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
    - [Schema](#schema)




## Exporting the Ledger Chain

1. Install Golang: https://golang.org/dl/

2. Ensure that your Go bin has been added to the PATH env variable: `export PATH=$PATH:$(go env GOPATH)/bin`
3. Download and install Stellar-Core v13.2.0 or later: https://github.com/stellar/stellar-core/blob/master/INSTALL.md

4. Run `go get github.com/stellar/stellar-etl` to install the ETL

5. Run export commands in order to export information about the legder

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

Every command accepts a `-h` parameter, which provides a help screen containing information about the command, its usage, and its flags.

### Bucket List Commands

These commands use the bucket list in order to ingest large amounts of data from the history of the stellar ledger. If you are trying to read large amounts of information in order to catch up to the current state of the ledger, these commands provide a good way to catchup quickly. However, they don't allow for custom start-ledger values. For updating within a user-defined range, see the Stellar Core commands.

#### export_accounts

```bash
> stellar-etl export_accounts --end-ledger 500000 --output exported_accounts.txt
```

This command exports accounts, starting from the genesis ledger and ending at the ledger determined by `end-ledger`. This command exports the point in time state of accounts, meaning that the exported data represents the account information as it was at `end-ledger`.

#### export_offers

```bash
> stellar-etl export_offers --end-ledger 500000 --output exported_offers.txt
```

This command exports offers, starting from the genesis ledger and ending at the ledger determined by `end-ledger`. This command exports the point in time state of offer, meaning that the exported data represents the offerbook as it was at `end-ledger`.

#### export_trustlines

```bash
> stellar-etl export_trustlines --end-ledger 500000 --output exported_trustlines.txt
```

This command exports trustlines, starting from the genesis ledger and ending at the ledger determined by `end-ledger`. This command exports the point in time state of trustlines, meaning that the exported data represents the trustline information as it was at `end-ledger`.

### History Archive Commands

These commands export information using the history archives. This allows users to provide a start and end ledger range. The commands in this category export a list of everything that occurred within the provided range.
#### export_ledgers

```bash
> stellar-etl export_ledgers --start-ledger 1000 \
--end-ledger 500000 --output exported_ledgers.txt
```

This command exports ledgers within the provided range. Both `start-ledger` and `end-ledger` are included in the export process.

#### export_transactions

```bash
> stellar-etl export_transactions --start-ledger 1000 \
--end-ledger 500000 --output exported_transactions.txt
```

This command exports transactions within the provided range. This range is inclusive.

#### export_operations

```bash
> stellar-etl export_operations --start-ledger 1000 \
--end-ledger 500000 --output exported_operations.txt
```

This command exports transactions within the provided range. This range is inclusive.

### Stellar Core Commands

These commands require a Stellar Core instance that is v13.2.0 or later. The commands use the Core instance to retrieve information about changes from the ledger. These changes can be in the form of accounts, offers, or trustlines.

As the Stellar network grows, the Stellar Core instance has to catch up on an increasingly large amount of information. This can add some overhead to the commands in this category.
#### export_ledger_entry_changes

```bash
> stellar-etl export_ledger_entry_changes --start-ledger 1000 \
--end-ledger 500000 --output exported_ledgers.txt
```

This command exports ledger changes within the provided ledger range. There are three data type flags that control which types of changes are exported. If no data type flags are set, then by default all three types are exported. If any are set, it is assumed that the others should not be exported. Changes are exported in batches of a size defined by the `batch-size` flag.

This command has two modes: bounded and unbounded.

##### Bounded
 If both a start and end ledger are provided, then the command runs in a bounded mode. This means that once all the ledgers in the range are processed and exported, the command shuts down.
 
##### Unbounded
If only a start ledger is provided, then the command runs in an unbounded fashion starting from the provided ledger. In this mode, the Stellar Core connects to the Stellar network and processes new changes as they occur on the network. Since the changes are continually exported in batches, this process can be continually run in the background in order to avoid the overhead of closing and starting new Stellar Core instances.

## Schema

See https://github.com/stellar/stellar-etl/blob/master/internal/transform/schema.go for the schemas of the data structures that are outputted by the ETL.

