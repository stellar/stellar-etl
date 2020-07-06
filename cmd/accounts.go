package cmd

import (
	"github.com/spf13/cobra"
)

// accountsCmd represents the accounts command
var accountsCmd = &cobra.Command{
	Use:   "export_accounts",
	Short: "Exports the account data.",
	Long:  `Exports historical account data within the specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		/*
			Functionality planning:
			1. Read in start and end ledger numbers
			2. Provide ledger number to ingestion system and receive data
			3. Convert data from ingestion into an slice of Account structs (length should at most be the limit)
			4. Write slice to output file
			TODO: Consider having a way to export directly to BigQuery using bigquery library
		*/
	},
}

func init() {
	rootCmd.AddCommand(accountsCmd)
	/*
		Needed flags:
			TODO: determine if providing ledger sequence number or timestamp is preferable (possibly could do both)

			start-time: the time for the beginning of the period to export; default to genesis ledger's creation time
			end-time: the time for the end of the period to export (required)

			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of accounts to export;
				TODO: measure a good default value that ensures all accounts within a 5 minute period will be exported with a single call

			output-file: filename of the output file
	*/
}
