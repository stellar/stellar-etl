package cmd

import (
	"github.com/spf13/cobra"
)

// operationsCmd represents the operations command
var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range",
	Long:  `Exports the operations data over a specified range. Each operation is an individual command that mutates the Stellar ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		/*
			Functionality planning:
			1. Read in start and end ledger numbers/timestamps
				1b. If timestamps are received, convert them to ledger sequence numbers
			2. Convert data from ingestion into a Go slice of Operation structs (length should at most be the limit)
				2b. Conversion will probably involve extracting the operations from the transactions that occurred over the time-frame
			3. Serialize slice and output it to a file
		*/
	},
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	/*
		Needed flags:
			TODO: determine if providing ledger sequence number or timestamp is preferable (possibly could do both)

			start-time: the time for the beginning of the period to export; default to genesis ledger's creation time
			end-time: the time for the end of the period to export (required)

			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of ledgers to export;
				Default to 300,000 as there are 60 ledgers in a 5 minute period, each with 50 transactions, and each transaction can have up to 100 operations

			output-file: filename of the output file

		Extra flags that may be useful:
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
	*/
}
