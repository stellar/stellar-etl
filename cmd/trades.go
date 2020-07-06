package cmd

import (
	"github.com/spf13/cobra"
)

// tradesCmd represents the trades command
var tradesCmd = &cobra.Command{
	Use:   "export_trades",
	Short: "Exports the trade data",
	Long:  `Exports trade data within the specified range to an output file`,
	Run: func(cmd *cobra.Command, args []string) {
		/*
			Functionality planning:
			1. Read in start and end ledger numbers/timestamps
				1b. If timestamps are received, convert them to ledger sequence numbers
			2. Receive data from ingestion and extract trades
			3. Put trade information into a Go slice of Trade structs (length should at most be the limit)
			4. Serialize slice and output it to a file
		*/
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	/*
		Needed flags:
			TODO: determine if providing ledger sequence number or timestamp is preferable (possibly could do both)
				*: If we do both, do not require both end-time and end-ledger; only need one or the other

			start-time: the time for the beginning of the period to export; default to genesis ledger's creation time
			end-time: the time for the end of the period to export (*required)

			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (*required)

			limit: maximum number of trades to export
				TODO: measure a good default value that ensures all trades within a 5 minute period will be exported with a single call

			output-file: filename of the output file

		Extra flags that may be useful:
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
	*/
}
