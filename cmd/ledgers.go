package cmd

import (
	"github.com/spf13/cobra"
)

// ledgersCmd represents the ledgers command
var ledgersCmd = &cobra.Command{
	Use:   "ledgers",
	Short: "Exports the ledger data.",
	Long:  `Exports ledger data within the specified ledger range to an output file. This each ledger also contains an extra numerical id, which is useful for constructing the history of the ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		/*
			Functionality planning:
			1. Read in start and end ledger numbers/timestamps
				1b. If timestamps are received, convert them to ledger sequence numbers
			2. Get each ledger in the range from the ingestion system
			3. For each ledger received, make a corresponding Ledger struct in the output array
			4. Serialize array and output it to a file
		*/
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	/*
		Needed flags:
			TODO: determine if providing ledger sequence number or timestamp is preferable (possibly could do both)

			start-time: the time for the beginning of the period to export; default to genesis ledger's creation time
			end-time: the time for the end of the period to export (required)

			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of ledgers to export; default to 60 (5 ledgers per second over our 5 minute update period)
			output-file: filename of the output file

		Extra flags that may be useful:
			serialize-method: the method for serialization of the output data (JSON, XRD, etc)
	*/
}
