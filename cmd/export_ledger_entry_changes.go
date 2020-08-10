/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/utils"
)

// exportLedgerEntryChangesCmd represents the exportLedgerEntryChanges command
var exportLedgerEntryChangesCmd = &cobra.Command{
	Use:   "export_ledger_entry_changes",
	Short: "This command exports the changes in accounts, offers, and trustlines.",
	Long: `This command instantiates a stellar-core instance and uses it to export about accounts, offers, and trustlines.
The information is exported in batches determined by the batch-size flag. Each exported file will include the changes to the 
relevent data type that occurred during that batch.

If the end-ledger is omitted, then the stellar-core node will continue running and exporting information as new ledgers are 
confirmed by the Stellar network. 

If no data type flags are set, then by default all of them are exported. If any are set, it is assumed that the others should not
be exported.`,
	Run: func(cmd *cobra.Command, args []string) {
		/*
			1. Instantiate a captive core instance
				a) If the start and end are provided, then use a bounded range and exit after exporting the info inside the range
				b) If the end is omitted, use an unbounded range and continue exporting as new ledgers are added to the network
			2. Call GetLedger() constantly in a separate goroutine
				a) Create channels for each data type
				b) Process changes for the ledger and send changes to the channel matching their type
			3. On the other end, receive changes from the channel
				a) Call transform on individual changes
				b) Once batch_size ledgers have been sent, encode and export the changes
		*/
	},
}

func init() {
	rootCmd.AddCommand(exportLedgerEntryChangesCmd)
	utils.AddBasicFlags("changes", exportLedgerEntryChangesCmd.Flags())
	exportLedgerEntryChangesCmd.Flags().Uint32P("batch-size", "b", 64, "number of ledgers to export changes from in each batches")
	exportLedgerEntryChangesCmd.Flags().BoolP("export-accounts", "a", false, "set in order to export account changes")
	exportLedgerEntryChangesCmd.Flags().BoolP("export-trustlines", "t", false, "set in order to export trustline changes")
	exportLedgerEntryChangesCmd.Flags().BoolP("export-offers", "f", false, "set in order to export offer changes")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range

			output-file: filename of the output file
			stdout: if true, prints to stdout instead of the command line
			limit: maximum number of changes to export in a given batch; if negative then everything gets exported
			batch-size: size of the export batches

			If none of the export_X flags are set, assume everything should be exported
				export_accounts: boolean flag; if set then accounts should be exported
				export_trustlines: boolean flag; if set then trustlines should be exported
				export_offers: boolean flag; if set then offers should be exported

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
