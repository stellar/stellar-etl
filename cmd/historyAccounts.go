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
)

// historyAccountsCmd represents the historyAccounts command
var historyAccountsCmd = &cobra.Command{
	Use:   "historyAccounts",
	Short: "Exports a collection of mappings between account id and a numeric id",
	Long:  `Exports a collection of mappings between account id and a numeric id. These mappings are beneficial for constructing the history of the ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		/*
			Functionality planning:
			1. Read in start and end ledger numbers/timestamps
				1b. If timestamps are received, convert them to sequence numbers
			2. Provide ledger range to ingestion system and receive data
			3. Convert data from ingestion into an array of HistoryAccount structs with length equal to the limit flag
			4. Serialize array and output to file
		*/
	},
}

func init() {
	rootCmd.AddCommand(historyAccountsCmd)
	/*
		Needed flags:
			TODO: determine if providing ledger sequence number or timestamp is preferable (possibly could do both)
			start-time: the time for the beginning of the period to export; default to genesis ledger's creation time
			end-time: the time for the end of the period to export (required)

			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of account mappings to export
				TODO: empircally test the default limit, which should handle a 5-minute period and should be the same as the default limit for the account export

			output-file: filename of the output file

		Extra flags that may be useful:
			serialize-method: the method for serialization of the output data (JSON, XRD, etc)


	*/
}
