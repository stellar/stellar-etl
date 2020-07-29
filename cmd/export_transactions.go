package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
)

// transactionsCmd represents the transactions command
var transactionsCmd = &cobra.Command{
	Use:   "export_transactions",
	Short: "Exports the transaction data over a specified range.",
	Long:  `Exports the transaction data over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		startNum, endNum, limit, path := getBasicFlags(cmd)

		absolutePath, err := filepath.Abs(path)
		if err != nil {
			logger.Fatal("could not get absolute filepath: ", err)
		}

		err = createOutputFile(absolutePath)
		if err != nil {
			logger.Fatal("could not create output file: ", err)
		}

		// TODO: check the permissions of the file to ensure that it can be written to
		outFile, err := os.OpenFile(absolutePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			logger.Fatal("could not open output file: ", err)
		}
		transactions, err := input.GetTransactions(startNum, endNum, limit)
		if err != nil {
			logger.Fatal("could not read transactions: ", err)
		}

		for i, transformInput := range transactions {
			transformed, err := transform.TransformTransaction(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				errMsg := fmt.Sprintf("could not transform transaction %d in ledger %d: ", transformInput.Transaction.Index, startNum+uint32(i))
				logger.Fatal(errMsg, err)
			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				errMsg := fmt.Sprintf("could not json encode transaction %d in ledger %d: ", transformInput.Transaction.Index, startNum+uint32(i))
				logger.Fatal(errMsg, err)
			}

			outFile.Write(marshalled)
			outFile.WriteString("\n")
		}
	},
}

func init() {
	rootCmd.AddCommand(transactionsCmd)
	transactionsCmd.Flags().Uint32P("start-ledger", "s", 0, "The ledger sequence number for the beginning of the export period")
	transactionsCmd.Flags().Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range (required)")
	transactionsCmd.Flags().Uint32P("limit", "l", 60000, "Maximum number of transactions to export")
	transactionsCmd.Flags().StringP("output-file", "o", "exported_transactions.txt", "Filename of the output file")
	transactionsCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (*required)

			limit: maximum number of transactions to export
				TODO: measure a good default value that ensures all transactions within a 5 minute period will be exported with a single call
				The current max_tx_set_size is 1000 and there are 60 new ledgers in a 5 minute period:
					1000*60 = 60000

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
