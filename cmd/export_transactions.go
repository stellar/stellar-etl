package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var transactionsCmd = &cobra.Command{
	Use:   "export_transactions",
	Short: "Exports the transaction data over a specified range.",
	Long:  `Exports the transaction data over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		endNum, useStdout, strictExport := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)

		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		transactions, err := input.GetTransactions(startNum, endNum, limit)
		if err != nil {
			cmdLogger.Fatal("could not read transactions: ", err)
		}

		failures := 0
		for _, transformInput := range transactions {
			transformed, err := transform.TransformTransaction(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				errMsg := fmt.Sprintf("could not transform transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq)
				if strictExport {
					cmdLogger.Fatal(errMsg, err)
				} else {
					cmdLogger.Warning(errMsg, err)
					failures++
					continue
				}

			}

			marshalled, err := json.Marshal(transformed)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				errMsg := fmt.Sprintf("could not json encode transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq)
				if strictExport {
					cmdLogger.Fatal(errMsg, err)
				} else {
					cmdLogger.Warning(errMsg, err)
					failures++
					continue
				}
			}

			if !useStdout {
				outFile.Write(marshalled)
				outFile.WriteString("\n")
			} else {
				fmt.Println(string(marshalled))
			}
		}

		if !strictExport {
			printTransformStats(len(transactions), failures)
		}
	},
}

func init() {
	rootCmd.AddCommand(transactionsCmd)
	utils.AddCommonFlags(transactionsCmd.Flags())
	utils.AddArchiveFlags("transactions", transactionsCmd.Flags())
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
