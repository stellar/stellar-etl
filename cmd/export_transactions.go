package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
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
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		transactions, err := input.GetTransactions(startNum, endNum, limit, isTest)
		if err != nil {
			cmdLogger.Fatal("could not read transactions: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, transformInput := range transactions {
			transformed, err := transform.TransformTransaction(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile)
			if err != nil {
				cmdLogger.LogError(err)
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(transactions), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, generateRunId(), path)
	},
}

func init() {
	rootCmd.AddCommand(transactionsCmd)
	utils.AddCommonFlags(transactionsCmd.Flags())
	utils.AddArchiveFlags("transactions", transactionsCmd.Flags())
	utils.AddGcsFlags(transactionsCmd.Flags())
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
