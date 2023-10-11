package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var ledgerTransactionCmd = &cobra.Command{
	Use:   "export_ledger_transaction",
	Short: "Exports the ledger_transaction transaction data over a specified range.",
	Long:  `Exports the ledger_transaction transaction data over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(isTest, isFuture)

		ledgerTransaction, err := input.GetTransactions(startNum, endNum, limit, env)
		if err != nil {
			cmdLogger.Fatal("could not read ledger_transaction: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, transformInput := range ledgerTransaction {
			transformed, err := transform.TransformLedgerTransaction(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform ledger_transaction transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export transaction: %v", err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(ledgerTransaction), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, path)
	},
}

func init() {
	rootCmd.AddCommand(ledgerTransactionCmd)
	utils.AddCommonFlags(ledgerTransactionCmd.Flags())
	utils.AddArchiveFlags("ledger_transaction", ledgerTransactionCmd.Flags())
	utils.AddGcsFlags(ledgerTransactionCmd.Flags())
	ledgerTransactionCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (*required)

			limit: maximum number of ledger_transaction to export
				TODO: measure a good default value that ensures all ledger_transaction within a 5 minute period will be exported with a single call
				The current max_ledger_transaction_set_size is 1000 and there are 60 new ledgers in a 5 minute period:
					1000*60 = 60000

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
