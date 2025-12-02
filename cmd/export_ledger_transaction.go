package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var ledgerTransactionCmd = &cobra.Command{
	Use:   "export_ledger_transaction",
	Short: "Exports the ledger_transaction transaction data over a specified range.",
	Long:  `Exports the ledger_transaction transaction data over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, startTimestamp, path, _, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		var err error

		// If start/end timestamps are provided, override start/end ledger.
		// TODO: StartTimestamp and EndTimestamp default to "" to fit with how our current parameters are parsed.
		// We should refactor this later when we refactor our parameter parsing.
		if startTimestamp != "" && commonArgs.EndTimestamp != "" {
			startNum, commonArgs.EndNum, err = TimestampToLedger(startTimestamp, commonArgs.EndTimestamp, commonArgs.IsTest, commonArgs.IsFuture)
			if err != nil {
				cmdLogger.Fatal("could not calculate ledger range: ", err)
			}
		}

		ledgerTransaction, err := input.GetTransactions(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read ledger_transaction: ", err)
		}

		outFile := MustOutFile(path)
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

			numBytes, err := ExportEntry(transformed, outFile, commonArgs.Extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export transaction: %v", err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		PrintTransformStats(len(ledgerTransaction), numFailures)

		MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(ledgerTransactionCmd)
	utils.AddCommonFlags(ledgerTransactionCmd.Flags())
	utils.AddArchiveFlags("ledger_transaction", ledgerTransactionCmd.Flags())
	utils.AddCloudStorageFlags(ledgerTransactionCmd.Flags())
	//ledgerTransactionCmd.MarkFlagRequired("end-ledger")

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
