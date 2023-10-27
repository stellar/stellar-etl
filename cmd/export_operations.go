package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var operationsCmd = &cobra.Command{
	Use:   "export_operations",
	Short: "Exports the operations data over a specified range",
	Long:  `Exports the operations data over a specified range. Each operation is an individual command that mutates the Stellar ledger.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(isTest, isFuture)

		operations, err := input.GetOperations(startNum, endNum, limit, env)
		if err != nil {
			cmdLogger.Fatal("could not read operations: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, transformInput := range operations {
			transformed, err := transform.TransformOperation(transformInput.Operation, transformInput.OperationIndex, transformInput.Transaction, transformInput.LedgerSeqNum, transformInput.LedgerCloseMeta, env.NetworkPassphrase)
			if err != nil {
				txIndex := transformInput.Transaction.Index
				cmdLogger.LogError(fmt.Errorf("could not transform operation %d in transaction %d in ledger %d: %v", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export operation: %v", err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(operations), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, path)
	},
}

func init() {
	rootCmd.AddCommand(operationsCmd)
	utils.AddCommonFlags(operationsCmd.Flags())
	utils.AddArchiveFlags("operations", operationsCmd.Flags())
	utils.AddGcsFlags(operationsCmd.Flags())
	operationsCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of operations to export; default to 6,000,000
				each transaction can have up to 100 operations
				each ledger can have up to 1000 transactions
				there are 60 new ledgers in a 5 minute period

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
