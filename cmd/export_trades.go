package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/stellar/stellar-etl/internal/toid"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

// tradesCmd represents the trades command
var tradesCmd = &cobra.Command{
	Use:   "export_trades",
	Short: "Exports the trade data",
	Long:  `Exports trade data within the specified range to an output file`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		outFile := mustOutFile(path)
		trades, err := input.GetTrades(startNum, endNum, limit, isTest)
		if err != nil {
			cmdLogger.Fatal("could not read trades ", err)
		}

		numFailures := 0
		totalNumBytes := 0
		for _, tradeInput := range trades {
			trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
			if err != nil {
				parsedID := toid.Parse(tradeInput.OperationHistoryID)
				cmdLogger.LogError(fmt.Errorf("from ledger %d, transaction %d, operation %d", parsedID.LedgerSequence, parsedID.TransactionOrder, parsedID.OperationOrder))
				numFailures += 1
				continue
			}

			for _, transformed := range trades {
				numBytes, err := exportEntry(transformed, outFile)
				if err != nil {
					cmdLogger.LogError(err)
					numFailures += 1
					continue
				}
				totalNumBytes += numBytes
			}
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(trades), numFailures)

		maybeUpload(gcpCredentials, gcsBucket, generateRunId(), path)
	},
}

func init() {
	rootCmd.AddCommand(tradesCmd)
	utils.AddCommonFlags(tradesCmd.Flags())
	utils.AddArchiveFlags("trades", tradesCmd.Flags())
	utils.AddGcsFlags(tradesCmd.Flags())
	tradesCmd.MarkFlagRequired("end-ledger")

	/*
		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
