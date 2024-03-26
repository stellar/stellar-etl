package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var diagnosticEventsCmd = &cobra.Command{
	Use:   "export_diagnostic_events",
	Short: "Exports the diagnostic events over a specified range.",
	Long:  `Exports the diagnostic events over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra, useCaptiveCore, datastoreUrl := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(isTest, isFuture, datastoreUrl)

		transactions, err := input.GetTransactions(startNum, endNum, limit, env, useCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read transactions: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		for _, transformInput := range transactions {
			transformed, err, ok := transform.TransformDiagnosticEvent(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform diagnostic events in transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
				numFailures += 1
				continue
			}

			if !ok {
				continue
			}
			for _, diagnosticEvent := range transformed {
				_, err := exportEntry(diagnosticEvent, outFile, extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export diagnostic event: %v", err))
					numFailures += 1
					continue
				}
			}
		}

		outFile.Close()

		printTransformStats(len(transactions), numFailures)

		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(diagnosticEventsCmd)
	utils.AddCommonFlags(diagnosticEventsCmd.Flags())
	utils.AddArchiveFlags("diagnostic_events", diagnosticEventsCmd.Flags())
	utils.AddCloudStorageFlags(diagnosticEventsCmd.Flags())
	diagnosticEventsCmd.MarkFlagRequired("end-ledger")

	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (*required)

			limit: maximum number of diagnostic events to export
				TODO: measure a good default value that ensures all diagnostic events within a 5 minute period will be exported with a single call
				The current max_tx_set_size is 1000 and there are 60 new ledgers in a 5 minute period:
					1000*60 = 60000

			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
