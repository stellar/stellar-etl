package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var tokenTransfersCmd = &cobra.Command{
	Use:   "export_token_transfer",
	Short: "Exports the token transfer event data.",
	Long:  `Exports token transfer data within the specified range to an output file. Encodes ledgers as JSON objects and exports them to the output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, path, _, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		var ledgers []utils.HistoryArchiveLedgerAndLCM
		var err error

		ledgers, err = input.GetLedgers(startNum, commonArgs.EndNum, limit, env, commonArgs.UseCaptiveCore)

		if err != nil {
			cmdLogger.Fatal("could not read ledgers: ", err)
		}

		outFile := MustOutFile(path)

		numFailures := 0
		totalNumBytes := 0
		for i, ledger := range ledgers {
			transformed, err := transform.TransformTokenTransfer(ledger.LCM, env.NetworkPassphrase)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not json transform ttp %d: %s", startNum+uint32(i), err))
				numFailures += 1
				continue
			}

			for _, transform := range transformed {
				numBytes, err := ExportEntry(transform, outFile, commonArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export ledger %d: %s", startNum+uint32(i), err))
					numFailures += 1
					continue
				}
				totalNumBytes += numBytes
			}
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		PrintTransformStats(len(ledgers), numFailures)

		MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(tokenTransfersCmd)
	utils.AddCommonFlags(tokenTransfersCmd.Flags())
	utils.AddArchiveFlags("token_transfer", tokenTransfersCmd.Flags())
	utils.AddCloudStorageFlags(tokenTransfersCmd.Flags())
	tokenTransfersCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range (required)

			limit: maximum number of ledgers to export; default to 60 (1 ledger per 5 seconds over our 5 minute update period)
			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
