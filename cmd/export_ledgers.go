package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var ledgersCmd = &cobra.Command{
	Use:   "export_ledgers",
	Short: "Exports the ledger data.",
	Long:  `Exports ledger data within the specified range to an output file. Encodes ledgers as JSON objects and exports them to the output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra, useCaptiveCore, datastoreUrl := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, path, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(isTest, isFuture, datastoreUrl)

		var ledgers []historyarchive.Ledger
		var err error

		if useCaptiveCore {
			ledgers, err = input.GetLedgersHistoryArchive(startNum, endNum, limit, isTest, isFuture)
		} else {
			ledgers, err = input.GetLedgers(startNum, endNum, limit, env, useCaptiveCore)
		}
		if err != nil {
			cmdLogger.Fatal("could not read ledgers: ", err)
		}

		outFile := mustOutFile(path)

		numFailures := 0
		totalNumBytes := 0
		for i, lcm := range ledgers {
			transformed, err := transform.TransformLedger(lcm)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not json transform ledger %d: %s", startNum+uint32(i), err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export ledger %d: %s", startNum+uint32(i), err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(ledgers), numFailures)

		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(ledgersCmd)
	utils.AddCommonFlags(ledgersCmd.Flags())
	utils.AddArchiveFlags("ledgers", ledgersCmd.Flags())
	utils.AddCloudStorageFlags(ledgersCmd.Flags())
	ledgersCmd.MarkFlagRequired("end-ledger")
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
