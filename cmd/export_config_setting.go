package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

var configSettingCmd = &cobra.Command{
	Use:   "export_config_setting",
	Short: "Exports the config setting information.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		settings, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeConfigSetting, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("Error getting ledger entries: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, setting := range settings {
			transformed, err := transform.TransformConfigSetting(setting, xdr.LedgerHeaderHistoryEntry{})
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform config setting %+v: %v", setting, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export config setting %+v: %v", setting, err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}
		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(settings), numFailures)
		maybeUpload(gcpCredentials, gcsBucket, path)

	},
}

func init() {
	rootCmd.AddCommand(configSettingCmd)
	utils.AddCommonFlags(configSettingCmd.Flags())
	utils.AddBucketFlags("config_settings", configSettingCmd.Flags())
	utils.AddGcsFlags(configSettingCmd.Flags())
	accountsCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			end-ledger: the ledger sequence number for the end of the export range (required)
			output-file: filename of the output file
			stdout: if set, output is printed to stdout

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			 end time as a replacement for end sequence numbers
	*/
}
