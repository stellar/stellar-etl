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
	Long: `Exports historical config settings data from the genesis ledger to the provided end-ledger to an output file. 
	The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
	should be used in an initial data dump. In order to get offer information within a specified ledger range, see 
	the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		env := utils.GetEnvironmentDetails(commonArgs.IsTest, commonArgs.IsFuture, commonArgs.DatastorePath)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		settings, err := input.GetEntriesFromGenesis(commonArgs.EndNum, xdr.LedgerEntryTypeConfigSetting, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("Error getting ledger entries: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		var header xdr.LedgerHeaderHistoryEntry
		for _, setting := range settings {
			transformed, err := transform.TransformConfigSetting(setting, header)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform config setting %+v: %v", setting, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, commonArgs.Extra)
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
		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)

	},
}

func init() {
	rootCmd.AddCommand(configSettingCmd)
	utils.AddCommonFlags(configSettingCmd.Flags())
	utils.AddBucketFlags("config_settings", configSettingCmd.Flags())
	utils.AddCloudStorageFlags(configSettingCmd.Flags())
	configSettingCmd.MarkFlagRequired("end-ledger")
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
