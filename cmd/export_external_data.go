package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var externalDataCmd = &cobra.Command{
	Use:   "export_external_data",
	Short: "Exports external data updated over a specified timestamp range",
	Long:  "Exports external data updated over a specified timestamp range to an output file.",
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		timestampArgs := utils.MustTimestampRangeFlags(cmd.Flags(), cmdLogger)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		provider := utils.MustProviderFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0

		switch provider {
		case "retool":
			entities, err := input.GetEntityData[utils.RetoolEntityDataTransformInput](nil, provider, timestampArgs.StartTime, timestampArgs.EndTime)
			if err != nil {
				cmdLogger.Fatal("could not read entity data: ", err)
			}

			for _, transformInput := range entities {
				transformed, err := transform.TransformRetoolEntityData(transformInput)
				if err != nil {
					numFailures += 1
					continue
				}

				numBytes, err := exportEntry(transformed, outFile, nil)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export entity data: %v", err))
					numFailures += 1
					continue
				}
				totalNumBytes += numBytes
			}
			outFile.Close()
			cmdLogger.Info("Number of bytes written: ", totalNumBytes)

			printTransformStats(len(entities), numFailures)

		default:
			panic("unsupported provider: " + provider)
		}

		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(externalDataCmd)
	utils.AddArchiveFlags("entity", externalDataCmd.Flags())
	utils.AddCloudStorageFlags(externalDataCmd.Flags())
	utils.AddTimestampRangeFlags(externalDataCmd.Flags())
	utils.AddProviderFlags(externalDataCmd.Flags())
	externalDataCmd.MarkFlagRequired("provider")
	externalDataCmd.MarkFlagRequired("start-time")
	externalDataCmd.MarkFlagRequired("end-time")
}
