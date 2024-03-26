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

// offersCmd represents the offers command
var offersCmd = &cobra.Command{
	Use:   "export_offers",
	Short: "Exports the data on offers made from the genesis ledger to a specified endpoint.",
	Long: `Exports historical offer data from the genesis ledger to the provided end-ledger to an output file. 
	The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
	should be used in an initial data dump. In order to get offer information within a specified ledger range, see 
	the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra, _, datastoreUrl := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture, datastoreUrl)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		offers, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeOffer, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read offers: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		var header xdr.LedgerHeaderHistoryEntry
		for _, offer := range offers {
			transformed, err := transform.TransformOffer(offer, header)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform offer %+v: %v", offer, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export offer %+v: %v", offer, err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}

		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(offers), numFailures)

		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
	},
}

func init() {
	rootCmd.AddCommand(offersCmd)
	utils.AddCommonFlags(offersCmd.Flags())
	utils.AddBucketFlags("offers", offersCmd.Flags())
	utils.AddCloudStorageFlags(offersCmd.Flags())
	offersCmd.MarkFlagRequired("end-ledger")
	/*
		Current flags:
			end-ledger: the ledger sequence number for the end of the export range (required)
			output-file: filename of the output file

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			end time as a replacement for end sequence numbers
	*/
}
