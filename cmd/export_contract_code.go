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

var codeCmd = &cobra.Command{
	Use:   "export_contract_code",
	Short: "Exports the contract code information.",
	Long:  `Exports historical contract code data from the genesis ledger to the provided end-ledger to an output file. 
	The command reads from the bucket list, which includes the full history of the Stellar ledger. As a result, it 
	should be used in an initial data dump. In order to get offer information within a specified ledger range, see 
	the export_ledger_entry_changes command.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		codes, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeContractCode, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("Error getting ledger entries: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		var header xdr.LedgerHeaderHistoryEntry
		for _, code := range codes {
			transformed, err := transform.TransformContractCode(code, header)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform contract code %+v: %v", code, err))
				numFailures += 1
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export contract code %+v: %v", code, err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}
		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(codes), numFailures)
		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)

	},
}

func init() {
	rootCmd.AddCommand(codeCmd)
	utils.AddCommonFlags(codeCmd.Flags())
	utils.AddBucketFlags("contract_code", codeCmd.Flags())
	utils.AddCloudStorageFlags(codeCmd.Flags())
	codeCmd.MarkFlagRequired("end-ledger")
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
