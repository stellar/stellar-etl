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

var dataCmd = &cobra.Command{
	Use:   "export_contract_data",
	Short: "Exports the contract data information.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture)
		path := utils.MustBucketFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		datas, err := input.GetEntriesFromGenesis(endNum, xdr.LedgerEntryTypeContractData, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("Error getting ledger entries: ", err)
		}

		outFile := mustOutFile(path)
		numFailures := 0
		totalNumBytes := 0
		for _, data := range datas {
			var header xdr.LedgerHeaderHistoryEntry
			TransformContractData := transform.NewTransformContractDataStruct(transform.AssetFromContractData, transform.ContractBalanceFromContractData)
			transformed, err, ok := TransformContractData.TransformContractData(data, env.NetworkPassphrase, header)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not transform contract data %+v: %v", data, err))
				numFailures += 1
				continue
			}

			if !ok {
				continue
			}

			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export contract data %+v: %v", data, err))
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}
		outFile.Close()
		cmdLogger.Info("Number of bytes written: ", totalNumBytes)

		printTransformStats(len(datas), numFailures)
		maybeUpload(gcpCredentials, gcsBucket, path)

	},
}

func init() {
	rootCmd.AddCommand(dataCmd)
	utils.AddCommonFlags(dataCmd.Flags())
	utils.AddBucketFlags("contract_data", dataCmd.Flags())
	utils.AddGcsFlags(dataCmd.Flags())
	dataCmd.MarkFlagRequired("end-ledger")
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
