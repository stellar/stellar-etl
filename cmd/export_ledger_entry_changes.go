package cmd

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var exportLedgerEntryChangesCmd = &cobra.Command{
	Use:   "export_ledger_entry_changes",
	Short: "This command exports the changes in accounts, offers, and trustlines.",
	Long: `This command instantiates a stellar-core instance and uses it to export about accounts, offers, and trustlines.
The information is exported in batches determined by the batch-size flag. Each exported file will include the changes to the 
relevent data type that occurred during that batch.

If the end-ledger is omitted, then the stellar-core node will continue running and exporting information as new ledgers are 
confirmed by the Stellar network. 

If no data type flags are set, then by default all of them are exported. If any are set, it is assumed that the others should not
be exported.`,
	Run: func(cmd *cobra.Command, args []string) {
		endNum, useStdout, strictExport := utils.MustCommonFlags(cmd.Flags(), cmdLogger)

		execPath, configPath, startNum, batchSize, outputFolder := utils.MustCoreFlags(cmd.Flags(), cmdLogger)
		exportAccounts, exportOffers, exportTrustlines := utils.MustExportTypeFlags(cmd.Flags(), cmdLogger)

		var folderPath string
		if !useStdout {
			folderPath = mustCreateFolder(outputFolder)
		}

		if batchSize <= 0 {
			cmdLogger.Fatalf("batch-size (%d) must be greater than 0", batchSize)
		}

		// If none of the export flags are set, then we assume that everything should be exported
		if !exportAccounts && !exportOffers && !exportTrustlines {
			exportAccounts, exportOffers, exportTrustlines = true, true, true
		}

		if configPath == "" && endNum == 0 {
			cmdLogger.Fatal("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)")
		}

		var err error
		execPath, err = filepath.Abs(execPath)
		if err != nil {
			cmdLogger.Fatal("could not get absolute filepath for stellar-core executable: ", err)
		}

		configPath, err = filepath.Abs(configPath)
		if err != nil {
			cmdLogger.Fatal("could not get absolute filepath for the config file: ", err)
		}

		core, err := input.PrepareCaptiveCore(execPath, configPath, startNum, endNum)
		if err != nil {
			cmdLogger.Fatal("error creating a prepared captive core instance: ", err)
		}

		accChannel, offChannel, trustChannel := createChangeChannels(exportAccounts, exportOffers, exportTrustlines)

		go input.StreamChanges(core, startNum, endNum, batchSize, accChannel, offChannel, trustChannel, cmdLogger)
		if endNum != 0 {
			batchCount := uint32(math.Ceil(float64(endNum-startNum+1) / float64(batchSize)))
			for i := uint32(0); i < batchCount; i++ {
				batchStart := startNum + i*batchSize
				// Subtract 1 from the end batch number because batches do not include the last batch in the range
				batchEnd := batchStart + batchSize - 1
				if batchEnd > endNum {
					batchEnd = endNum
				}

				transformedAccounts, transformedOffers, transformedTrustlines := input.ReceiveChanges(accChannel, offChannel, trustChannel, strictExport, cmdLogger)
				exportTransformedData(batchStart, batchEnd, folderPath, useStdout, strictExport, transformedAccounts, transformedOffers, transformedTrustlines)
			}

		} else {
			var batchNum uint32 = 0
			for {
				batchStart := startNum + batchNum*batchSize
				batchEnd := batchStart + batchSize - 1
				transformedAccounts, transformedOffers, transformedTrustlines := input.ReceiveChanges(accChannel, offChannel, trustChannel, strictExport, cmdLogger)
				exportTransformedData(batchStart, batchEnd, folderPath, useStdout, strictExport, transformedAccounts, transformedOffers, transformedTrustlines)
				batchNum++
			}
		}
	},
}

func mustCreateFolder(path string) string {
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		cmdLogger.Fatal("could not get absolute filepath: ", err)
	}

	_, err = os.Stat(path)

	if os.IsNotExist(err) {
		err := os.Mkdir(path, 0777)
		if err != nil {
			cmdLogger.Fatal("could not create folder: ", err)
		}
	}

	return absolutePath
}

// exportEntry exports the provided entry, printing either to the file or to stdout.
func exportEntry(entry interface{}, file *os.File, useStdout, strictExport bool) {
	marshalled, err := json.Marshal(entry)
	if err != nil {
		if strictExport {
			cmdLogger.Fatal("could not json encode account", err)
		} else {
			cmdLogger.Warning("could not json encode account", err)
		}
	}

	if !useStdout {
		file.Write(marshalled)
		file.WriteString("\n")
	} else {
		fmt.Println(string(marshalled))
	}
}

func exportTransformedData(start, end uint32, folderPath string, useStdout, strictExport bool, accounts []transform.AccountOutput, offers []transform.OfferOutput, trusts []transform.TrustlineOutput) {
	var accountFile, offersFile, trustFile *os.File
	if !useStdout {
		accountFile = mustOutFile(filepath.Join(folderPath, fmt.Sprintf("%d-%d-accounts.txt", start, end)))
		offersFile = mustOutFile(filepath.Join(folderPath, fmt.Sprintf("%d-%d-offers.txt", start, end)))
		trustFile = mustOutFile(filepath.Join(folderPath, fmt.Sprintf("%d-%d-trustlines.txt", start, end)))
	}

	for _, acc := range accounts {
		exportEntry(acc, accountFile, useStdout, strictExport)
	}

	for _, off := range offers {
		exportEntry(off, offersFile, useStdout, strictExport)
	}

	for _, trust := range trusts {
		exportEntry(trust, trustFile, useStdout, strictExport)
	}
}

func createChangeChannels(exportAccounts, exportOffers, exportTrustlines bool) (accChan, offChan, trustChan chan input.ChangeBatch) {
	if exportAccounts {
		accChan = make(chan input.ChangeBatch)
	}

	if exportOffers {
		offChan = make(chan input.ChangeBatch)
	}

	if exportTrustlines {
		trustChan = make(chan input.ChangeBatch)
	}

	return
}

func init() {
	rootCmd.AddCommand(exportLedgerEntryChangesCmd)
	utils.AddCommonFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddCoreFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddExportTypeFlags(exportLedgerEntryChangesCmd.Flags())

	exportLedgerEntryChangesCmd.MarkFlagRequired("start-ledger")
	exportLedgerEntryChangesCmd.MarkFlagRequired("core-executable")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range

			output-folder: folder that will contain the output files
			stdout: if true, prints to stdout instead of the command line
			limit: maximum number of changes to export in a given batch; if negative then everything gets exported
			batch-size: size of the export batches

			core-executable: path to stellar-core executable
			core-config: path to stellar-core config file

			If none of the export_X flags are set, assume everything should be exported
				export_accounts: boolean flag; if set then accounts should be exported
				export_trustlines: boolean flag; if set then trustlines should be exported
				export_offers: boolean flag; if set then offers should be exported

		TODO: implement extra flags if possible
			serialize-method: the method for serialization of the output data (JSON, XDR, etc)
			start and end time as a replacement for start and end sequence numbers
	*/
}
