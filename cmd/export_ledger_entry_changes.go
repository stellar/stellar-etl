package cmd

import (
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
		startNum, endNum := utils.MustRangeFlags(cmd.Flags(), cmdLogger)

		path, useStdout := utils.MustOutputFlags(cmd.Flags(), cmdLogger)
		var outFile *os.File
		if !useStdout {
			outFile = mustOutFile(path)
		}

		execPath, configPath, exportAccounts, exportOffers, exportTrustlines, batchSize := utils.MustCoreFlags(cmd.Flags(), cmdLogger)

		//if none of the export flags are set, then we assume that everything should be exported
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
			batchCount := int(math.Ceil(float64(endNum-startNum+1) / float64(batchSize)))
			for i := 0; i < batchCount; i++ {
				transformedAccounts, transformedOffers, transformedTrustlines := input.ReceiveChanges(accChannel, offChannel, trustChannel, cmdLogger)
				fmt.Printf("Exporting batch %d/%d \n", i+1, batchCount)
				exportTransformedData(outFile, useStdout, transformedAccounts, transformedOffers, transformedTrustlines)
				fmt.Println("---------------------------------------")
			}

		} else {
			for {
				transformedAccounts, transformedOffers, transformedTrustlines := input.ReceiveChanges(accChannel, offChannel, trustChannel, cmdLogger)
				exportTransformedData(outFile, useStdout, transformedAccounts, transformedOffers, transformedTrustlines)
			}
		}
	},
}

func exportTransformedData(file *os.File, useStdout bool, accounts []transform.AccountOutput, offers []transform.OfferOutput, trusts []transform.TrustlineOutput) {
	// TODO: make exports of different types go to different files. Also make each batch export to its own file
	if !useStdout {
		file.WriteString(fmt.Sprint(accounts))
		file.WriteString("\n")
		file.WriteString(fmt.Sprint(offers))
		file.WriteString("\n")
		file.WriteString(fmt.Sprint(trusts))
		file.WriteString("\n")
	} else {
		fmt.Println("ACC: ", fmt.Sprint(accounts))
		fmt.Println("OFF: ", fmt.Sprint(offers))
		fmt.Println("TRU: ", fmt.Sprint(trusts))
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

	utils.AddOutputFlags("changes", exportLedgerEntryChangesCmd.Flags())
	utils.AddRangeFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddCoreFlags(exportLedgerEntryChangesCmd.Flags())

	exportLedgerEntryChangesCmd.MarkFlagRequired("start-ledger")
	exportLedgerEntryChangesCmd.MarkFlagRequired("core-executable")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range

			output-file: filename of the output file
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
