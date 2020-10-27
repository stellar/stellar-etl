package cmd

import (
	"math"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

// exportOrderbooksCmd represents the exportOrderbooks command
var exportOrderbooksCmd = &cobra.Command{
	Use:   "export_orderbooks",
	Short: "This command exports the historical orderbooks",
	Long: `This command instantiates a stellar-core instance and uses it to export about orderbooks.
	The information is exported in batches determined by the batch-size flag.
	
	If the end-ledger is omitted, then the stellar-core node will continue running and exporting information as new ledgers are 
	confirmed by the Stellar network.`,
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

		core, err := input.PrepareCaptiveCore(execPath, configPath, checkpointSeq, endNum)
		if err != nil {
			cmdLogger.Fatal("error creating a prepared captive core instance: ", err)
		}

		checkpointSeq := utils.GetMostRecentCheckpoint(startNum)
		orderbook, err := input.GetEntriesFromGenesis(checkpointSeq, xdr.LedgerEntryTypeOffer)
		if err != nil {
			cmdLogger.Fatal("could not read inital orderbook: ", err)
		}

		
		orderbookChannel := make(chan transform.NormalizedOfferOutput)

		// stream changes in batches of 1, allows for applying batches
		go input.StreamOrderbooks(core, startNum, endNum, 1, orderbookChannel, cmdLogger)
		if endNum != 0 {
			batchCount := uint32(math.Ceil(float64(endNum-startNum+1) / float64(batchSize)))
			for i := uint32(0); i < batchCount; i++ {
				batchStart := startNum + i*batchSize
				// Subtract 1 from the end batch number because batches do not include the last batch in the range
				batchEnd := batchStart + batchSize - 1
				if batchEnd > endNum {
					batchEnd = endNum
				}

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

func init() {
	rootCmd.AddCommand(exportOrderbooksCmd)
	utils.AddCommonFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddCoreFlags(exportLedgerEntryChangesCmd.Flags())

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
	*/
}
