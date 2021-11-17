package cmd

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

// exportOrderbooksCmd represents the exportOrderbooks command
var exportOrderbooksCmd = &cobra.Command{
	Use:   "export_orderbooks",
	Short: "This command exports the historical orderbooks",
	Long: `This command instantiates a stellar-core instance and uses it to export normalized orderbooks.
	The information is exported in batches determined by the batch-size flag. The normalized data is exported in multiple 
	different files within the exported data folder. These files are dimAccounts.txt, dimOffers.txt, dimMarkets.txt, and factEvents.txt.
	These files contain normalized data that helps save storage space. 
	
	If the end-ledger is omitted, then the stellar-core node will continue running and exporting information as new ledgers are 
	confirmed by the Stellar network. In this unbounded case, a stellar-core config path is required to utilize the Captive Core toml.`,
	Run: func(cmd *cobra.Command, args []string) {
		endNum, strictExport, isTest, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest)

		execPath, configPath, startNum, batchSize, outputFolder := utils.MustCoreFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		if batchSize <= 0 {
			cmdLogger.Fatalf("batch-size (%d) must be greater than 0", batchSize)
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

		checkpointSeq := utils.GetMostRecentCheckpoint(startNum)
		core, err := input.PrepareCaptiveCore(execPath, configPath, checkpointSeq, endNum, env)
		if err != nil {
			cmdLogger.Fatal("error creating a prepared captive core instance: ", err)
		}

		orderbook, err := input.GetEntriesFromGenesis(checkpointSeq, xdr.LedgerEntryTypeOffer, env.ArchiveURLs)
		if err != nil {
			cmdLogger.Fatal("could not read initial orderbook: ", err)
		}

		orderbookChannel := make(chan input.OrderbookBatch)

		go input.StreamOrderbooks(core, startNum, endNum, batchSize, orderbookChannel, orderbook, env, cmdLogger)

		// If the end sequence number is defined, we work in a closed range and export a finite number of batches
		if endNum != 0 {
			batchCount := uint32(math.Ceil(float64(endNum-startNum+1) / float64(batchSize)))
			for i := uint32(0); i < batchCount; i++ {
				batchStart := startNum + i*batchSize
				// Subtract 1 from the end batch number because batches do not include the last batch in the range
				batchEnd := batchStart + batchSize - 1
				if batchEnd > endNum {
					batchEnd = endNum
				}

				parser := input.ReceiveParsedOrderbooks(orderbookChannel, cmdLogger)
				exportOrderbook(batchStart, batchEnd, outputFolder, parser, gcpCredentials, gcsBucket, extra)
			}
		} else {
			// otherwise, we export in an unbounded manner where batches are constantly exported
			var batchNum uint32 = 0
			for {
				batchStart := startNum + batchNum*batchSize
				batchEnd := batchStart + batchSize - 1
				parser := input.ReceiveParsedOrderbooks(orderbookChannel, cmdLogger)
				exportOrderbook(batchStart, batchEnd, outputFolder, parser, gcpCredentials, gcsBucket, extra)
				batchNum++
			}
		}
	},
}

// writeSlice writes the slice either to a file.
func writeSlice(file *os.File, slice [][]byte, extra map[string]string) error {

	for _, data := range slice {
		bytesToWrite := data
		if len(extra) > 0 {
			i := map[string]interface{}{}
			decoder := json.NewDecoder(bytes.NewReader(data))
			decoder.UseNumber()
			err := decoder.Decode(&i)
			if err != nil {
				return err
			}
			for k, v := range extra {
				i[k] = v
			}
			bytesToWrite, err = json.Marshal(i)
			if err != nil {
				return err
			}
		}
		file.WriteString(string(bytesToWrite) + "\n")
	}

	file.Close()
	return nil
}

func exportOrderbook(
	start, end uint32,
	folderPath string,
	parser *input.OrderbookParser,
	gcpCredentials, gcsBucket string,
	extra map[string]string) {
	marketsFilePath := filepath.Join(folderPath, exportFilename(start, end, "dimMarkets"))
	offersFilePath := filepath.Join(folderPath, exportFilename(start, end, "dimOffers"))
	accountsFilePath := filepath.Join(folderPath, exportFilename(start, end, "dimAccounts"))
	eventsFilePath := filepath.Join(folderPath, exportFilename(start, end, "factEvents"))

	marketsFile := mustOutFile(marketsFilePath)
	offersFile := mustOutFile(offersFilePath)
	accountsFile := mustOutFile(accountsFilePath)
	eventsFile := mustOutFile(eventsFilePath)

	err := writeSlice(marketsFile, parser.Markets, extra)
	if err != nil {
		cmdLogger.LogError(err)
	}
	err = writeSlice(offersFile, parser.Offers, extra)
	if err != nil {
		cmdLogger.LogError(err)
	}
	err = writeSlice(accountsFile, parser.Accounts, extra)
	if err != nil {
		cmdLogger.LogError(err)
	}
	err = writeSlice(eventsFile, parser.Events, extra)
	if err != nil {
		cmdLogger.LogError(err)
	}

	maybeUpload(gcpCredentials, gcsBucket, marketsFilePath)
	maybeUpload(gcpCredentials, gcsBucket, offersFilePath)
	maybeUpload(gcpCredentials, gcsBucket, accountsFilePath)
	maybeUpload(gcpCredentials, gcsBucket, eventsFilePath)
}

func init() {
	rootCmd.AddCommand(exportOrderbooksCmd)
	utils.AddCommonFlags(exportOrderbooksCmd.Flags())
	utils.AddCoreFlags(exportOrderbooksCmd.Flags(), "orderbooks_output/")
	utils.AddGcsFlags(exportOrderbooksCmd.Flags())

	exportOrderbooksCmd.MarkFlagRequired("start-ledger")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range

			output-folder: folder that will contain the output files
			limit: maximum number of changes to export in a given batch; if negative then everything gets exported
			batch-size: size of the export batches

			core-executable: path to stellar-core executable
			core-config: path to stellar-core config file
	*/
}
