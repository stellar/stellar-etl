package cmd

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var exportLedgerEntryChangesCmd = &cobra.Command{
	Use:   "export_ledger_entry_changes",
	Short: "This command exports the changes in accounts, offers, trustlines and liquidity pools.",
	Long: `This command instantiates a stellar-core instance and uses it to export about accounts, offers, trustlines and liquidity pools.
The information is exported in batches determined by the batch-size flag. Each exported file will include the changes to the
relevant data type that occurred during that batch.

If the end-ledger is omitted, then the stellar-core node will continue running and exporting information as new ledgers are
confirmed by the Stellar network.

If no data type flags are set, then by default all of them are exported. If any are set, it is assumed that the others should not
be exported.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		env := utils.GetEnvironmentDetails(commonArgs)

		_, configPath, startNum, batchSize, outputFolder, parquetOutputFolder := utils.MustCoreFlags(cmd.Flags(), cmdLogger)
		exports := utils.MustExportTypeFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		cmd.Flags()

		err := os.MkdirAll(outputFolder, os.ModePerm)
		if err != nil {
			cmdLogger.Fatalf("unable to mkdir %s: %v", outputFolder, err)
		}

		err = os.MkdirAll(parquetOutputFolder, os.ModePerm)
		if err != nil {
			cmdLogger.Fatalf("unable to mkdir %s: %v", parquetOutputFolder, err)
		}

		if batchSize <= 0 {
			cmdLogger.Fatalf("batch-size (%d) must be greater than 0", batchSize)
		}

		// If none of the export flags are set, then we assume that everything should be exported
		allFalse := true
		for _, value := range exports {
			if value {
				allFalse = false
				break
			}
		}

		if allFalse {
			for export_name := range exports {
				exports[export_name] = true
			}
		}

		if configPath == "" && commonArgs.EndNum == 0 {
			cmdLogger.Fatal("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)")
		}

		ctx := context.Background()
		backend, err := utils.CreateLedgerBackend(ctx, commonArgs.UseCaptiveCore, env)
		if err != nil {
			cmdLogger.Fatal("error creating a cloud storage backend: ", err)
		}

		err = backend.PrepareRange(ctx, ledgerbackend.BoundedRange(startNum, commonArgs.EndNum))
		if err != nil {
			cmdLogger.Fatal("error preparing ledger range for cloud storage backend: ", err)
		}

		if commonArgs.EndNum == 0 {
			commonArgs.EndNum = math.MaxInt32
		}

		changeChan := make(chan input.ChangeBatch)
		closeChan := make(chan int)
		go input.StreamChanges(&backend, startNum, commonArgs.EndNum, batchSize, changeChan, closeChan, env, cmdLogger)

		for {
			select {
			case <-closeChan:
				return
			case batch, ok := <-changeChan:
				if !ok {
					continue
				}
				transformedOutputs := map[string][]interface{}{
					"accounts":           {},
					"signers":            {},
					"claimable_balances": {},
					"offers":             {},
					"trustlines":         {},
					"liquidity_pools":    {},
					"contract_data":      {},
					"contract_code":      {},
					"config_settings":    {},
					"ttl":                {},
					"ttp_core_events":    {},
					"history_contract":   {},
				}

				for _, lcm := range batch.LCM {
					ttp, _ := transform.TransformTokenTransfer(lcm, "Public Global Stellar Network ; September 2015")
					for _, ttpSingle := range ttp {
						transformedOutputs["ttp_core_events"] = append(transformedOutputs["ttp_core_events"], ttpSingle)
					}
				}
				for _, lcm := range batch.LCM {
					txReader, _ := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta("Public Global Stellar Network ; September 2015", lcm)
					lhe := txReader.GetHeader()
					txSlice := []input.LedgerTransformInput{}

					for -1 < 0 {
						tx, err := txReader.Read()
						if err == io.EOF {
							break
						}

						txSlice = append(txSlice, input.LedgerTransformInput{
							Transaction:     tx,
							LedgerHistory:   lhe,
							LedgerCloseMeta: lcm,
						})
					}
					for _, tx := range txSlice {
						events, _ := transform.TransformContractEvent(tx.Transaction, tx.LedgerHistory)
						for _, event := range events {
							transformedOutputs["history_contract"] = append(transformedOutputs["history_contract"], event)
						}
					}
				}
				for entryType, changes := range batch.Changes {
					switch entryType {
					case xdr.LedgerEntryTypeTtl:
						if !exports["export-ttl"] {
							continue
						}
						for i, change := range changes.Changes {
							ttl, err := transform.TransformTtl(change, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming ttl entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["ttl"] = append(transformedOutputs["ttl"], ttl)
						}
					}
				}

				err := exportTransformedData(
					batch.BatchStart,
					batch.BatchEnd,
					outputFolder,
					parquetOutputFolder,
					transformedOutputs,
					cloudCredentials,
					cloudStorageBucket,
					cloudProvider,
					commonArgs.Extra,
					commonArgs.WriteParquet,
				)
				if err != nil {
					cmdLogger.LogError(err)
					continue
				}
			}
		}
	},
}

func exportTransformedData(
	start, end uint32,
	folderPath string,
	parquetFolderPath string,
	transformedOutput map[string][]interface{},
	cloudCredentials, cloudStorageBucket, cloudProvider string,
	extra map[string]string,
	writeParquet bool) error {

	for resource, output := range transformedOutput {
		// Filenames are typically exclusive of end point. This processor
		// is different and we have to increment by 1 since the end batch number
		// is included in this filename.
		path := filepath.Join(folderPath, exportFilename(start, end+1, resource))
		parquetPath := filepath.Join(parquetFolderPath, exportParquetFilename(start, end+1, resource))
		outFile := MustOutFile(path)
		var transformedResource []transform.SchemaParquet
		var parquetSchema interface{}
		var skip bool
		for _, o := range output {
			_, err := ExportEntry(o, outFile, extra)
			if err != nil {
				return err
			}

			if writeParquet {
				switch v := o.(type) {
				case transform.AccountOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.AccountOutputParquet)
					skip = false
				case transform.AccountSignerOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.AccountSignerOutputParquet)
					skip = false
				case transform.ClaimableBalanceOutput:
					// Skipping ClaimableBalanceOutputParquet because it is not needed in the current scope of work
					// Note that ClaimableBalanceOutputParquet uses nested structs that will need to be handled
					// for parquet conversion
					skip = true
				case transform.ConfigSettingOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.ConfigSettingOutputParquet)
					skip = false
				case transform.ContractCodeOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.ContractCodeOutputParquet)
					skip = false
				case transform.ContractDataOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.ContractDataOutputParquet)
					skip = false
				case transform.PoolOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.PoolOutputParquet)
					skip = false
				case transform.OfferOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.OfferOutputParquet)
					skip = false
				case transform.TrustlineOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.TrustlineOutputParquet)
					skip = false
				case transform.TtlOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.TtlOutputParquet)
					skip = false
				case transform.TokenTransferOutput:
					transformedResource = append(transformedResource, v)
					parquetSchema = new(transform.TokenTransferOutputParquet)
					skip = false
				}
			}
		}

		MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)

		if !skip && writeParquet {
			WriteParquet(transformedResource, parquetPath, parquetSchema)
			MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
		}
	}

	return nil
}

func init() {
	rootCmd.AddCommand(exportLedgerEntryChangesCmd)
	utils.AddCommonFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddCoreFlags(exportLedgerEntryChangesCmd.Flags(), "changes_output/")
	utils.AddExportTypeFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddCloudStorageFlags(exportLedgerEntryChangesCmd.Flags())

	exportLedgerEntryChangesCmd.MarkFlagRequired("start-ledger")
	/*
		Current flags:
			start-ledger: the ledger sequence number for the beginning of the export period
			end-ledger: the ledger sequence number for the end of the export range

			output-folder: folder that will contain the output files
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
