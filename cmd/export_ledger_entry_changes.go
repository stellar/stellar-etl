package cmd

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
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
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest, isFuture)

		execPath, configPath, startNum, batchSize, outputFolder := utils.MustCoreFlags(cmd.Flags(), cmdLogger)
		exports := utils.MustExportTypeFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)

		err := os.MkdirAll(outputFolder, os.ModePerm)
		if err != nil {
			cmdLogger.Fatalf("unable to mkdir %s: %v", outputFolder, err)
		}

		if batchSize <= 0 {
			cmdLogger.Fatalf("batch-size (%d) must be greater than 0", batchSize)
		}

		// If none of the export flags are set, then we assume that everything should be exported
		allFalse := true
		for _, value := range exports {
			if true == value {
				allFalse = false
				break
			}
		}

		if allFalse {
			for export_name, _ := range exports {
				exports[export_name] = true
			}
		}

		if configPath == "" && endNum == 0 {
			cmdLogger.Fatal("stellar-core needs a config file path when exporting ledgers continuously (endNum = 0)")
		}

		execPath, err = filepath.Abs(execPath)
		if err != nil {
			cmdLogger.Fatal("could not get absolute filepath for stellar-core executable: ", err)
		}

		configPath, err = filepath.Abs(configPath)
		if err != nil {
			cmdLogger.Fatal("could not get absolute filepath for the config file: ", err)
		}

		core, err := input.PrepareCaptiveCore(execPath, configPath, startNum, endNum, env)
		if err != nil {
			cmdLogger.Fatal("error creating a prepared captive core instance: ", err)
		}

		if endNum == 0 {
			endNum = math.MaxInt32
		}

		changeChan := make(chan input.ChangeBatch)
		closeChan := make(chan int)
		go input.StreamChanges(core, startNum, endNum, batchSize, changeChan, closeChan, env, cmdLogger)

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
				}

				for entryType, changes := range batch.Changes {
					switch entryType {
					case xdr.LedgerEntryTypeAccount:
						if !exports["export-accounts"] {
							continue
						}
						for i, change := range changes.Changes {
							if changed, err := change.AccountChangedExceptSigners(); err != nil {
								cmdLogger.LogError(fmt.Errorf("unable to identify changed accounts: %v", err))
								continue
							} else if changed {

								acc, err := transform.TransformAccount(change, changes.LedgerHeaders[i])
								if err != nil {
									entry, _, _, _ := utils.ExtractEntryFromChange(change)
									cmdLogger.LogError(fmt.Errorf("error transforming account entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
									continue
								}
								transformedOutputs["accounts"] = append(transformedOutputs["accounts"], acc)
							}
							if change.AccountSignersChanged() {
								signers, err := transform.TransformSigners(change, changes.LedgerHeaders[i])
								if err != nil {
									entry, _, _, _ := utils.ExtractEntryFromChange(change)
									cmdLogger.LogError(fmt.Errorf("error transforming account signers from %d :%s", entry.LastModifiedLedgerSeq, err))
									continue
								}
								for _, s := range signers {
									transformedOutputs["signers"] = append(transformedOutputs["signers"], s)
								}
							}
						}
					case xdr.LedgerEntryTypeClaimableBalance:
						if !exports["export-balances"] {
							continue
						}
						for i, change := range changes.Changes {
							balance, err := transform.TransformClaimableBalance(change, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming balance entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["claimable_balances"] = append(transformedOutputs["claimable_balances"], balance)
						}
					case xdr.LedgerEntryTypeOffer:
						if !exports["export-offers"] {
							continue
						}
						for i, change := range changes.Changes {
							offer, err := transform.TransformOffer(change, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming offer entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["offers"] = append(transformedOutputs["offers"], offer)
						}
					case xdr.LedgerEntryTypeTrustline:
						if !exports["export-trustlines"] {
							continue
						}
						for i, change := range changes.Changes {
							trust, err := transform.TransformTrustline(change, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming trustline entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["trustlines"] = append(transformedOutputs["trustlines"], trust)
						}
					case xdr.LedgerEntryTypeLiquidityPool:
						if !exports["export-pools"] {
							continue
						}
						for i, change := range changes.Changes {
							pool, err := transform.TransformPool(change, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming liquidity pool entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["liquidity_pools"] = append(transformedOutputs["liquidity_pools"], pool)
						}
					case xdr.LedgerEntryTypeContractData:
						if !exports["export-contract-data"] {
							continue
						}
						for i, change := range changes.Changes {
							TransformContractData := transform.NewTransformContractDataStruct(transform.AssetFromContractData, transform.ContractBalanceFromContractData)
							contractData, err, _ := TransformContractData.TransformContractData(change, env.NetworkPassphrase, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming contract data entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}

							// Empty contract data that has no error is a nonce. Does not need to be recorded
							if contractData == (transform.ContractDataOutput{}) {
								continue
							}

							transformedOutputs["contract_data"] = append(transformedOutputs["contract_data"], contractData)
						}
					case xdr.LedgerEntryTypeContractCode:
						if !exports["export-contract-code"] {
							continue
						}
						for i, change := range changes.Changes {
							contractCode, err := transform.TransformContractCode(change, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming contract code entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["contract_code"] = append(transformedOutputs["contract_code"], contractCode)
						}
					case xdr.LedgerEntryTypeConfigSetting:
						if !exports["export-config-settings"] {
							continue
						}
						for i, change := range changes.Changes {
							configSettings, err := transform.TransformConfigSetting(change, changes.LedgerHeaders[i])
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming config settings entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["config_settings"] = append(transformedOutputs["config_settings"], configSettings)
						}
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

				err := exportTransformedData(batch.BatchStart, batch.BatchEnd, outputFolder, transformedOutputs, cloudCredentials, cloudStorageBucket, cloudProvider, extra)
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
	transformedOutput map[string][]interface{},
	cloudCredentials, cloudStorageBucket, cloudProvider string,
	extra map[string]string) error {

	for resource, output := range transformedOutput {
		// Filenames are typically exclusive of end point. This processor
		// is different and we have to increment by 1 since the end batch number
		// is included in this filename.
		path := filepath.Join(folderPath, exportFilename(start, end+1, resource))
		outFile := mustOutFile(path)
		for _, o := range output {
			_, err := exportEntry(o, outFile, extra)
			if err != nil {
				return err
			}
		}
		maybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
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
	exportLedgerEntryChangesCmd.MarkFlagRequired("core-executable")
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
