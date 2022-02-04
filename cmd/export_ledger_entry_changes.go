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
relevent data type that occurred during that batch.

If the end-ledger is omitted, then the stellar-core node will continue running and exporting information as new ledgers are 
confirmed by the Stellar network. 

If no data type flags are set, then by default all of them are exported. If any are set, it is assumed that the others should not
be exported.`,
	Run: func(cmd *cobra.Command, args []string) {
		endNum, strictExport, isTest, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		env := utils.GetEnvironmentDetails(isTest)

		execPath, configPath, startNum, batchSize, outputFolder := utils.MustCoreFlags(cmd.Flags(), cmdLogger)
		exportAccounts, exportOffers, exportTrustlines, exportPools := utils.MustExportTypeFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)

		err := os.MkdirAll(outputFolder, os.ModePerm)
		if err != nil {
			cmdLogger.Fatalf("unable to mkdir %s: %v", outputFolder, err)
		}

		if batchSize <= 0 {
			cmdLogger.Fatalf("batch-size (%d) must be greater than 0", batchSize)
		}

		// If none of the export flags are set, then we assume that everything should be exported
		if !exportAccounts && !exportOffers && !exportTrustlines && !exportPools {
			exportAccounts, exportOffers, exportTrustlines, exportPools = true, true, true, true
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

		if endNum != 0 {
			endNum = endNum + 1
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
				}
				for entryType, changes := range batch.Changes {
					switch entryType {
					case xdr.LedgerEntryTypeAccount:
						for _, change := range changes {
							if changed, err := change.AccountChangedExceptSigners(); err != nil {
								cmdLogger.LogError(fmt.Errorf("unable to identify changed accounts: %v", err))
								continue
							} else if changed {
								acc, err := transform.TransformAccount(change)
								if err != nil {
									entry, _, _, _ := utils.ExtractEntryFromChange(change)
									cmdLogger.LogError(fmt.Errorf("error transforming account entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
									continue
								}
								transformedOutputs["accounts"] = append(transformedOutputs["accounts"], acc)
							}
							if change.AccountSignersChanged() {
								signers, err := transform.TransformSigners(change)
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
						for _, change := range changes {
							balance, err := transform.TransformClaimableBalance(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming balance entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["claimable_balances"] = append(transformedOutputs["claimable_balances"], balance)
						}
					case xdr.LedgerEntryTypeOffer:
						for _, change := range changes {
							offer, err := transform.TransformOffer(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming offer entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["offers"] = append(transformedOutputs["offers"], offer)
						}
					case xdr.LedgerEntryTypeTrustline:
						for _, change := range changes {
							trust, err := transform.TransformTrustline(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming trustline entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["trustlines"] = append(transformedOutputs["trustlines"], trust)
						}
					case xdr.LedgerEntryTypeLiquidityPool:
						for _, change := range changes {
							pool, err := transform.TransformPool(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming liquidity pool entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOutputs["liquidity_pools"] = append(transformedOutputs["liquidity_pools"], pool)
						}
					}
				}

				err := exportTransformedData(batch.BatchStart, batch.BatchEnd, outputFolder, transformedOutputs, gcpCredentials, gcsBucket, extra)
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
	gcpCredentials, gcsBucket string,
	extra map[string]string) error {

	for resource, output := range transformedOutput {
		path := filepath.Join(folderPath, exportFilename(start, end, resource))
		outFile := mustOutFile(path)
		for _, o := range output {
			_, err := exportEntry(o, outFile, extra)
			if err != nil {
				return err
			}
		}
		maybeUpload(gcpCredentials, gcsBucket, path)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(exportLedgerEntryChangesCmd)
	utils.AddCommonFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddCoreFlags(exportLedgerEntryChangesCmd.Flags(), "changes_output/")
	utils.AddExportTypeFlags(exportLedgerEntryChangesCmd.Flags())
	utils.AddGcsFlags(exportLedgerEntryChangesCmd.Flags())

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
