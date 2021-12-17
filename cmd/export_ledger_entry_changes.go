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
				transformedAccounts := []transform.AccountOutput{}
				transformedClaimableBalances := []transform.ClaimableBalanceOutput{}
				transformedOffers := []transform.OfferOutput{}
				transformedTrustlines := []transform.TrustlineOutput{}
				transformedPools := []transform.PoolOutput{}
				for entryType, changes := range batch.Changes {
					switch entryType {
					case xdr.LedgerEntryTypeAccount:
						for _, change := range changes {
							acc, err := transform.TransformAccount(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming account entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedAccounts = append(transformedAccounts, acc)
						}
					case xdr.LedgerEntryTypeClaimableBalance:
						for _, change := range changes {
							balance, err := transform.TransformClaimableBalance(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming balance entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedClaimableBalances = append(transformedClaimableBalances, balance)
						}
					case xdr.LedgerEntryTypeOffer:
						for _, change := range changes {
							offer, err := transform.TransformOffer(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming offer entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedOffers = append(transformedOffers, offer)
						}
					case xdr.LedgerEntryTypeTrustline:
						for _, change := range changes {
							trust, err := transform.TransformTrustline(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming trustline entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedTrustlines = append(transformedTrustlines, trust)
						}
					case xdr.LedgerEntryTypeLiquidityPool:
						for _, change := range changes {
							pool, err := transform.TransformPool(change)
							if err != nil {
								entry, _, _, _ := utils.ExtractEntryFromChange(change)
								cmdLogger.LogError(fmt.Errorf("error transforming liquidity pool entry last updated at %d: %s", entry.LastModifiedLedgerSeq, err))
								continue
							}
							transformedPools = append(transformedPools, pool)
						}
					}
				}

				err := exportTransformedData(batch.BatchStart, batch.BatchEnd, outputFolder, transformedAccounts, transformedClaimableBalances, transformedOffers, transformedTrustlines, transformedPools, gcpCredentials, gcsBucket, extra)
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
	accounts []transform.AccountOutput,
	balances []transform.ClaimableBalanceOutput,
	offers []transform.OfferOutput,
	trusts []transform.TrustlineOutput,
	pools []transform.PoolOutput,
	gcpCredentials, gcsBucket string,
	extra map[string]string) error {

	accountsPath := filepath.Join(folderPath, exportFilename(start, end, "accounts"))
	balancesPath := filepath.Join(folderPath, exportFilename(start, end, "claimable_balances"))
	offersPath := filepath.Join(folderPath, exportFilename(start, end, "offers"))
	trustPath := filepath.Join(folderPath, exportFilename(start, end, "trustlines"))
	poolPath := filepath.Join(folderPath, exportFilename(start, end, "liquidity_pools"))

	accountFile := mustOutFile(accountsPath)
	balancesFile := mustOutFile(balancesPath)
	offersFile := mustOutFile(offersPath)
	trustFile := mustOutFile(trustPath)
	poolFile := mustOutFile(poolPath)

	for _, acc := range accounts {
		_, err := exportEntry(acc, accountFile, extra)
		if err != nil {
			return err
		}
	}

	for _, bal := range balances {
		_, err := exportEntry(bal, balancesFile, extra)
		if err != nil {
			return err
		}
	}

	for _, off := range offers {
		_, err := exportEntry(off, offersFile, extra)
		if err != nil {
			return err
		}
	}

	for _, trust := range trusts {
		_, err := exportEntry(trust, trustFile, extra)
		if err != nil {
			return err
		}
	}

	for _, pool := range pools {
		_, err := exportEntry(pool, poolFile, extra)
		if err != nil {
			return err
		}
	}

	maybeUpload(gcpCredentials, gcsBucket, accountsPath)
	maybeUpload(gcpCredentials, gcsBucket, balancesPath)
	maybeUpload(gcpCredentials, gcsBucket, offersPath)
	maybeUpload(gcpCredentials, gcsBucket, trustPath)
	maybeUpload(gcpCredentials, gcsBucket, poolPath)
	return nil
}

func createChangeChannels(exportAccounts, exportOffers, exportTrustlines, exportPools bool) (accChan, offChan, trustChan, poolChan chan input.ChangeBatch) {
	if exportAccounts {
		accChan = make(chan input.ChangeBatch)
	}

	if exportOffers {
		offChan = make(chan input.ChangeBatch)
	}

	if exportTrustlines {
		trustChan = make(chan input.ChangeBatch)
	}

	if exportPools {
		poolChan = make(chan input.ChangeBatch)
	}

	return
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
