package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/stellar-etl/v2/internal/input"
	"github.com/stellar/stellar-etl/v2/internal/transform"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

var effectsCmd = &cobra.Command{
	Use:   "export_effects",
	Short: "Exports the effects data over a specified range.",
	Long: `Exports the effects data over a specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-effects.txt in the output folder.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = commonArgs.StrictExport
		startNum, batchSize, outputFolder, parquetOutputFolder := utils.MustHistoryArchiveFlags(cmd.Flags(), cmdLogger)
		cloudStorageBucket, cloudCredentials, cloudProvider := utils.MustCloudStorageFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		if err := os.MkdirAll(outputFolder, os.ModePerm); err != nil {
			cmdLogger.Fatalf("unable to mkdir %s: %v", outputFolder, err)
		}
		if commonArgs.WriteParquet {
			if err := os.MkdirAll(parquetOutputFolder, os.ModePerm); err != nil {
				cmdLogger.Fatalf("unable to mkdir %s: %v", parquetOutputFolder, err)
			}
		}
		if batchSize == 0 {
			cmdLogger.Fatalf("batch-size (%d) must be greater than 0", batchSize)
		}

		ctx := context.Background()
		backend, err := utils.CreateLedgerBackend(ctx, commonArgs.UseCaptiveCore, env)
		if err != nil {
			cmdLogger.Fatal("could not create ledger backend: ", err)
		}
		if err := backend.PrepareRange(ctx, ledgerbackend.BoundedRange(startNum, commonArgs.EndNum)); err != nil {
			cmdLogger.Fatal("could not prepare ledger range: ", err)
		}

		batchChan := make(chan input.LedgerBatch)
		closeChan := make(chan int)
		go input.StreamLedgerBatches(&backend, startNum, commonArgs.EndNum, batchSize, batchChan, closeChan, cmdLogger)

		totalAttempts, totalFailures := 0, 0
		for {
			select {
			case <-closeChan:
				PrintTransformStats(totalAttempts, totalFailures)
				return
			case batch, ok := <-batchChan:
				if !ok {
					continue
				}

				path := filepath.Join(outputFolder, exportFilename(batch.BatchStart, batch.BatchEnd+1, "effects"))
				parquetPath := filepath.Join(parquetOutputFolder, exportParquetFilename(batch.BatchStart, batch.BatchEnd+1, "effects"))
				outFile := MustOutFile(path)
				var transformedEffects []transform.SchemaParquet

				for _, lcm := range batch.Ledgers {
					txInputs, err := input.TransactionsFromLedger(lcm, env.NetworkPassphrase)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not read transactions from ledger %d: %v", lcm.LedgerSequence(), err))
						continue
					}
					for _, txInput := range txInputs {
						totalAttempts++
						ledgerSeq := uint32(txInput.LedgerHistory.Header.LedgerSeq)
						effects, err := transform.TransformEffect(txInput.Transaction, ledgerSeq, txInput.LedgerCloseMeta, env.NetworkPassphrase)
						if err != nil {
							cmdLogger.LogError(fmt.Errorf("could not transform effects for transaction %d in ledger %d: %v", txInput.Transaction.Index, ledgerSeq, err))
							totalFailures++
							continue
						}

						for _, effect := range effects {
							if _, err := ExportEntry(effect, outFile, commonArgs.Extra); err != nil {
								cmdLogger.LogError(fmt.Errorf("could not export effect: %v", err))
								totalFailures++
								continue
							}
							if commonArgs.WriteParquet {
								transformedEffects = append(transformedEffects, effect)
							}
						}
					}
				}

				outFile.Close()
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
				if commonArgs.WriteParquet {
					WriteParquet(transformedEffects, parquetPath, new(transform.EffectOutputParquet))
					MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(effectsCmd)
	utils.AddCommonFlags(effectsCmd.Flags())
	utils.AddHistoryArchiveFlags("effects", effectsCmd.Flags(), "exported_effects/")
	utils.AddCloudStorageFlags(effectsCmd.Flags())
	effectsCmd.MarkFlagRequired("end-ledger")
}
