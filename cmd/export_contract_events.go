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

var contractEventsCmd = &cobra.Command{
	Use:   "export_contract_events",
	Short: "Exports the contract events over a specified range.",
	Long: `Exports the contract events over a specified range. Ledgers are
processed in batches of batch-size; each batch produces one file named
{start}-{end}-contract_events.txt in the output folder.`,
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

				path := filepath.Join(outputFolder, exportFilename(batch.BatchStart, batch.BatchEnd+1, "contract_events"))
				parquetPath := filepath.Join(parquetOutputFolder, exportParquetFilename(batch.BatchStart, batch.BatchEnd+1, "contract_events"))
				outFile := MustOutFile(path)
				var transformedEvents []transform.SchemaParquet

				for _, lcm := range batch.Ledgers {
					txInputs, err := input.TransactionsFromLedger(lcm, env.NetworkPassphrase)
					if err != nil {
						cmdLogger.LogError(fmt.Errorf("could not read transactions from ledger %d: %v", lcm.LedgerSequence(), err))
						continue
					}
					for _, txInput := range txInputs {
						totalAttempts++
						events, err := transform.TransformContractEvent(txInput.Transaction, txInput.LedgerHistory)
						if err != nil {
							ledgerSeq := txInput.LedgerHistory.Header.LedgerSeq
							cmdLogger.LogError(fmt.Errorf("could not transform contract events for transaction %d in ledger %d: %v", txInput.Transaction.Index, ledgerSeq, err))
							totalFailures++
							continue
						}
						for _, event := range events {
							if _, err := ExportEntry(event, outFile, commonArgs.Extra); err != nil {
								cmdLogger.LogError(fmt.Errorf("could not export contract event: %v", err))
								totalFailures++
								continue
							}
							if commonArgs.WriteParquet {
								transformedEvents = append(transformedEvents, event)
							}
						}
					}
				}

				outFile.Close()
				MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, path)
				if commonArgs.WriteParquet {
					WriteParquet(transformedEvents, parquetPath, new(transform.ContractEventOutputParquet))
					MaybeUpload(cloudCredentials, cloudStorageBucket, cloudProvider, parquetPath)
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(contractEventsCmd)
	utils.AddCommonFlags(contractEventsCmd.Flags())
	utils.AddHistoryArchiveFlags("contract_events", contractEventsCmd.Flags(), "exported_contract_events/")
	utils.AddCloudStorageFlags(contractEventsCmd.Flags())
	contractEventsCmd.MarkFlagRequired("start-ledger")
	contractEventsCmd.MarkFlagRequired("end-ledger")
}
