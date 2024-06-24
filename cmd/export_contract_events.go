package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var contractEventsCmd = &cobra.Command{
	Use:   "export_contract_events",
	Short: "Exports the contract events over a specified range.",
	Long:  `Exports the contract events over a specified range to an output file.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		cmdArgs := utils.MustFlags(cmd.Flags(), cmdLogger)

		// TODO: https://stellarorg.atlassian.net/browse/HUBBLE-386 GetEnvironmentDetails should be refactored
		commonArgs := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(commonArgs)

		transactions, err := input.GetTransactions(cmdArgs.StartNum, cmdArgs.EndNum, cmdArgs.Limit, env, cmdArgs.UseCaptiveCore)
		if err != nil {
			cmdLogger.Fatal("could not read transactions: ", err)
		}

		outFile := mustOutFile(cmdArgs.Path)
		numFailures := 0
		for _, transformInput := range transactions {
			transformed, err := transform.TransformContractEvent(transformInput.Transaction, transformInput.LedgerHistory)
			if err != nil {
				ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
				cmdLogger.LogError(fmt.Errorf("could not transform contract events in transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
				numFailures += 1
				continue
			}

			for _, contractEvent := range transformed {
				_, err := exportEntry(contractEvent, outFile, cmdArgs.Extra)
				if err != nil {
					cmdLogger.LogError(fmt.Errorf("could not export contract event: %v", err))
					numFailures += 1
					continue
				}
			}
		}

		outFile.Close()

		printTransformStats(len(transactions), numFailures)

		maybeUpload(cmdArgs.Credentials, cmdArgs.Bucket, cmdArgs.Provider, cmdArgs.Path)
	},
}

func init() {
	rootCmd.AddCommand(contractEventsCmd)
	utils.AddCommonFlags(contractEventsCmd.Flags())
	utils.AddArchiveFlags("contract_events", contractEventsCmd.Flags())
	utils.AddCloudStorageFlags(contractEventsCmd.Flags())

	contractEventsCmd.MarkFlagRequired("start-ledger")
	contractEventsCmd.MarkFlagRequired("end-ledger")
}
