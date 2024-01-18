package cmd

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/stellar/stellar-etl/internal/input"
	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

var allHistoryCmd = &cobra.Command{
	Use:   "export_all_history",
	Short: "Exports all stellar network history.",
	Long: `Exports historical stellar network data between provided start-ledger/end-ledger to output files. 
This is a termporary command used to reduce the amount of requests to history archives 
in order to mitigate egress costs for the entity hosting history archives.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmdLogger.SetLevel(logrus.InfoLevel)
		endNum, strictExport, isTest, isFuture, extra := utils.MustCommonFlags(cmd.Flags(), cmdLogger)
		cmdLogger.StrictExport = strictExport
		startNum, _, limit := utils.MustArchiveFlags(cmd.Flags(), cmdLogger)
		gcsBucket, gcpCredentials := utils.MustGcsFlags(cmd.Flags(), cmdLogger)
		env := utils.GetEnvironmentDetails(isTest, isFuture)

		allHistory, err := input.GetAllHistory(startNum, endNum, limit, env)
		if err != nil {
			cmdLogger.Fatal("could not read all history: ", err)
		}

		getOperations(allHistory.Operations, extra, gcpCredentials, gcsBucket, "exported_operations.txt", env)
		getTrades(allHistory.Trades, extra, gcpCredentials, gcsBucket, "exported_trades.txt")
		getEffects(allHistory.Ledgers, extra, gcpCredentials, gcsBucket, "exported_effects.txt", env)
		getTransactions(allHistory.Ledgers, extra, gcpCredentials, gcsBucket, "exported_transactions.txt")
		getDiagnosticEvents(allHistory.Ledgers, extra, gcpCredentials, gcsBucket, "exported_diagnostic_events.txt")
	},
}

func getOperations(operations []input.OperationTransformInput, extra map[string]string, gcpCredentials string, gcsBucket string, path string, env utils.EnvironmentDetails) {
	outFileOperations := mustOutFile(path)
	numFailures := 0
	totalNumBytes := 0
	for _, transformInput := range operations {
		transformed, err := transform.TransformOperation(transformInput.Operation, transformInput.OperationIndex, transformInput.Transaction, transformInput.LedgerSeqNum, transformInput.LedgerCloseMeta, env.NetworkPassphrase)
		if err != nil {
			txIndex := transformInput.Transaction.Index
			cmdLogger.LogError(fmt.Errorf("could not transform operation %d in transaction %d in ledger %d: %v", transformInput.OperationIndex, txIndex, transformInput.LedgerSeqNum, err))
			numFailures += 1
			continue
		}

		numBytes, err := exportEntry(transformed, outFileOperations, extra)
		if err != nil {
			cmdLogger.LogError(fmt.Errorf("could not export operation: %v", err))
			numFailures += 1
			continue
		}
		totalNumBytes += numBytes
	}

	outFileOperations.Close()
	cmdLogger.Info("Number of bytes written: ", totalNumBytes)

	printTransformStats(len(operations), numFailures)

	maybeUpload(gcpCredentials, gcsBucket, path)
}

func getTrades(trades []input.TradeTransformInput, extra map[string]string, gcpCredentials string, gcsBucket string, path string) {
	outFile := mustOutFile(path)
	numFailures := 0
	totalNumBytes := 0
	for _, tradeInput := range trades {
		trades, err := transform.TransformTrade(tradeInput.OperationIndex, tradeInput.OperationHistoryID, tradeInput.Transaction, tradeInput.CloseTime)
		if err != nil {
			parsedID := toid.Parse(tradeInput.OperationHistoryID)
			cmdLogger.LogError(fmt.Errorf("from ledger %d, transaction %d, operation %d: %v", parsedID.LedgerSequence, parsedID.TransactionOrder, parsedID.OperationOrder, err))
			numFailures += 1
			continue
		}

		for _, transformed := range trades {
			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(err)
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}
	}

	outFile.Close()
	cmdLogger.Info("Number of bytes written: ", totalNumBytes)

	printTransformStats(len(trades), numFailures)

	maybeUpload(gcpCredentials, gcsBucket, path)
}

func getEffects(transactions []input.LedgerTransformInput, extra map[string]string, gcpCredentials string, gcsBucket string, path string, env utils.EnvironmentDetails) {
	outFile := mustOutFile(path)
	numFailures := 0
	totalNumBytes := 0
	for _, transformInput := range transactions {
		LedgerSeq := uint32(transformInput.LedgerHistory.Header.LedgerSeq)
		effects, err := transform.TransformEffect(transformInput.Transaction, LedgerSeq, transformInput.LedgerCloseMeta, env.NetworkPassphrase)
		if err != nil {
			txIndex := transformInput.Transaction.Index
			cmdLogger.Errorf("could not transform transaction %d in ledger %d: %v", txIndex, LedgerSeq, err)
			numFailures += 1
			continue
		}

		for _, transformed := range effects {
			numBytes, err := exportEntry(transformed, outFile, extra)
			if err != nil {
				cmdLogger.LogError(err)
				numFailures += 1
				continue
			}
			totalNumBytes += numBytes
		}
	}

	outFile.Close()
	cmdLogger.Info("Number of bytes written: ", totalNumBytes)

	printTransformStats(len(transactions), numFailures)

	maybeUpload(gcpCredentials, gcsBucket, path)
}

func getTransactions(transactions []input.LedgerTransformInput, extra map[string]string, gcpCredentials string, gcsBucket string, path string) {
	outFile := mustOutFile(path)
	numFailures := 0
	totalNumBytes := 0
	for _, transformInput := range transactions {
		transformed, err := transform.TransformTransaction(transformInput.Transaction, transformInput.LedgerHistory)
		if err != nil {
			ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
			cmdLogger.LogError(fmt.Errorf("could not transform transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
			numFailures += 1
			continue
		}

		numBytes, err := exportEntry(transformed, outFile, extra)
		if err != nil {
			cmdLogger.LogError(fmt.Errorf("could not export transaction: %v", err))
			numFailures += 1
			continue
		}
		totalNumBytes += numBytes
	}

	outFile.Close()
	cmdLogger.Info("Number of bytes written: ", totalNumBytes)

	printTransformStats(len(transactions), numFailures)

	maybeUpload(gcpCredentials, gcsBucket, path)
}

func getDiagnosticEvents(transactions []input.LedgerTransformInput, extra map[string]string, gcpCredentials string, gcsBucket string, path string) {
	outFile := mustOutFile(path)
	numFailures := 0
	for _, transformInput := range transactions {
		transformed, err, ok := transform.TransformDiagnosticEvent(transformInput.Transaction, transformInput.LedgerHistory)
		if err != nil {
			ledgerSeq := transformInput.LedgerHistory.Header.LedgerSeq
			cmdLogger.LogError(fmt.Errorf("could not transform diagnostic events in transaction %d in ledger %d: ", transformInput.Transaction.Index, ledgerSeq))
			numFailures += 1
			continue
		}

		if !ok {
			continue
		}
		for _, diagnosticEvent := range transformed {
			_, err := exportEntry(diagnosticEvent, outFile, extra)
			if err != nil {
				cmdLogger.LogError(fmt.Errorf("could not export diagnostic event: %v", err))
				numFailures += 1
				continue
			}
		}
	}

	outFile.Close()

	printTransformStats(len(transactions), numFailures)

	maybeUpload(gcpCredentials, gcsBucket, path)
}

func init() {
	rootCmd.AddCommand(allHistoryCmd)
	utils.AddCommonFlags(allHistoryCmd.Flags())
	utils.AddArchiveFlags("all_history", allHistoryCmd.Flags())
	utils.AddGcsFlags(allHistoryCmd.Flags())
	allHistoryCmd.MarkFlagRequired("end-ledger")
}
