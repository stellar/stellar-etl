package transform

import (
	"fmt"
	"strconv"

	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/xdr"
)

// TransformLedger converts a ledger from the history archive ingestion system into a form suitable for BigQuery
func TransformLedger(inputLedger historyarchive.Ledger) (LedgerOutput, error) {
	ledgerHeader := inputLedger.Header.Header

	outputSequence := uint32(ledgerHeader.LedgerSeq)

	outputLedgerID := toid.New(int32(outputSequence), 0, 0).ToInt64()

	outputLedgerHash := utils.HashToHexString(inputLedger.Header.Hash)
	outputPreviousHash := utils.HashToHexString(ledgerHeader.PreviousLedgerHash)

	outputLedgerHeader, err := xdr.MarshalBase64(ledgerHeader)
	if err != nil {
		return LedgerOutput{}, fmt.Errorf("for ledger %d (ledger id=%d): %v", outputSequence, outputLedgerID, err)
	}

	outputTransactionCount, outputOperationCount, outputSuccessfulCount, outputFailedCount, outputTxSetOperationCount, err := extractCounts(inputLedger)
	if err != nil {
		return LedgerOutput{}, fmt.Errorf("for ledger %d (ledger id=%d): %v", outputSequence, outputLedgerID, err)
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return LedgerOutput{}, err
	}

	outputTotalCoins := int64(ledgerHeader.TotalCoins)
	if outputTotalCoins < 0 {
		return LedgerOutput{}, fmt.Errorf("the total number of coins (%d) is negative for ledger %d (ledger id=%d)", outputTotalCoins, outputSequence, outputLedgerID)
	}

	outputFeePool := int64(ledgerHeader.FeePool)
	if outputFeePool < 0 {
		return LedgerOutput{}, fmt.Errorf("the fee pool (%d) is negative for ledger %d (ledger id=%d)", outputFeePool, outputSequence, outputLedgerID)
	}

	outputBaseFee := uint32(ledgerHeader.BaseFee)

	outputBaseReserve := uint32(ledgerHeader.BaseReserve)

	outputMaxTxSetSize := uint32(ledgerHeader.MaxTxSetSize)
	if int64(outputMaxTxSetSize) < int64(outputTransactionCount) {
		return LedgerOutput{}, fmt.Errorf("the transaction count is greater than the maximum transaction set size (%d > %d) for ledger %d (ledger id=%d)", outputTransactionCount, outputMaxTxSetSize, outputSequence, outputLedgerID)
	}

	outputProtocolVersion := uint32(ledgerHeader.LedgerVersion)

	transformedLedger := LedgerOutput{
		Sequence:                   outputSequence,
		LedgerID:                   outputLedgerID,
		LedgerHash:                 outputLedgerHash,
		PreviousLedgerHash:         outputPreviousHash,
		LedgerHeader:               outputLedgerHeader,
		TransactionCount:           outputTransactionCount,
		OperationCount:             outputOperationCount,
		SuccessfulTransactionCount: outputSuccessfulCount,
		FailedTransactionCount:     outputFailedCount,
		TxSetOperationCount:        outputTxSetOperationCount,
		ClosedAt:                   outputCloseTime,
		TotalCoins:                 outputTotalCoins,
		FeePool:                    outputFeePool,
		BaseFee:                    outputBaseFee,
		BaseReserve:                outputBaseReserve,
		MaxTxSetSize:               outputMaxTxSetSize,
		ProtocolVersion:            outputProtocolVersion,
	}
	return transformedLedger, nil
}

func TransactionProcessing(l xdr.LedgerCloseMeta) []xdr.TransactionResultMeta {
	switch l.V {
	case 0:
		return l.MustV0().TxProcessing
	case 1:
		return l.MustV1().TxProcessing
	case 2:
		return l.MustV2().TxProcessing
	default:
		panic(fmt.Sprintf("Unsupported LedgerCloseMeta.V: %d", l.V))
	}
}

func extractCounts(ledger historyarchive.Ledger) (transactionCount int32, operationCount int32, successTxCount int32, failedTxCount int32, txSetOperationCount string, err error) {
	transactions := GetTransactionSet(ledger)
	results := ledger.TransactionResult.TxResultSet.Results
	txCount := len(transactions)
	if txCount != len(results) {
		err = fmt.Errorf("The number of transactions and results are different (%d != %d)", txCount, len(results))
		return
	}

	txSetOperationCounter := int32(0)
	for i := 0; i < txCount; i++ {
		operations := transactions[i].Operations()
		numberOfOps := int32(len(operations))
		txSetOperationCounter += numberOfOps

		// for successful transactions, the operation count is based on the operations results slice
		if results[i].Result.Successful() {
			operationResults, ok := results[i].Result.OperationResults()
			if !ok {
				err = fmt.Errorf("Could not access operation results for result %d", i)
				return
			}

			successTxCount++
			operationCount += int32(len(operationResults))
		} else {
			failedTxCount++
		}

	}
	transactionCount = int32(txCount) - failedTxCount
	txSetOperationCount = strconv.FormatInt(int64(txSetOperationCounter), 10)
	return
}

func GetTransactionSet(transactionEntry historyarchive.Ledger) (transactionProcessing []xdr.TransactionEnvelope) {
	switch transactionEntry.Transaction.Ext.V {
	case 0:
		return transactionEntry.Transaction.TxSet.Txs
	case 1:
		return getTransactionPhase(transactionEntry.Transaction.Ext.GeneralizedTxSet.V1TxSet.Phases)
	default:
		panic(fmt.Sprintf("Unsupported TransactionHistoryEntry.Ext: %d", transactionEntry.Transaction.Ext.V))
	}
}

func getTransactionPhase(transactionPhase []xdr.TransactionPhase) (transactionEnvelope []xdr.TransactionEnvelope) {
	transactionSlice := []xdr.TransactionEnvelope{}
	for _, phase := range transactionPhase {
		switch phase.V {
		case 0:
			components := phase.MustV0Components()
			for _, component := range components {
				switch component.Type {
				case 0:
					transactionSlice = append(transactionSlice, component.TxsMaybeDiscountedFee.Txs...)

				default:
					panic(fmt.Sprintf("Unsupported TxSetComponentType: %d", component.Type))
				}

			}
		default:
			panic(fmt.Sprintf("Unsupported TransactionPhase.V: %d", phase.V))
		}
	}
	return transactionSlice

}
