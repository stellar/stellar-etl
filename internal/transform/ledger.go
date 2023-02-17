package transform

import (
	"fmt"
	"strconv"

	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

// TransformLedger converts a ledger from the history archive ingestion system into a form suitable for BigQuery
func TransformLedger(inputLedgerMeta xdr.SerializedLedgerCloseMeta) (LedgerOutput, error) {
	ledger, ok := inputLedgerMeta.GetV0()
	if !ok {
		return LedgerOutput{}, fmt.Errorf("Could not access the v0 information for given ledger")
	}

	outputSequence := uint32(ledger.V0.LedgerHeader.Header.LedgerSeq)

	outputLedgerID := toid.New(int32(outputSequence), 0, 0).ToInt64()

	outputLedgerHash := ledger.LedgerHash().HexString()
	outputPreviousHash := ledger.PreviousLedgerHash().HexString()

	outputLedgerHeader, err := xdr.MarshalBase64(ledger.V0.LedgerHeader.Header)
	if err != nil {
		return LedgerOutput{}, fmt.Errorf("for ledger %d (ledger id=%d): %v", outputSequence, outputLedgerID, err)
	}

	outputTransactionCount, outputOperationCount, outputSuccessfulCount, outputFailedCount, outputTxSetOperationCount, err := extractCounts(*ledger.V0)
	if err != nil {
		return LedgerOutput{}, fmt.Errorf("for ledger %d (ledger id=%d): %v", outputSequence, outputLedgerID, err)
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledger.V0.LedgerHeader.Header.ScpValue.CloseTime)
	if err != nil {
		return LedgerOutput{}, fmt.Errorf("for ledger %d (ledger id=%d): %v", outputSequence, outputLedgerID, err)
	}

	outputTotalCoins := int64(ledger.V0.LedgerHeader.Header.TotalCoins)
	if outputTotalCoins < 0 {
		return LedgerOutput{}, fmt.Errorf("The total number of coins (%d) is negative for ledger %d (ledger id=%d)", outputTotalCoins, outputSequence, outputLedgerID)
	}

	outputFeePool := int64(ledger.V0.LedgerHeader.Header.FeePool)
	if outputFeePool < 0 {
		return LedgerOutput{}, fmt.Errorf("The fee pool (%d) is negative for ledger %d (ledger id=%d)", outputFeePool, outputSequence, outputLedgerID)
	}

	outputBaseFee := uint32(ledger.V0.LedgerHeader.Header.BaseFee)

	outputBaseReserve := uint32(ledger.V0.LedgerHeader.Header.BaseReserve)

	outputMaxTxSetSize := uint32(ledger.V0.LedgerHeader.Header.MaxTxSetSize)
	if int64(outputMaxTxSetSize) < int64(outputTransactionCount) {
		return LedgerOutput{}, fmt.Errorf("The transaction count is greater than the maximum transaction set size (%d > %d) for ledger %d (ledger id=%d)", outputTransactionCount, outputMaxTxSetSize, outputSequence, outputLedgerID)
	}

	outputProtocolVersion := uint32(ledger.V0.LedgerHeader.Header.LedgerVersion)

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

func extractCounts(lcm xdr.LedgerCloseMetaV0) (transactionCount int32, operationCount int32, successTxCount int32, failedTxCount int32, txSetOperationCount string, err error) {
	transactions := lcm.TxSet.Txs
	results := lcm.TxProcessing
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
