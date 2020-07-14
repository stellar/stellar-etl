package transform

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

//ConvertLedger converts a ledger from the history archive ingestion system into a form suitable for BigQuery
func ConvertLedger(inputLedgerMeta xdr.LedgerCloseMeta) (LedgerOutput, error) {
	ledger, ok := inputLedgerMeta.GetV0()
	if !ok {
		return LedgerOutput{}, errors.New("Could not access the version 0 information for the provided ledger")
	}
	ledgerHeaderHistory := ledger.LedgerHeader
	ledgerHeader := ledgerHeaderHistory.Header

	outputSequence := int32(ledgerHeader.LedgerSeq)
	if outputSequence < 0 {
		return LedgerOutput{}, errors.New("The sequence is a negative value")
	}

	outputLedgerHash := utils.HashToHexString(ledgerHeaderHistory.Hash)
	outputPreviousHash := utils.HashToHexString(ledgerHeader.PreviousLedgerHash)

	hashedLedgerHeader, err := xdr.MarshalBase64(ledgerHeader)
	if err != nil {
		return LedgerOutput{}, err
	}
	outputLedgerHeader := []byte(hashedLedgerHeader)

	outputTransactionCount, outputOperationCount, outputSuccessfulCount, outputFailedCount, outputTxSetOperationCount, err := extractCounts(ledger)
	if err != nil {
		return LedgerOutput{}, err
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return LedgerOutput{}, err
	}

	outputTotalCoins := int64(ledgerHeader.TotalCoins)
	if outputTotalCoins < 0 {
		return LedgerOutput{}, errors.New("The total number of coins is a negative value")
	}

	outputFeePool := int64(ledgerHeader.FeePool)
	if outputFeePool < 0 {
		return LedgerOutput{}, errors.New("The fee pool is a negative value")
	}

	outputBaseFee := int32(ledgerHeader.BaseFee)
	if outputBaseFee < 0 {
		return LedgerOutput{}, errors.New("The base fee is a negative value")
	}

	outputBaseReserve := int32(ledgerHeader.BaseReserve)
	if outputBaseReserve < 0 {
		return LedgerOutput{}, errors.New("The base reserve is a negative value")
	}

	outputMaxTxSetSize := int32(ledgerHeader.MaxTxSetSize)
	if outputMaxTxSetSize < 0 {
		return LedgerOutput{}, errors.New("The maximum transaction set size is a negative value")
	} else if outputMaxTxSetSize < outputTransactionCount {
		return LedgerOutput{}, errors.New("The transaction count is greater than the maximum transaction set size")
	}

	outputProtocolVersion := int32(ledgerHeader.LedgerVersion)
	if outputProtocolVersion < 0 {
		return LedgerOutput{}, errors.New("The protocol version is a negative value")
	}

	transformedLedger := LedgerOutput{
		Sequence:           outputSequence,
		LedgerHash:         outputLedgerHash,
		PreviousLedgerHash: outputPreviousHash,
		LedgerHeader:       outputLedgerHeader,

		TransactionCount:           outputTransactionCount,
		OperationCount:             outputOperationCount,
		SuccessfulTransactionCount: outputSuccessfulCount,
		FailedTransactionCount:     outputFailedCount,
		TxSetOperationCount:        outputTxSetOperationCount,

		ClosedAt: outputCloseTime,

		TotalCoins:      outputTotalCoins,
		FeePool:         outputFeePool,
		BaseFee:         outputBaseFee,
		BaseReserve:     outputBaseReserve,
		MaxTxSetSize:    outputMaxTxSetSize,
		ProtocolVersion: outputProtocolVersion,
	}
	return transformedLedger, nil
}

func extractCounts(lcm xdr.LedgerCloseMetaV0) (int32, int32, int32, int32, string, error) {
	transactions := lcm.TxSet.Txs
	results := lcm.TxProcessing
	txCount := len(transactions)
	if txCount != len(results) {
		return 0, 0, 0, 0, "", errors.New("The number of transactions and results are different")
	}
	var operationCount, successTxCount, failedTxCount, txSetOperationCount int32
	for i := 0; i < txCount; i++ {
		operationResults, ok := results[i].Result.OperationResults()
		if !ok {
			return 0, 0, 0, 0, "", fmt.Errorf("Could not access operation results for result %d", i)
		}
		numberOfOps := int32(len(operationResults))
		txSetOperationCount += numberOfOps

		if results[i].Result.Successful() {
			successTxCount++
			operationCount += numberOfOps
		} else {
			failedTxCount++
		}

	}

	return int32(txCount) - failedTxCount, operationCount, successTxCount, failedTxCount, strconv.FormatInt(int64(txSetOperationCount), 10), nil
}
