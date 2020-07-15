package transform

import (
<<<<<<< HEAD
	"errors"
=======
>>>>>>> master
	"fmt"
	"strconv"

	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

<<<<<<< HEAD
//ConvertLedger converts a ledger from the history archive ingestion system into a form suitable for BigQuery
func ConvertLedger(inputLedgerMeta xdr.LedgerCloseMeta) (LedgerOutput, error) {
	ledger, ok := inputLedgerMeta.GetV0()
	if !ok {
		return LedgerOutput{}, errors.New("Could not access the version 0 information for the provided ledger")
=======
//TransformLedger converts a ledger from the history archive ingestion system into a form suitable for BigQuery
func TransformLedger(inputLedgerMeta xdr.LedgerCloseMeta) (LedgerOutput, error) {
	ledger, ok := inputLedgerMeta.GetV0()
	if !ok {
		return LedgerOutput{}, fmt.Errorf("Could not access the v0 information for given ledger")
>>>>>>> master
	}
	ledgerHeaderHistory := ledger.LedgerHeader
	ledgerHeader := ledgerHeaderHistory.Header

	outputSequence := int32(ledgerHeader.LedgerSeq)
	if outputSequence < 0 {
<<<<<<< HEAD
		return LedgerOutput{}, errors.New("The sequence is a negative value")
=======
		return LedgerOutput{}, fmt.Errorf("Ledger sequence %d is negative", outputSequence)
>>>>>>> master
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
<<<<<<< HEAD
		return LedgerOutput{}, errors.New("The total number of coins is a negative value")
=======
		return LedgerOutput{}, fmt.Errorf("The total number of coins (%d) is negative for ledger %d", outputTotalCoins, outputSequence)
>>>>>>> master
	}

	outputFeePool := int64(ledgerHeader.FeePool)
	if outputFeePool < 0 {
<<<<<<< HEAD
		return LedgerOutput{}, errors.New("The fee pool is a negative value")
=======
		return LedgerOutput{}, fmt.Errorf("The fee pool (%d) is negative for ledger %d", outputFeePool, outputSequence)
>>>>>>> master
	}

	outputBaseFee := int32(ledgerHeader.BaseFee)
	if outputBaseFee < 0 {
<<<<<<< HEAD
		return LedgerOutput{}, errors.New("The base fee is a negative value")
=======
		return LedgerOutput{}, fmt.Errorf("The base fee (%d) is negative for ledger %d", outputBaseFee, outputSequence)
>>>>>>> master
	}

	outputBaseReserve := int32(ledgerHeader.BaseReserve)
	if outputBaseReserve < 0 {
<<<<<<< HEAD
		return LedgerOutput{}, errors.New("The base reserve is a negative value")
=======
		return LedgerOutput{}, fmt.Errorf("The base reserve (%d) is negative for ledger %d", outputBaseReserve, outputSequence)
>>>>>>> master
	}

	outputMaxTxSetSize := int32(ledgerHeader.MaxTxSetSize)
	if outputMaxTxSetSize < 0 {
<<<<<<< HEAD
		return LedgerOutput{}, errors.New("The maximum transaction set size is a negative value")
	} else if outputMaxTxSetSize < outputTransactionCount {
		return LedgerOutput{}, errors.New("The transaction count is greater than the maximum transaction set size")
=======
		return LedgerOutput{}, fmt.Errorf("The max transaction set size (%d) is negative for ledger %d", outputMaxTxSetSize, outputSequence)
	} else if outputMaxTxSetSize < outputTransactionCount {
		return LedgerOutput{}, fmt.Errorf("The transaction count is greater than the maximum transaction set size (%d > %d)", outputTransactionCount, outputMaxTxSetSize)
>>>>>>> master
	}

	outputProtocolVersion := int32(ledgerHeader.LedgerVersion)
	if outputProtocolVersion < 0 {
<<<<<<< HEAD
		return LedgerOutput{}, errors.New("The protocol version is a negative value")
=======
		return LedgerOutput{}, fmt.Errorf("The protocol version (%d) is negative for ledger %d", outputProtocolVersion, outputSequence)
>>>>>>> master
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

<<<<<<< HEAD
func extractCounts(lcm xdr.LedgerCloseMetaV0) (int32, int32, int32, int32, string, error) {
=======
func extractCounts(lcm xdr.LedgerCloseMetaV0) (transactionCount int32, operationCount int32, successTxCount int32, failedTxCount int32, txSetOperationCount string, err error) {
>>>>>>> master
	transactions := lcm.TxSet.Txs
	results := lcm.TxProcessing
	txCount := len(transactions)
	if txCount != len(results) {
<<<<<<< HEAD
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
=======
		err = fmt.Errorf("The number of transactions and results are different (%d != %d)", txCount, len(results))
		return
	}
	txSetOperationCounter := int32(0)
	for i := 0; i < txCount; i++ {
		operationResults, ok := results[i].Result.OperationResults()
		if !ok {
			err = fmt.Errorf("Could not access operation results for result %d", i)
			return
		}
		numberOfOps := int32(len(operationResults))
		txSetOperationCounter += numberOfOps
>>>>>>> master

		if results[i].Result.Successful() {
			successTxCount++
			operationCount += numberOfOps
		} else {
			failedTxCount++
		}

	}
<<<<<<< HEAD

	return int32(txCount) - failedTxCount, operationCount, successTxCount, failedTxCount, strconv.FormatInt(int64(txSetOperationCount), 10), nil
=======
	transactionCount = int32(txCount) - failedTxCount
	txSetOperationCount = strconv.FormatInt(int64(txSetOperationCounter), 10)
	return
>>>>>>> master
}
