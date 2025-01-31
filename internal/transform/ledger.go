package transform

import (
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/go/xdr"
)

// TransformLedger converts a ledger from the history archive ingestion system into a form suitable for BigQuery
func TransformLedger(inputLedger historyarchive.Ledger, lcm xdr.LedgerCloseMeta) (LedgerOutput, error) {
	outputSequence := ledger.Sequence(lcm)
	outputLedgerHash := ledger.Hash(lcm)
	outputPreviousHash := ledger.PreviousHash(lcm)
	successTxCount, totalTxCount := ledger.TransactionCounts(lcm)
	successOpCount, totalOpCount := ledger.OperationCounts(lcm)
	outputClosedAt := ledger.ClosedAt(lcm)
	outputTotalCoins := ledger.TotalCoins(lcm)
	outputFeePool := ledger.FeePool(lcm)
	outputBaseFee := ledger.BaseFee(lcm)
	outputBaseReserve := ledger.BaseReserve(lcm)
	outputMaxTxSetSize := ledger.MaxTxSetSize(lcm)
	outputProtocolVersion := ledger.LedgerVersion(lcm)
	outputSorobanFeeWrite1Kb, _ := ledger.SorobanFeeWrite1Kb(lcm)
	outputTotalByteSizeOfBucketList, _ := ledger.TotalByteSizeOfBucketList(lcm)
	outputNodeID, _ := ledger.NodeID(lcm)
	outputSignature, _ := ledger.Signature(lcm)

	transformedLedger := LedgerOutput{
		Sequence:                   outputSequence,
		LedgerHash:                 outputLedgerHash,
		PreviousLedgerHash:         outputPreviousHash,
		TransactionCount:           int32(totalTxCount),
		OperationCount:             int32(successOpCount),
		SuccessfulTransactionCount: int32(successTxCount),
		TxSetOperationCount:        string(totalOpCount),
		ClosedAt:                   outputClosedAt,
		TotalCoins:                 outputTotalCoins,
		FeePool:                    outputFeePool,
		BaseFee:                    outputBaseFee,
		BaseReserve:                outputBaseReserve,
		MaxTxSetSize:               outputMaxTxSetSize,
		ProtocolVersion:            outputProtocolVersion,
		SorobanFeeWrite1Kb:         outputSorobanFeeWrite1Kb,
		NodeID:                     outputNodeID,
		Signature:                  outputSignature,
		TotalByteSizeOfBucketList:  outputTotalByteSizeOfBucketList,
	}
	return transformedLedger, nil
}
