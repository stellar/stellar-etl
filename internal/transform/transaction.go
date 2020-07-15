package transform

import (
	"fmt"

	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

//TransformTransaction converts a transaction from the history archive ingestion system into a form suitable for BigQuery
func TransformTransaction(transaction io.LedgerTransaction, lhe xdr.LedgerHeaderHistoryEntry) (TransactionOutput, error) {
	ledgerHeader := lhe.Header
	outputTransactionHash := utils.HashToHexString(transaction.Result.TransactionHash)
	outputLedgerSequence := int32(ledgerHeader.LedgerSeq)
	outputApplicationOrder := int32(transaction.Index)
	outputAccount, err := utils.GetAccountAddressFromMuxedAccount(transaction.Envelope.SourceAccount())
	if err != nil {
		return TransactionOutput{}, err
	}
	outputAccountSequence := int32(transaction.Envelope.SeqNum())
	outputMaxFee := int64(transaction.Envelope.Fee())
	outputFeeCharged := int64(transaction.Result.Result.FeeCharged)
	outputOperationCount := int32(len(transaction.Envelope.Operations()))
	outputCreatedAt, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return TransactionOutput{}, err
	}
	outputMemoType := transaction.Envelope.Memo().Type.String()
	outputMemo, _ := transaction.Envelope.Memo().GetText()
	timeBound := transaction.Envelope.TimeBounds()
	outputTimeBounds := ""
	if timeBound != nil {
		outputTimeBounds = fmt.Sprintf("[%d, %d)", timeBound.MinTime, timeBound.MaxTime)
	}
	outputSuccessful := transaction.Result.Successful()
	transformedTransaction := TransactionOutput{
		TransactionHash:  outputTransactionHash,
		LedgerSequence:   outputLedgerSequence,
		ApplicationOrder: outputApplicationOrder,

		Account:         outputAccount,
		AccountSequence: outputAccountSequence,

		MaxFee:     outputMaxFee,
		FeeCharged: outputFeeCharged,

		OperationCount: outputOperationCount,
		CreatedAt:      outputCreatedAt,

		MemoType: outputMemoType,
		Memo:     outputMemo,

		TimeBounds: outputTimeBounds,
		Successful: outputSuccessful,
	}
	return transformedTransaction, nil
}
