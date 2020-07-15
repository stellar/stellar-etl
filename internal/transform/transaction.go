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
	if outputLedgerSequence < 0 {
		return TransactionOutput{}, fmt.Errorf("Ledger sequence %d is negative", outputLedgerSequence)
	}
	outputApplicationOrder := int32(transaction.Index)
	if outputApplicationOrder < 0 {
		return TransactionOutput{}, fmt.Errorf("The application order (%d) is negative for ledger %d", outputApplicationOrder, outputLedgerSequence)
	}
	outputAccount, err := utils.GetAccountAddressFromMuxedAccount(transaction.Envelope.SourceAccount())
	if err != nil {
		return TransactionOutput{}, err
	}
	outputAccountSequence := int32(transaction.Envelope.SeqNum())
	if outputAccountSequence < 0 {
		return TransactionOutput{}, fmt.Errorf("The account sequence number (%d) is negative for ledger %d; transaction %d", outputAccountSequence, outputLedgerSequence, outputApplicationOrder)
	}
	outputMaxFee := int64(transaction.Envelope.Fee())
	if outputMaxFee < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee (%d) is negative for ledger %d; transaction %d", outputMaxFee, outputLedgerSequence, outputApplicationOrder)
	}
	outputFeeCharged := int64(transaction.Result.Result.FeeCharged)
	if outputFeeCharged < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee charged (%d) is negative for ledger %d; transaction %d", outputFeeCharged, outputLedgerSequence, outputApplicationOrder)
	}
	outputOperationCount := int32(len(transaction.Envelope.Operations()))
	if outputOperationCount < 0 {
		return TransactionOutput{}, fmt.Errorf("The operation count (%d) is negative for ledger %d; transaction %d", outputOperationCount, outputLedgerSequence, outputApplicationOrder)
	}
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
