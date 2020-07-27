package transform

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

//TransformTransaction converts a transaction from the history archive ingestion system into a form suitable for BigQuery
func TransformTransaction(transaction io.LedgerTransaction, lhe xdr.LedgerHeaderHistoryEntry) (TransactionOutput, error) {
	ledgerHeader := lhe.Header
	outputTransactionHash := utils.HashToHexString(transaction.Result.TransactionHash)
	outputLedgerSequence := uint32(ledgerHeader.LedgerSeq)

	outputApplicationOrder := uint32(transaction.Index)

	outputAccount, err := utils.GetAccountAddressFromMuxedAccount(transaction.Envelope.SourceAccount())
	if err != nil {
		return TransactionOutput{}, err
	}

	outputAccountSequence := transaction.Envelope.SeqNum()
	if outputAccountSequence < 0 {
		return TransactionOutput{}, fmt.Errorf("The account's sequence number (%d) is negative for ledger %d; transaction %d", outputAccountSequence, outputLedgerSequence, outputApplicationOrder)
	}

	outputMaxFee := transaction.Envelope.Fee()
	if outputMaxFee < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee (%d) is negative for ledger %d; transaction %d", outputMaxFee, outputLedgerSequence, outputApplicationOrder)
	}

	outputFeeCharged := int64(transaction.Result.Result.FeeCharged)
	if outputFeeCharged < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee charged (%d) is negative for ledger %d; transaction %d", outputFeeCharged, outputLedgerSequence, outputApplicationOrder)
	}

	outputOperationCount := int32(len(transaction.Envelope.Operations()))
	outputCreatedAt, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return TransactionOutput{}, err
	}

	memoObject := transaction.Envelope.Memo()
	outputMemoContents := ""
	switch xdr.MemoType(memoObject.Type) {
	case xdr.MemoTypeMemoText:
		outputMemoContents = memoObject.MustText()
	case xdr.MemoTypeMemoId:
		outputMemoContents = strconv.FormatUint(uint64(memoObject.MustId()), 10)
	case xdr.MemoTypeMemoHash:
		hash := memoObject.MustHash()
		outputMemoContents = base64.StdEncoding.EncodeToString(hash[:])
	case xdr.MemoTypeMemoReturn:
		hash := memoObject.MustRetHash()
		outputMemoContents = base64.StdEncoding.EncodeToString(hash[:])
	}

	outputMemoType := memoObject.Type.String()
	timeBound := transaction.Envelope.TimeBounds()
	outputTimeBounds := ""
	if timeBound != nil {
		if timeBound.MaxTime < timeBound.MinTime {
			return TransactionOutput{}, fmt.Errorf("The max time is earlier than the min time (%d < %d) for ledger %d; transaction %d",
				timeBound.MaxTime, timeBound.MinTime, outputLedgerSequence, outputApplicationOrder)
		}

		outputTimeBounds = fmt.Sprintf("[%d, %d)", timeBound.MinTime, timeBound.MaxTime)
	}

	outputSuccessful := transaction.Result.Successful()
	transformedTransaction := TransactionOutput{
		TransactionHash:  outputTransactionHash,
		LedgerSequence:   outputLedgerSequence,
		ApplicationOrder: outputApplicationOrder,
		Account:          outputAccount,
		AccountSequence:  outputAccountSequence,
		MaxFee:           outputMaxFee,
		FeeCharged:       outputFeeCharged,
		OperationCount:   outputOperationCount,
		CreatedAt:        outputCreatedAt,
		MemoType:         outputMemoType,
		Memo:             outputMemoContents,
		TimeBounds:       outputTimeBounds,
		Successful:       outputSuccessful,
	}
	return transformedTransaction, nil
}
