package transform

import (
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransformTransaction converts a transaction from the history archive ingestion system into a form suitable for BigQuery
func TransformTransaction(transaction ingest.LedgerTransaction, lhe xdr.LedgerHeaderHistoryEntry) (TransactionOutput, error) {
	ledgerHeader := lhe.Header
	outputTransactionHash := utils.HashToHexString(transaction.Result.TransactionHash)
	outputLedgerSequence := uint32(ledgerHeader.LedgerSeq)

	outputApplicationOrder := uint32(transaction.Index)

	outputTransactionID := toid.New(int32(outputLedgerSequence), int32(outputApplicationOrder), 0).ToInt64()

	outputAccount, err := utils.GetAccountAddressFromMuxedAccount(transaction.Envelope.SourceAccount())
	if err != nil {
		return TransactionOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, outputApplicationOrder, outputTransactionID, err)
	}

	outputAccountSequence := transaction.Envelope.SeqNum()
	if outputAccountSequence < 0 {
		return TransactionOutput{}, fmt.Errorf("The account's sequence number (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputAccountSequence, outputLedgerSequence, outputApplicationOrder, outputTransactionID)
	}

	outputMaxFee := transaction.Envelope.Fee()
	if outputMaxFee < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputMaxFee, outputLedgerSequence, outputApplicationOrder, outputTransactionID)
	}

	outputFeeCharged := int64(transaction.Result.Result.FeeCharged)
	if outputFeeCharged < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee charged (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputFeeCharged, outputLedgerSequence, outputApplicationOrder, outputTransactionID)
	}

	outputOperationCount := int32(len(transaction.Envelope.Operations()))
	outputCreatedAt, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return TransactionOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, outputApplicationOrder, outputTransactionID, err)
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
			return TransactionOutput{}, fmt.Errorf("The max time is earlier than the min time (%d < %d) for ledger %d; transaction %d (transaction id=%d)",
				timeBound.MaxTime, timeBound.MinTime, outputLedgerSequence, outputApplicationOrder, outputTransactionID)
		}

		outputTimeBounds = fmt.Sprintf("[%d, %d)", timeBound.MinTime, timeBound.MaxTime)
	}

	outputSuccessful := transaction.Result.Successful()
	transformedTransaction := TransactionOutput{
		TransactionHash:  outputTransactionHash,
		LedgerSequence:   outputLedgerSequence,
		ApplicationOrder: outputApplicationOrder,
		TransactionID:    outputTransactionID,
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
