package transform

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/guregu/null"
	"github.com/lib/pq"
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

	transactionIndex := uint32(transaction.Index)

	outputTransactionID := toid.New(int32(outputLedgerSequence), int32(transactionIndex), 0).ToInt64()

	sourceAccount := transaction.Envelope.SourceAccount()
	outputAccount, err := utils.GetAccountAddressFromMuxedAccount(transaction.Envelope.SourceAccount())
	if err != nil {
		return TransactionOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, transactionIndex, outputTransactionID, err)
	}

	outputAccountSequence := transaction.Envelope.SeqNum()
	if outputAccountSequence < 0 {
		return TransactionOutput{}, fmt.Errorf("The account's sequence number (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputAccountSequence, outputLedgerSequence, transactionIndex, outputTransactionID)
	}

	outputMaxFee := transaction.Envelope.Fee()
	if outputMaxFee < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputMaxFee, outputLedgerSequence, transactionIndex, outputTransactionID)
	}

	outputFeeCharged := int64(transaction.Result.Result.FeeCharged)
	if outputFeeCharged < 0 {
		return TransactionOutput{}, fmt.Errorf("The fee charged (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputFeeCharged, outputLedgerSequence, transactionIndex, outputTransactionID)
	}

	outputOperationCount := int32(len(transaction.Envelope.Operations()))

	outputTxEnvelope, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return TransactionOutput{}, err
	}

	outputTxResult, err := xdr.MarshalBase64(&transaction.Result.Result)
	if err != nil {
		return TransactionOutput{}, err
	}

	outputTxMeta, err := xdr.MarshalBase64(transaction.UnsafeMeta)
	if err != nil {
		return TransactionOutput{}, err
	}

	outputTxFeeMeta, err := xdr.MarshalBase64(transaction.FeeChanges)
	if err != nil {
		return TransactionOutput{}, err
	}

	outputCreatedAt, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return TransactionOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, transactionIndex, outputTransactionID, err)
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
		if timeBound.MaxTime < timeBound.MinTime && timeBound.MaxTime != 0 {

			return TransactionOutput{}, fmt.Errorf("The max time is earlier than the min time (%d < %d) for ledger %d; transaction %d (transaction id=%d)",
				timeBound.MaxTime, timeBound.MinTime, outputLedgerSequence, transactionIndex, outputTransactionID)
		}

		if timeBound.MaxTime == 0 {
			outputTimeBounds = fmt.Sprintf("[%d,)", timeBound.MinTime)
		} else {
			outputTimeBounds = fmt.Sprintf("[%d,%d)", timeBound.MinTime, timeBound.MaxTime)
		}

	}

	ledgerBound := transaction.Envelope.LedgerBounds()
	outputLedgerBound := ""
	if ledgerBound != nil {
		outputLedgerBound = fmt.Sprintf("[%d,%d)", int64(ledgerBound.MinLedger), int64(ledgerBound.MaxLedger))
	}

	minSequenceNumber := transaction.Envelope.MinSeqNum()
	outputMinSequence := null.Int{}
	if minSequenceNumber != nil {
		outputMinSequence = null.IntFrom(int64(*minSequenceNumber))
	}

	minSequenceAge := transaction.Envelope.MinSeqAge()
	outputMinSequenceAge := null.Int{}
	if minSequenceAge != nil {
		outputMinSequenceAge = null.IntFrom(int64(*minSequenceAge))
	}

	minSequenceLedgerGap := transaction.Envelope.MinSeqLedgerGap()
	outputMinSequenceLedgerGap := null.Int{}
	if minSequenceLedgerGap != nil {
		outputMinSequenceLedgerGap = null.IntFrom(int64(*minSequenceLedgerGap))
	}

	// Soroban fees and resources
	// Note: MaxFee and FeeCharged is the sum of base transaction fees + Soroban fees
	// Breakdown of Soroban fees can be calculated by the config_setting resource pricing * the resources used

	var sorobanData xdr.SorobanTransactionData
	var hasSorobanData bool
	var outputResourceFee int64
	var outputSorobanResourcesInstructions uint32
	var outputSorobanResourcesReadBytes uint32
	var outputSorobanResourcesWriteBytes uint32
	var outputInclusionFeeBid int64
	var outputInclusionFeeCharged int64
	var outputResourceFeeRefund int64

	// Soroban data can exist in V1 and FeeBump transactionEnvelopes
	switch transaction.Envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		sorobanData, hasSorobanData = transaction.Envelope.V1.Tx.Ext.GetSorobanData()
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		sorobanData, hasSorobanData = transaction.Envelope.FeeBump.Tx.InnerTx.V1.Tx.Ext.GetSorobanData()
	}

	if hasSorobanData {
		outputResourceFee = int64(sorobanData.ResourceFee)
		outputSorobanResourcesInstructions = uint32(sorobanData.Resources.Instructions)
		outputSorobanResourcesReadBytes = uint32(sorobanData.Resources.ReadBytes)
		outputSorobanResourcesWriteBytes = uint32(sorobanData.Resources.WriteBytes)
		outputInclusionFeeBid = int64(transaction.Envelope.Fee()) - outputResourceFee

		accountBalanceStart, accountBalanceEnd := getAccountBalanceFromLedgerEntryChanges(transaction.FeeChanges, sourceAccount.Address())
		initialFeeCharged := accountBalanceStart - accountBalanceEnd
		outputInclusionFeeCharged = initialFeeCharged - outputResourceFee

		meta, ok := transaction.UnsafeMeta.GetV3()
		if ok {
			accountBalanceStart, accountBalanceEnd := getAccountBalanceFromLedgerEntryChanges(meta.TxChangesAfter, sourceAccount.Address())
			outputResourceFeeRefund = accountBalanceEnd - accountBalanceStart
		}

		// TODO: FeeCharged is calculated incorrectly in protocol 20. Remove when protocol is updated and the bug is fixed
		outputFeeCharged = outputResourceFee - outputResourceFeeRefund + outputInclusionFeeCharged
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return TransactionOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, transactionIndex, outputTransactionID, err)
	}

	outputTxResultCode := transaction.Result.Result.Result.Code.String()

	outputSuccessful := transaction.Result.Successful()
	transformedTransaction := TransactionOutput{
		TransactionHash:              outputTransactionHash,
		LedgerSequence:               outputLedgerSequence,
		TransactionID:                outputTransactionID,
		Account:                      outputAccount,
		AccountSequence:              outputAccountSequence,
		MaxFee:                       outputMaxFee,
		FeeCharged:                   outputFeeCharged,
		OperationCount:               outputOperationCount,
		TxEnvelope:                   outputTxEnvelope,
		TxResult:                     outputTxResult,
		TxMeta:                       outputTxMeta,
		TxFeeMeta:                    outputTxFeeMeta,
		CreatedAt:                    outputCreatedAt,
		MemoType:                     outputMemoType,
		Memo:                         outputMemoContents,
		TimeBounds:                   outputTimeBounds,
		Successful:                   outputSuccessful,
		LedgerBounds:                 outputLedgerBound,
		MinAccountSequence:           outputMinSequence,
		MinAccountSequenceAge:        outputMinSequenceAge,
		MinAccountSequenceLedgerGap:  outputMinSequenceLedgerGap,
		ExtraSigners:                 formatSigners(transaction.Envelope.ExtraSigners()),
		ClosedAt:                     outputCloseTime,
		ResourceFee:                  outputResourceFee,
		SorobanResourcesInstructions: outputSorobanResourcesInstructions,
		SorobanResourcesReadBytes:    outputSorobanResourcesReadBytes,
		SorobanResourcesWriteBytes:   outputSorobanResourcesWriteBytes,
		TransactionResultCode:        outputTxResultCode,
		InclusionFeeBid:              outputInclusionFeeBid,
		InclusionFeeCharged:          outputInclusionFeeCharged,
		ResourceFeeRefund:            outputResourceFeeRefund,
	}

	// Add Muxed Account Details, if exists
	if sourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
		muxedAddress, err := sourceAccount.GetAddress()
		if err != nil {
			return TransactionOutput{}, err
		}
		transformedTransaction.AccountMuxed = muxedAddress

	}

	// Add Fee Bump Details, if exists
	if transaction.Envelope.IsFeeBump() {
		feeBumpAccount := transaction.Envelope.FeeBumpAccount()
		feeAccount := feeBumpAccount.ToAccountId()
		if sourceAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
			feeAccountMuxed := feeAccount.Address()
			transformedTransaction.FeeAccountMuxed = feeAccountMuxed
		}
		transformedTransaction.FeeAccount = feeAccount.Address()
		innerHash := transaction.Result.InnerHash()
		transformedTransaction.InnerTransactionHash = hex.EncodeToString(innerHash[:])
		transformedTransaction.NewMaxFee = uint32(transaction.Envelope.FeeBumpFee())
	}

	return transformedTransaction, nil
}

func getAccountBalanceFromLedgerEntryChanges(changes xdr.LedgerEntryChanges, sourceAccountAddress string) (int64, int64) {
	var accountBalanceStart int64
	var accountBalanceEnd int64

	for _, change := range changes {
		switch change.Type {
		case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
			accountEntry, ok := change.Updated.Data.GetAccount()
			if !ok {
				continue
			}

			if accountEntry.AccountId.Address() == sourceAccountAddress {
				accountBalanceEnd = int64(accountEntry.Balance)
			}
		case xdr.LedgerEntryChangeTypeLedgerEntryState:
			accountEntry, ok := change.State.Data.GetAccount()
			if !ok {
				continue
			}

			if accountEntry.AccountId.Address() == sourceAccountAddress {
				accountBalanceStart = int64(accountEntry.Balance)
			}
		}
	}

	return accountBalanceStart, accountBalanceEnd
}

func formatSigners(s []xdr.SignerKey) pq.StringArray {
	if s == nil {
		return nil
	}

	signers := make([]string, len(s))
	for i, key := range s {
		signers[i] = key.Address()
	}

	return signers
}
