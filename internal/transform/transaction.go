package transform

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/guregu/null"
	"github.com/lib/pq"
	"github.com/stellar/stellar-etl/v2/internal/toid"
	"github.com/stellar/stellar-etl/v2/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
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
		return TransactionOutput{}, fmt.Errorf("the account's sequence number (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputAccountSequence, outputLedgerSequence, transactionIndex, outputTransactionID)
	}

	outputMaxFee := transaction.Envelope.Fee()

	outputFeeCharged := int64(transaction.Result.Result.FeeCharged)
	if outputFeeCharged < 0 {
		return TransactionOutput{}, fmt.Errorf("the fee charged (%d) is negative for ledger %d; transaction %d (transaction id=%d)", outputFeeCharged, outputLedgerSequence, transactionIndex, outputTransactionID)
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

			return TransactionOutput{}, fmt.Errorf("the max time is earlier than the min time (%d < %d) for ledger %d; transaction %d (transaction id=%d)",
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
	var outputSorobanResourcesDiskReadBytes uint32
	var outputSorobanResourcesWriteBytes uint32
	var outputSorobanArchivedEntries []uint32
	var outputInclusionFeeBid int64
	var outputInclusionFeeCharged int64
	var outputResourceFeeRefund int64
	var outputTotalNonRefundableResourceFeeCharged int64
	var outputTotalRefundableResourceFeeCharged int64
	var outputRentFeeCharged int64
	var feeAccountAddress string

	// Soroban data can exist in V1 and FeeBump transactionEnvelopes
	switch transaction.Envelope.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		sorobanData, hasSorobanData = transaction.Envelope.V1.Tx.Ext.GetSorobanData()
		feeAccountAddress = sourceAccount.Address()
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		sorobanData, hasSorobanData = transaction.Envelope.FeeBump.Tx.InnerTx.V1.Tx.Ext.GetSorobanData()
		feeBumpAccount := transaction.Envelope.FeeBumpAccount()
		feeAccountAddress = feeBumpAccount.Address()
	}

	if hasSorobanData {
		outputResourceFee = int64(sorobanData.ResourceFee)
		outputSorobanResourcesInstructions = uint32(sorobanData.Resources.Instructions)
		outputSorobanResourcesDiskReadBytes = uint32(sorobanData.Resources.DiskReadBytes)
		outputSorobanResourcesWriteBytes = uint32(sorobanData.Resources.WriteBytes)
		if transaction.Envelope.IsFeeBump() {
			outputInclusionFeeBid = int64(transaction.Envelope.FeeBumpFee()) - outputResourceFee
		} else {
			outputInclusionFeeBid = int64(transaction.Envelope.Fee()) - outputResourceFee
		}
		if sorobanData.Ext.ResourceExt != nil {
			for _, entry := range sorobanData.Ext.ResourceExt.ArchivedSorobanEntries {
				outputSorobanArchivedEntries = append(outputSorobanArchivedEntries, uint32(entry))
			}
		}

		accountBalanceStart, accountBalanceEnd := getAccountBalanceFromLedgerEntryChanges(transaction.FeeChanges, feeAccountAddress)
		initialFeeCharged := accountBalanceStart - accountBalanceEnd
		outputInclusionFeeCharged = initialFeeCharged - outputResourceFee

		meta, ok := transaction.UnsafeMeta.GetV3()
		if ok {
			accountBalanceStart, accountBalanceEnd := getAccountBalanceFromLedgerEntryChanges(meta.TxChangesAfter, feeAccountAddress)
			outputResourceFeeRefund = accountBalanceEnd - accountBalanceStart
			if meta.SorobanMeta != nil {
				extV1, ok := meta.SorobanMeta.Ext.GetV1()
				if ok {
					outputTotalNonRefundableResourceFeeCharged = int64(extV1.TotalNonRefundableResourceFeeCharged)
					outputTotalRefundableResourceFeeCharged = int64(extV1.TotalRefundableResourceFeeCharged)
					outputRentFeeCharged = int64(extV1.RentFeeCharged)
				}
			}
		}

		// For P23 onwards, transaction meta v3 will be empty
		metav4, ok := transaction.UnsafeMeta.GetV4()
		if ok {
			accountBalanceStart, accountBalanceEnd := getAccountBalanceFromLedgerEntryChanges(metav4.TxChangesAfter, feeAccountAddress)
			outputResourceFeeRefund = accountBalanceEnd - accountBalanceStart
			if metav4.SorobanMeta != nil {
				extV1, ok := metav4.SorobanMeta.Ext.GetV1()
				if ok {
					outputTotalNonRefundableResourceFeeCharged = int64(extV1.TotalNonRefundableResourceFeeCharged)
					outputTotalRefundableResourceFeeCharged = int64(extV1.TotalRefundableResourceFeeCharged)
					outputRentFeeCharged = int64(extV1.RentFeeCharged)
				}
			}
		}
		// Protocol 20 contained a bug where the feeCharged was incorrectly calculated but was fixed for
		// Protocol 21 with https://github.com/stellar/stellar-core/issues/4188
		// Any Soroban Fee Bump transactions before P21 will need the below logic to calculate the correct feeCharged
		if ledgerHeader.LedgerVersion < 21 && transaction.Envelope.Type == xdr.EnvelopeTypeEnvelopeTypeTxFeeBump {
			outputFeeCharged = outputResourceFee - outputResourceFeeRefund
		}
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return TransactionOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, transactionIndex, outputTransactionID, err)
	}

	outputTxResultCode := transaction.Result.Result.Result.Code.String()

	txSigners, err := getTxSigners(transaction.Envelope.Signatures())
	if err != nil {
		return TransactionOutput{}, err
	}

	outputSuccessful := transaction.Result.Successful()
	transformedTransaction := TransactionOutput{
		TransactionHash:                      outputTransactionHash,
		LedgerSequence:                       outputLedgerSequence,
		TransactionID:                        outputTransactionID,
		Account:                              outputAccount,
		AccountSequence:                      outputAccountSequence,
		MaxFee:                               outputMaxFee,
		FeeCharged:                           outputFeeCharged,
		OperationCount:                       outputOperationCount,
		TxEnvelope:                           outputTxEnvelope,
		TxResult:                             outputTxResult,
		TxMeta:                               outputTxMeta,
		TxFeeMeta:                            outputTxFeeMeta,
		CreatedAt:                            outputCreatedAt,
		MemoType:                             outputMemoType,
		Memo:                                 outputMemoContents,
		TimeBounds:                           outputTimeBounds,
		Successful:                           outputSuccessful,
		LedgerBounds:                         outputLedgerBound,
		MinAccountSequence:                   outputMinSequence,
		MinAccountSequenceAge:                outputMinSequenceAge,
		MinAccountSequenceLedgerGap:          outputMinSequenceLedgerGap,
		ExtraSigners:                         formatSigners(transaction.Envelope.ExtraSigners()),
		ClosedAt:                             outputCloseTime,
		ResourceFee:                          outputResourceFee,
		SorobanResourcesInstructions:         outputSorobanResourcesInstructions,
		SorobanResourcesReadBytes:            outputSorobanResourcesDiskReadBytes,
		SorobanResourcesDiskReadBytes:        outputSorobanResourcesDiskReadBytes,
		SorobanResourcesWriteBytes:           outputSorobanResourcesWriteBytes,
		SorobanResourcesArchivedEntries:      outputSorobanArchivedEntries,
		TransactionResultCode:                outputTxResultCode,
		InclusionFeeBid:                      outputInclusionFeeBid,
		InclusionFeeCharged:                  outputInclusionFeeCharged,
		ResourceFeeRefund:                    outputResourceFeeRefund,
		TotalNonRefundableResourceFeeCharged: outputTotalNonRefundableResourceFeeCharged,
		TotalRefundableResourceFeeCharged:    outputTotalRefundableResourceFeeCharged,
		RentFeeCharged:                       outputRentFeeCharged,
		TxSigners:                            txSigners,
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
		if feeBumpAccount.Type == xdr.CryptoKeyTypeKeyTypeMuxedEd25519 {
			feeAccountMuxed := feeBumpAccount.Address()
			transformedTransaction.FeeAccountMuxed = feeAccountMuxed
		}
		transformedTransaction.FeeAccount = feeAccount.Address()
		innerHash := transaction.Result.InnerHash()
		transformedTransaction.InnerTransactionHash = hex.EncodeToString(innerHash[:])
		transformedTransaction.NewMaxFee = uint32(transaction.Envelope.FeeBumpFee())
		txSigners, err := getTxSigners(transaction.Envelope.FeeBump.Signatures)
		if err != nil {
			return TransactionOutput{}, err
		}

		transformedTransaction.TxSigners = txSigners
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

func getTxSigners(xdrSignatures []xdr.DecoratedSignature) ([]string, error) {
	signers := make([]string, len(xdrSignatures))

	for i, sig := range xdrSignatures {
		signerAccount, err := strkey.Encode(strkey.VersionByteAccountID, sig.Signature)
		if err != nil {
			return nil, err
		}
		signers[i] = signerAccount
	}

	return signers, nil
}
