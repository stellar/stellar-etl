package transform

import (
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/go/xdr"
)

// TransformTransaction converts a transaction from the history archive ingestion system into a form suitable for BigQuery
func TransformTransaction(transaction ingest.LedgerTransaction, lhe xdr.LedgerHeaderHistoryEntry) (TransactionOutput, error) {

	outputTransactionHash, _ := xdr.MarshalBase64(transaction.Hash)
	outputAccount, _ := transaction.Account()
	outputFeeCharged, _ := transaction.FeeCharged()
	outputMemoType := transaction.MemoType()
	outputTimeBounds, _ := transaction.TimeBounds()
	outputLedgerBound, _ := transaction.LedgerBounds()
	outputMinSequence, _ := transaction.MinSequence()
	outputMinSequenceAge, _ := transaction.MinSequenceAge()
	outputMinSequenceLedgerGap, _ := transaction.MinSequenceLedgerGap()
	outputResourceFee, _ := transaction.SorobanResourceFee()
	outputSorobanResourcesInstructions, _ := transaction.SorobanResourcesInstructions()
	outputSorobanResourcesReadBytes, _ := transaction.SorobanResourcesReadBytes()
	outputSorobanResourcesWriteBytes, _ := transaction.SorobanResourcesWriteBytes()
	outputInclusionFeeBid, _ := transaction.InclusionFeeBid()
	outputInclusionFeeCharged, _ := transaction.InclusionFeeCharged()
	outputResourceFeeRefund, _ := transaction.SorobanResourceFeeRefund()
	outputTotalNonRefundableResourceFeeCharged, _ := transaction.SorobanTotalNonRefundableResourceFeeCharged()
	outputTotalRefundableResourceFeeCharged, _ := transaction.SorobanTotalRefundableResourceFeeCharged()
	outputRentFeeCharged, _ := transaction.SorobanRentFeeCharged()
	outputTxSigners, _ := transaction.Signers()
	outputAccountMuxed, _ := transaction.AccountMuxed()
	outputFeeAccountMuxed, _ := transaction.FeeAccountMuxed()
	outputFeeAccount, _ := transaction.FeeAccount()
	outputInnerTransactionHash, _ := transaction.InnerTransactionHash()
	outputNewMaxFee, _ := transaction.NewMaxFee()

	outputSuccessful := transaction.Result.Successful()
	transformedTransaction := TransactionOutput{
		TransactionHash:                      outputTransactionHash,
		LedgerSequence:                       ledger.Sequence(transaction.Ledger),
		TransactionID:                        outputTransactionID,
		Account:                              outputAccount,
		AccountSequence:                      transaction.AccountSequence(),
		MaxFee:                               transaction.MaxFee(),
		FeeCharged:                           outputFeeCharged,
		OperationCount:                       transaction.OperationCount(),
		MemoType:                             outputMemoType,
		Memo:                                 transaction.Memo(),
		TimeBounds:                           outputTimeBounds,
		Successful:                           outputSuccessful,
		LedgerBounds:                         outputLedgerBound,
		MinAccountSequence:                   outputMinSequence,
		MinAccountSequenceAge:                outputMinSequenceAge,
		MinAccountSequenceLedgerGap:          outputMinSequenceLedgerGap,
		ExtraSigners:                         formatSigners(transaction.Envelope.ExtraSigners()),
		ClosedAt:                             ledger.ClosedAt(transaction.Ledger),
		ResourceFee:                          outputResourceFee,
		SorobanResourcesInstructions:         outputSorobanResourcesInstructions,
		SorobanResourcesReadBytes:            outputSorobanResourcesReadBytes,
		SorobanResourcesWriteBytes:           outputSorobanResourcesWriteBytes,
		InclusionFeeBid:                      outputInclusionFeeBid,
		InclusionFeeCharged:                  outputInclusionFeeCharged,
		ResourceFeeRefund:                    outputResourceFeeRefund,
		TotalNonRefundableResourceFeeCharged: outputTotalNonRefundableResourceFeeCharged,
		TotalRefundableResourceFeeCharged:    outputTotalRefundableResourceFeeCharged,
		RentFeeCharged:                       outputRentFeeCharged,
		TxSigners:                            outputTxSigners,
		AccountMuxed:                         outputAccountMuxed,
		FeeAccountMuxed:                      outputFeeAccountMuxed,
		FeeAccount:                           outputFeeAccount,
		InnerTransactionHash:                 outputInnerTransactionHash,
		NewMaxFee:                            outputNewMaxFee,
	}

	return transformedTransaction, nil
}
