package transform

import (
	"fmt"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransformTransaction converts a transaction from the history archive ingestion system into a form suitable for BigQuery
func TransformTx(transaction ingest.LedgerTransaction, lhe xdr.LedgerHeaderHistoryEntry) (TxOutput, error) {
	ledgerHeader := lhe.Header
	outputLedgerSequence := uint32(ledgerHeader.LedgerSeq)

	outputTxEnvelope, err := xdr.MarshalBase64(transaction.Envelope)
	if err != nil {
		return TxOutput{}, err
	}

	outputTxResult, err := xdr.MarshalBase64(&transaction.Result)
	if err != nil {
		return TxOutput{}, err
	}

	outputTxMeta, err := xdr.MarshalBase64(transaction.UnsafeMeta)
	if err != nil {
		return TxOutput{}, err
	}

	outputTxFeeMeta, err := xdr.MarshalBase64(transaction.FeeChanges)
	if err != nil {
		return TxOutput{}, err
	}

	outputTxLedgerHistory, err := xdr.MarshalBase64(lhe)
	if err != nil {
		return TxOutput{}, err
	}

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return TxOutput{}, fmt.Errorf("could not convert close time to UTC timestamp: %v", err)
	}

	transformedTx := TxOutput{
		LedgerSequence:  outputLedgerSequence,
		TxEnvelope:      outputTxEnvelope,
		TxResult:        outputTxResult,
		TxMeta:          outputTxMeta,
		TxFeeMeta:       outputTxFeeMeta,
		TxLedgerHistory: outputTxLedgerHistory,
		ClosedAt:        outputCloseTime,
	}

	return transformedTx, nil
}
