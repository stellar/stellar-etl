package transform

import (
	"fmt"

	"github.com/stellar/stellar-etl/internal/toid"
	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// TransformDiagnosticEvent converts a transaction's diagnostic events from the history archive ingestion system into a form suitable for BigQuery
func TransformDiagnosticEvent(transaction ingest.LedgerTransaction, lhe xdr.LedgerHeaderHistoryEntry) ([]DiagnosticEventOutput, error, bool) {
	ledgerHeader := lhe.Header
	outputTransactionHash := utils.HashToHexString(transaction.Result.TransactionHash)
	outputLedgerSequence := uint32(ledgerHeader.LedgerSeq)

	transactionIndex := uint32(transaction.Index)

	outputTransactionID := toid.New(int32(outputLedgerSequence), int32(transactionIndex), 0).ToInt64()

	outputCloseTime, err := utils.TimePointToUTCTimeStamp(ledgerHeader.ScpValue.CloseTime)
	if err != nil {
		return []DiagnosticEventOutput{}, fmt.Errorf("for ledger %d; transaction %d (transaction id=%d): %v", outputLedgerSequence, transactionIndex, outputTransactionID, err), false
	}

	diagnosticEvents, err := transaction.GetDiagnosticEvents()
	if err != nil {
		return []DiagnosticEventOutput{}, nil, false
	}

	var transformedDiagnosticEvents []DiagnosticEventOutput

	for _, diagnoticEvent := range diagnosticEvents {
		var outputContractId string

		outputInSuccessfulContractCall := diagnoticEvent.InSuccessfulContractCall
		event := diagnoticEvent.Event
		outputExtV := event.Ext.V
		outputType := event.Type.String()
		outputBodyV := event.Body.V
		body, _ := event.Body.GetV0()

		outputBody, _ := xdr.MarshalBase64(body)

		if event.ContractId != nil {
			contractId := *event.ContractId
			contractIdByte, _ := contractId.MarshalBinary()
			outputContractId, _ = strkey.Encode(strkey.VersionByteContract, contractIdByte)
		}

		transformedDiagnosticEvent := DiagnosticEventOutput{
			TransactionHash:          outputTransactionHash,
			LedgerSequence:           outputLedgerSequence,
			TransactionID:            outputTransactionID,
			ClosedAt:                 outputCloseTime,
			InSuccessfulContractCall: outputInSuccessfulContractCall,
			ExtV:                     outputExtV,
			ContractId:               outputContractId,
			Type:                     outputType,
			BodyV:                    outputBodyV,
			Body:                     outputBody,
		}

		transformedDiagnosticEvents = append(transformedDiagnosticEvents, transformedDiagnosticEvent)
	}

	return transformedDiagnosticEvents, nil, true
}
