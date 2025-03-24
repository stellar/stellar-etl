package transform

import (
	"github.com/stellar/go/ingest"
	diagnosticevent "github.com/stellar/go/ingest/diagnostic_event"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/go/xdr"
)

// TransformContractEvent converts a transaction's contract events and diagnostic events into a form suitable for BigQuery.
// It is known that contract events are a subset of the diagnostic events XDR definition. We are opting to call all of these events
// contract events for better clarity to data analytics users.
func TransformContractEvent(event xdr.DiagnosticEvent, transaction ingest.LedgerTransaction) (ContractEventOutput, error) {
	outputTransactionHash, _ := xdr.MarshalBase64(transaction.Hash)
	outputContractID, _, _ := diagnosticevent.ContractID(event)
	outputTopics, _ := diagnosticevent.Topics(event)
	outputData, _ := diagnosticevent.Topics(event)

	transformedDiagnosticEvent := ContractEventOutput{
		TransactionHash:          outputTransactionHash,
		TransactionID:            transaction.ID(),
		Successful:               transaction.Successful(),
		LedgerSequence:           ledger.Sequence(transaction.Ledger),
		ClosedAt:                 ledger.ClosedAt(transaction.Ledger),
		InSuccessfulContractCall: diagnosticevent.Successful(event),
		ContractId:               outputContractID,
		Type:                     diagnosticevent.Type(event),
		TopicsDecoded:            outputTopics,
		DataDecoded:              outputData,
	}

	return transformedDiagnosticEvent, nil
}
