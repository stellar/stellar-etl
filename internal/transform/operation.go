package transform

import (
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledger"
	"github.com/stellar/go/xdr"
)

type liquidityPoolDelta struct {
	ReserveA        xdr.Int64
	ReserveB        xdr.Int64
	TotalPoolShares xdr.Int64
}

// TransformOperation converts an operation from the history archive ingestion system into a form suitable for BigQuery
func TransformOperation(operation ingest.LedgerOperation) (OperationOutput, error) {
	outputSourceAccountMuxed, _ := operation.SourceAccountMuxed()
	outputDetails, _ := operation.OperationDetails()

	transformedOperation := OperationOutput{
		SourceAccount:        operation.SourceAccount(),
		SourceAccountMuxed:   outputSourceAccountMuxed,
		Type:                 operation.Type(),
		TypeString:           operation.TypeString(),
		TransactionID:        operation.Transaction.ID(),
		OperationID:          operation.ID(),
		ClosedAt:             ledger.ClosedAt(operation.Transaction.Ledger),
		LedgerSequence:       ledger.Sequence(operation.Transaction.Ledger),
		OperationDetailsJSON: outputDetails,
	}

	return transformedOperation, nil
}
