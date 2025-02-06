package transform

import (
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformTtl converts an ttl ledger change entry into a form suitable for BigQuery
func TransformTtl(ledgerChange ingest.Change) (TtlOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return TtlOutput{}, err
	}

	ttl, ok := ledgerEntry.Data.GetTtl()
	if !ok {
		return TtlOutput{}, fmt.Errorf("could not extract ttl from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	// LedgerEntryChange must contain a ttl change to be parsed, otherwise skip
	if ledgerEntry.Data.Type != xdr.LedgerEntryTypeTtl {
		return TtlOutput{}, nil
	}

	keyHash := ttl.KeyHash.HexString()
	liveUntilLedgerSeq := ttl.LiveUntilLedgerSeq

	changeDetails := utils.GetChangesDetails(ledgerChange)

	transformedPool := TtlOutput{
		KeyHash:            keyHash,
		LiveUntilLedgerSeq: uint32(liveUntilLedgerSeq),
		LastModifiedLedger: uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:  uint32(changeType),
		Deleted:            outputDeleted,
		ClosedAt:           changeDetails.ClosedAt,
		LedgerSequence:     changeDetails.LedgerSequence,
		TransactionID:      changeDetails.TransactionID,
		OperationID:        changeDetails.OperationID,
		OperationType:      changeDetails.OperationType,
	}

	return transformedPool, nil
}
