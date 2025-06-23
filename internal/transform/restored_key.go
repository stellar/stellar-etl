package transform

import (
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformRestoredKey converts an ttl ledger change entry into a form suitable for BigQuery
func TransformRestoredKey(ledgerChange ingest.Change, header xdr.LedgerHeaderHistoryEntry) (RestoredKeyOutput, error) {
	ledgerEntry, changeType, _, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return RestoredKeyOutput{}, err
	}

	if changeType != xdr.LedgerEntryChangeTypeLedgerEntryRestored {
		return RestoredKeyOutput{}, err
	}
	key, err := ledgerEntry.LedgerKey()
	if err != nil {
		return RestoredKeyOutput{}, err
	}

	closedAt, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return RestoredKeyOutput{}, err
	}

	outputLastModifiedLedger := uint32(ledgerEntry.LastModifiedLedgerSeq)
	ledgerSequence := header.Header.LedgerSeq

	transformedKey := RestoredKeyOutput{
		Key:                key,
		LastModifiedLedger: outputLastModifiedLedger,
		LedgerEntryChange:  uint32(changeType),
		ClosedAt:           closedAt,
		LedgerSequence:     uint32(ledgerSequence),
	}
	return transformedKey, nil
}
