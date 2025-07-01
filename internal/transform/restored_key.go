package transform

import (
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformRestoredKey fetches keyhash from restored ledger change entry into a form suitable for BigQuery
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
	ledgerKeyHash, err := xdr.MarshalBase64(key)
	ledgerEntryType := key.Type.String()
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
		LedgerKeyHash:      ledgerKeyHash,
		LedgerEntryType:    ledgerEntryType,
		LastModifiedLedger: outputLastModifiedLedger,
		ClosedAt:           closedAt,
		LedgerSequence:     uint32(ledgerSequence),
	}
	return transformedKey, nil
}
