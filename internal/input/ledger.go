package input

import (
	"fmt"

	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/xdr"
)

func createBackend() (*ledgerbackend.HistoryArchiveBackend, error) {
	archiveStellarURL := "http://history.stellar.org/prd/core-live/core_live_001"
	return ledgerbackend.NewHistoryArchiveBackendFromURL(archiveStellarURL)
}

func validateLedgerRange(start, end, latestNum uint32) error {
	if end < start {
		return fmt.Errorf("End sequence number is less than start (%d < %d)", end, start)
	}

	if latestNum < start {
		return fmt.Errorf("Latest sequence number is less than start sequence number (%d < %d)", latestNum, start)
	}

	if latestNum < end {
		return fmt.Errorf("Latest sequence number is less than end sequence number (%d < %d)", latestNum, end)
	}

	return nil
}

// GetLedgers returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgers(start, end, limit uint32) ([]xdr.LedgerCloseMeta, error) {
	backend, err := createBackend()
	if err != nil {
		return []xdr.LedgerCloseMeta{}, err
	}

	latestNum, err := backend.GetLatestLedgerSequence()
	if err != nil {
		return []xdr.LedgerCloseMeta{}, err
	}

	err = validateLedgerRange(start, end, latestNum)
	if err != nil {
		return []xdr.LedgerCloseMeta{}, err
	}

	metaSlice := []xdr.LedgerCloseMeta{}
	for seq := start; seq <= end; seq++ {
		ok, ledger, err := backend.GetLedger(seq)
		if !ok {
			return []xdr.LedgerCloseMeta{}, fmt.Errorf("Ledger %d does not exist in the history archives", seq)
		}

		if err != nil {
			return []xdr.LedgerCloseMeta{}, err
		}

		metaSlice = append(metaSlice, ledger)
		if uint32(len(metaSlice)) >= limit {
			break
		}
	}

	return metaSlice, nil
}
