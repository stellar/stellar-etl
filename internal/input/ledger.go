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

func validateLedgerRange(start uint32, end uint32, backend ledgerbackend.HistoryArchiveBackend) error {
	if start < 0 {
		return fmt.Errorf("Start sequence number (%d) is negative", start)
	}

	if end < 0 {
		return fmt.Errorf("End sequence number (%d) is negative", end)
	}

	if end < start {
		return fmt.Errorf("End sequence number is less than end (%d < %d)", end, start)
	}

	latestNum, err := backend.GetLatestLedgerSequence()
	if err != nil {
		return err
	}

	if latestNum < start {
		return fmt.Errorf("Latest sequence number is less than start sequence number (%d < %d)", latestNum, start)
	}

	if latestNum < end {
		return fmt.Errorf("Latest sequence number is less than start sequence number (%d < %d)", latestNum, end)
	}

	return nil
}

// GetLedgers returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgers(start uint32, end uint32) ([]xdr.LedgerCloseMeta, error) {
	backend, err := createBackend()
	if err != nil {
		return []xdr.LedgerCloseMeta{}, err
	}

	err = validateLedgerRange(start, end, *backend)
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
	}

	return metaSlice, nil
}
