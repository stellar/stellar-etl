package input

import (
	"fmt"

	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

func validateLedgerRange(start, end, latestNum uint32) error {
	if start == 0 {
		return fmt.Errorf("Start sequence number equal to 0. There is no ledger 0 (genesis ledger is ledger 1)")
	}

	if end == 0 {
		return fmt.Errorf("End sequence number equal to 0. There is no ledger 0 (genesis ledger is ledger 1)")
	}

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
func GetLedgers(start, end uint32, limit int64) ([]xdr.LedgerCloseMeta, error) {
	backend, err := utils.CreateBackend()
	if err != nil {
		return []xdr.LedgerCloseMeta{}, err
	}

	defer backend.Close()

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
		if int64(len(metaSlice)) >= limit && limit >= 0 {
			break
		}
	}

	return metaSlice, nil
}
