package input

import (
	"fmt"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/xdr"
)

// GetLedgers returns a slice of ledger close metas for the ledgers in the provided range (inclusive on both ends)
func GetLedgers(start, end uint32, limit int64) ([]xdr.LedgerCloseMeta, error) {
	backend, err := utils.CreateBackend(start, end)
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
