package input

import (
	"context"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// LedgerBatch represents a batch of pre-fetched ledger close metas covering
// the inclusive range [BatchStart, BatchEnd].
type LedgerBatch struct {
	BatchStart uint32
	BatchEnd   uint32
	Ledgers    []xdr.LedgerCloseMeta
}

// StreamLedgerBatches fetches ledgers in batch-size increments from the given
// backend and sends each batch on batchChan. When done, it closes batchChan
// and sends on closeChan. The caller owns the backend and is responsible for
// PrepareRange.
//
// Unlike the batch loop in StreamChanges (which skips single-ledger ranges),
// StreamLedgerBatches iterates inclusively and always emits at least one
// batch when start <= end.
func StreamLedgerBatches(
	backend *ledgerbackend.LedgerBackend,
	start, end, batchSize uint32,
	batchChan chan LedgerBatch,
	closeChan chan int,
	logger *utils.EtlLogger,
) {
	ctx := context.Background()
	batchStart := start
	for batchStart <= end {
		batchEnd := batchStart + batchSize - 1
		if batchEnd > end || batchEnd < batchStart {
			batchEnd = end
		}

		ledgers := make([]xdr.LedgerCloseMeta, 0, batchEnd-batchStart+1)
		for seq := batchStart; seq <= batchEnd; seq++ {
			lcm, err := (*backend).GetLedger(ctx, seq)
			if err != nil {
				logger.Fatalf("unable to get ledger %d from backend: %v", seq, err)
			}
			ledgers = append(ledgers, lcm)
		}

		batchChan <- LedgerBatch{
			BatchStart: batchStart,
			BatchEnd:   batchEnd,
			Ledgers:    ledgers,
		}

		if batchEnd == end {
			break
		}
		batchStart = batchEnd + 1
	}
	close(batchChan)
	closeChan <- 1
}
