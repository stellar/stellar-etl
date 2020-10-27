package input

import (
	"fmt"
	"math"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/go/exp/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
)

type OrderbookBatch struct {
	BatchStart uint32
	BatchEnd   uint32
	Parser     OrderbookParser
}

// OrderbookParser handles parsing orderbooks
type OrderbookParser struct {
	Events            [][]byte
	Markets           [][]byte
	SeenMarketHashes  map[uint64]bool
	Offers            [][]byte
	SeenOfferHashes   map[uint64]bool
	Accounts          [][]byte
	SeenAccountHashes map[uint64]bool
}

func GetOfferChanges(core *ledgerbackend.CaptiveStellarCore, firstSeq, nextSeq uint32) (*ingestio.LedgerEntryChangeCache, error) {
	offChanges := ingestio.NewLedgerEntryChangeCache()

	for seq := firstSeq; seq <= nextSeq; {
		changeReader, err := ingestio.NewLedgerChangeReader(core, password, seq)
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("unable to create change reader for ledger %d: ", seq), err)
		}

		err = addLedgerChangesToCache(changeReader, nil, offChanges, nil)
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("unable to read changes from ledger %d: ", seq), err)
		}

		changeReader.Close()
		seq++
	}

	return offChanges, nil
}

func exportOrderbookBatch(batchStart, batchEnd uint32, core *ledgerbackend.CaptiveStellarCore, orderbookChan chan ChangeBatch, startOrderbook []ingestio.Change, logger *log.Entry) {
	prevSeq := batchStart
	curSeq := batchStart + 1
	for curSeq <= batchEnd {
		latestLedger, err := core.GetLatestLedgerSequence()
		if err != nil {
			logger.Error("unable to get the lastest ledger sequence: ", err)
		}

		// if this ledger is available, we process its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented and we try again on the next iteration of the loop
		if curSeq <= latestLedger {
			changeCache, err := GetOfferChanges(core, prevSeq, curSeq)
			if err != nil {
				logger.Fatal(fmt.Sprintf("unable to get offerchanges between ledger %d and %d", prevSeq, curSeq), err)
			}

			for _, change := range startOrderbook {
				changeCache.AddChange(change)
			}

			startOrderbook = changeCache.GetChanges()
			prevSeq = curSeq
			curSeq++
		}

	}
}

func StreamOrderbooks(core *ledgerbackend.CaptiveStellarCore, start, end, batchSize uint32, orderbookChannel chan ChangeBatch, startOrderbook []ingestio.Change, logger *log.Entry) {
	if end != 0 {
		totalBatches := uint32(math.Ceil(float64(end-start+1) / float64(batchSize)))
		for currentBatch := uint32(0); currentBatch < totalBatches; currentBatch++ {
			batchStart := start + currentBatch*batchSize
			batchEnd := batchStart + batchSize
			if batchEnd > end+1 {
				batchEnd = end + 1
			}
			exportOrderbookBatch(batchStart, batchEnd, core, orderbookChannel, startOrderbook, logger)
		}
	} else {
		batchStart := start
		batchEnd := batchStart + batchSize
		for {
			exportOrderbookBatch(batchStart, batchEnd, core, orderbookChannel, startOrderbook, logger)
			batchStart = batchEnd
			batchEnd = batchStart + batchSize
		}
	}
}
