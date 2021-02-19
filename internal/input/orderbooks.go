package input

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/log"
	"github.com/stellar/stellar-etl/internal/transform"
	"github.com/stellar/stellar-etl/internal/utils"
)

// OrderbookBatch represents a batch of orderbooks
type OrderbookBatch struct {
	BatchStart uint32
	BatchEnd   uint32
	Orderbooks map[uint32][]ingest.Change
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
	Logger            *log.Entry
	Strict            bool
}

func (o *OrderbookParser) convertOffer(allConvertedOffers []transform.NormalizedOfferOutput, index int, offer ingest.Change, seq uint32, wg *sync.WaitGroup) {
	defer wg.Done()
	transformed, err := transform.TransformOfferNormalized(offer, seq)
	if err != nil {
		errorMsg := fmt.Sprintf("error json marshalling offer #%d in ledger sequence number #%d", index, seq)
		if o.Strict {
			o.Logger.Fatal(errorMsg, err)
		} else {
			o.Logger.Warning(errorMsg, err)
		}
	} else {
		allConvertedOffers[index] = transformed
	}
}

func NewOrderbookParser(strictExport bool, logger *log.Entry) OrderbookParser {
	return OrderbookParser{
		Events:            make([][]byte, 0),
		Markets:           make([][]byte, 0),
		SeenMarketHashes:  make(map[uint64]bool),
		Offers:            make([][]byte, 0),
		SeenOfferHashes:   make(map[uint64]bool),
		Accounts:          make([][]byte, 0),
		SeenAccountHashes: make(map[uint64]bool),
		Logger:            logger,
		Strict:            strictExport,
	}
}

func (o *OrderbookParser) parseOrderbook(orderbook []ingest.Change, seq uint32) {
	var group sync.WaitGroup
	allConverted := make([]transform.NormalizedOfferOutput, len(orderbook))
	for i, v := range orderbook {
		group.Add(1)
		go o.convertOffer(allConverted, i, v, seq, &group)
	}

	group.Wait()

	for _, converted := range allConverted {
		if _, exists := o.SeenMarketHashes[converted.Market.ID]; !exists {
			o.SeenMarketHashes[converted.Market.ID] = true
			marshalledMarket, err := json.Marshal(converted.Market)
			if err != nil {
				errorMsg := fmt.Sprintf("error json marshalling market for offer: %d", converted.Offer.HorizonID)
				if o.Strict {
					o.Logger.Fatal(errorMsg, err)
				} else {
					o.Logger.Warning(errorMsg, err)
					continue
				}
			}

			o.Markets = append(o.Markets, marshalledMarket)
		}

		if _, exists := o.SeenAccountHashes[converted.Account.ID]; !exists {
			o.SeenAccountHashes[converted.Account.ID] = true
			marshalledAccount, err := json.Marshal(converted.Account)
			if err != nil {
				errorMsg := fmt.Sprintf("error json marshalling account for offer: %d", converted.Offer.HorizonID)
				if o.Strict {
					o.Logger.Fatal(errorMsg, err)
				} else {
					o.Logger.Warning(errorMsg, err)
					continue
				}
			}

			o.Accounts = append(o.Accounts, marshalledAccount)
		}

		if _, exists := o.SeenOfferHashes[converted.Offer.DimOfferID]; !exists {
			o.SeenOfferHashes[converted.Offer.DimOfferID] = true
			marshalledOffer, err := json.Marshal(converted.Offer)
			if err != nil {
				errorMsg := fmt.Sprintf("error json marshalling offer: %d", converted.Offer.HorizonID)
				if o.Strict {
					o.Logger.Fatal(errorMsg, err)
				} else {
					o.Logger.Warning(errorMsg, err)
					continue
				}
			}

			o.Offers = append(o.Offers, marshalledOffer)

		}

		marshalledEvent, err := json.Marshal(converted.Event)
		if err != nil {
			errorMsg := fmt.Sprintf("error json marshalling event for offer: %d", converted.Offer.HorizonID)
			if o.Strict {
				o.Logger.Fatal(errorMsg, err)
			} else {
				o.Logger.Warning(errorMsg, err)
				continue
			}
		} else {
			o.Events = append(o.Events, marshalledEvent)
		}
	}
}

// GetOfferChanges gets the offer changes that ocurred between the firstSeq ledger and nextSeq ledger
func GetOfferChanges(core *ledgerbackend.CaptiveStellarCore, firstSeq, nextSeq uint32) (*ingest.ChangeCompactor, error) {
	offChanges := ingest.NewChangeCompactor()

	for seq := firstSeq; seq <= nextSeq; {
		latestLedger, err := core.GetLatestLedgerSequence()
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("unable to get latest ledger at ledger %d: ", seq), err)
		}

		// if this ledger is available, we can read its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented and we try again on the next iteration of the loop
		if seq <= latestLedger {
			changeReader, err := ingest.NewLedgerChangeReader(core, password, seq)
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
	}

	return offChanges, nil
}

func exportOrderbookBatch(batchStart, batchEnd uint32, core *ledgerbackend.CaptiveStellarCore, orderbookChan chan OrderbookBatch, startOrderbook []ingest.Change, logger *log.Entry) {
	batchMap := make(map[uint32][]ingest.Change)
	batchMap[batchStart] = make([]ingest.Change, len(startOrderbook))
	copy(batchMap[batchStart], startOrderbook)

	prevSeq := batchStart
	curSeq := batchStart + 1
	for curSeq < batchEnd {
		latestLedger, err := core.GetLatestLedgerSequence()
		if err != nil {
			logger.Fatal("unable to get the lastest ledger sequence: ", err)
		}

		// if this ledger is available, we process its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented and we try again on the next iteration of the loop
		if curSeq <= latestLedger {
			UpdateOrderbook(prevSeq, curSeq, startOrderbook, core, logger)
			batchMap[curSeq] = make([]ingest.Change, len(startOrderbook))
			copy(batchMap[curSeq], startOrderbook)
			prevSeq = curSeq
			curSeq++
		}
	}

	batch := OrderbookBatch{
		BatchStart: batchStart,
		BatchEnd:   batchEnd,
		Orderbooks: batchMap,
	}

	orderbookChan <- batch
}

// UpdateOrderbook updates an orderbook at ledger start to its state at ledger end
func UpdateOrderbook(start, end uint32, orderbook []ingest.Change, core *ledgerbackend.CaptiveStellarCore, logger *log.Entry) {
	if start > end {
		logger.Fatalf("unable to update orderbook start ledger %d is after end %d: ", start, end)
	}

	changeCache, err := GetOfferChanges(core, start, end)
	if err != nil {
		logger.Fatal(fmt.Sprintf("unable to get offer changes between ledger %d and %d: ", start, end), err)
	}

	for _, change := range orderbook {
		changeCache.AddChange(change)
	}

	orderbook = changeCache.GetChanges()
}

// StreamOrderbooks exports all the batches of orderbooks between start and end to the orderbookChannel. If end is 0, then it exports in an unbounded fashion
func StreamOrderbooks(core *ledgerbackend.CaptiveStellarCore, start, end, batchSize uint32, orderbookChannel chan OrderbookBatch, startOrderbook []ingest.Change, logger *log.Entry) {
	// The initial orderbook is at the checkpoint sequence, not the start of the range, so it needs to be updated
	checkpointSeq := utils.GetMostRecentCheckpoint(start)
	UpdateOrderbook(checkpointSeq, start, startOrderbook, core, logger)

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

// ReceiveParsedOrderbooks reads a batch from the orderbookChannel, parses it using an orderbook parser, and returns the parser.
func ReceiveParsedOrderbooks(orderbookChannel chan OrderbookBatch, strictExport bool, logger *log.Entry) *OrderbookParser {
	batchParser := NewOrderbookParser(strictExport, logger)
	batchRead := false
	for {
		select {
		case batch, ok := <-orderbookChannel:
			// if ok is false, it means the channel is closed. There will be no more batches, so we can set the channel to nil
			if !ok {
				orderbookChannel = nil
				break
			}

			for seq, orderbook := range batch.Orderbooks {
				batchParser.parseOrderbook(orderbook, seq)
			}

			batchRead = true
		}

		//if we have read a batch or the channel is closed, then break
		if batchRead || orderbookChannel == nil {
			break
		}
	}

	return &batchParser
}
