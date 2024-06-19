package input

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
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
	Logger            *utils.EtlLogger
}

// convertOffer converts an offer to its normalized form and adds it to the AllConvertedOffers
func (o *OrderbookParser) convertOffer(allConvertedOffers []transform.NormalizedOfferOutput, index int, offer ingest.Change, seq uint32, wg *sync.WaitGroup) {
	defer wg.Done()
	transformed, err := transform.TransformOfferNormalized(offer, seq)
	if err != nil {
		errorMsg := fmt.Errorf("error json marshalling offer #%d in ledger sequence number #%d: %s", index, seq, err)
		o.Logger.LogError(errorMsg)
	} else {
		allConvertedOffers[index] = transformed
	}
}

// NewOrderbookParser creates a new orderbook parser and returns it
func NewOrderbookParser(logger *utils.EtlLogger) OrderbookParser {
	return OrderbookParser{
		Events:            make([][]byte, 0),
		Markets:           make([][]byte, 0),
		SeenMarketHashes:  make(map[uint64]bool),
		Offers:            make([][]byte, 0),
		SeenOfferHashes:   make(map[uint64]bool),
		Accounts:          make([][]byte, 0),
		SeenAccountHashes: make(map[uint64]bool),
		Logger:            logger,
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
				errorMsg := fmt.Errorf("error json marshalling market for offer  %d: %s", converted.Offer.HorizonID, err)
				o.Logger.LogError(errorMsg)
				continue
			}

			o.Markets = append(o.Markets, marshalledMarket)
		}

		if _, exists := o.SeenAccountHashes[converted.Account.ID]; !exists {
			o.SeenAccountHashes[converted.Account.ID] = true
			marshalledAccount, err := json.Marshal(converted.Account)
			if err != nil {
				errorMsg := fmt.Errorf("error json marshalling account for offer  %d: %s", converted.Offer.HorizonID, err)
				o.Logger.LogError(errorMsg)
				continue
			}

			o.Accounts = append(o.Accounts, marshalledAccount)
		}

		if _, exists := o.SeenOfferHashes[converted.Offer.DimOfferID]; !exists {
			o.SeenOfferHashes[converted.Offer.DimOfferID] = true
			marshalledOffer, err := json.Marshal(converted.Offer)
			if err != nil {
				errorMsg := fmt.Errorf("error json marshalling offer %d: %s", converted.Offer.HorizonID, err)
				o.Logger.LogError(errorMsg)
				continue
			}

			o.Offers = append(o.Offers, marshalledOffer)

		}

		marshalledEvent, err := json.Marshal(converted.Event)
		if err != nil {
			errorMsg := fmt.Errorf("error json marshalling event for offer %d: %s", converted.Offer.HorizonID, err)
			o.Logger.LogError(errorMsg)
			continue
		} else {
			o.Events = append(o.Events, marshalledEvent)
		}
	}
}

// GetOfferChanges gets the offer changes that occurred between the firstSeq ledger and nextSeq ledger
func GetOfferChanges(core *ledgerbackend.CaptiveStellarCore, env utils.EnvironmentDetails, firstSeq, nextSeq uint32) (*ingest.ChangeCompactor, error) {
	offChanges := ingest.NewChangeCompactor()
	ctx := context.Background()

	for seq := firstSeq; seq <= nextSeq; {
		latestLedger, err := core.GetLatestLedgerSequence(ctx)
		if err != nil {
			return nil, fmt.Errorf(fmt.Sprintf("unable to get latest ledger at ledger %d: ", seq), err)
		}

		// if this ledger is available, we can read its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented and we try again on the next iteration of the loop
		if seq <= latestLedger {
			changeReader, err := ingest.NewLedgerChangeReader(ctx, core, env.NetworkPassphrase, seq)
			if err != nil {
				return nil, fmt.Errorf(fmt.Sprintf("unable to create change reader for ledger %d: ", seq), err)
			}

			for {
				change, err := changeReader.Read()
				if err == io.EOF {
					break
				}

				if err != nil {
					return nil, fmt.Errorf(fmt.Sprintf("unable to read changes from ledger %d: ", seq), err)
				}
				offChanges.AddChange(change)
			}

			changeReader.Close()
			seq++
		}
	}

	return offChanges, nil
}

func exportOrderbookBatch(batchStart, batchEnd uint32, core *ledgerbackend.CaptiveStellarCore, orderbookChan chan OrderbookBatch, startOrderbook []ingest.Change, env utils.EnvironmentDetails, logger *utils.EtlLogger) {
	batchMap := make(map[uint32][]ingest.Change)
	batchMap[batchStart] = make([]ingest.Change, len(startOrderbook))
	copy(batchMap[batchStart], startOrderbook)
	ctx := context.Background()

	prevSeq := batchStart
	curSeq := batchStart + 1
	for curSeq < batchEnd {
		latestLedger, err := core.GetLatestLedgerSequence(ctx)
		if err != nil {
			logger.Fatal("unable to get the lastest ledger sequence: ", err)
		}

		// if this ledger is available, we process its changes and move on to the next ledger by incrementing seq.
		// Otherwise, nothing is incremented and we try again on the next iteration of the loop
		if curSeq <= latestLedger {
			UpdateOrderbook(prevSeq, curSeq, startOrderbook, core, env, logger)
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
func UpdateOrderbook(start, end uint32, orderbook []ingest.Change, core *ledgerbackend.CaptiveStellarCore, env utils.EnvironmentDetails, logger *utils.EtlLogger) {
	if start > end {
		logger.Fatalf("unable to update orderbook start ledger %d is after end %d: ", start, end)
	}

	changeCache, err := GetOfferChanges(core, env, start, end)
	if err != nil {
		logger.Fatal(fmt.Sprintf("unable to get offer changes between ledger %d and %d: ", start, end), err)
	}

	for _, change := range orderbook {
		changeCache.AddChange(change)
	}

	orderbook = changeCache.GetChanges()
}

// StreamOrderbooks exports all the batches of orderbooks between start and end to the orderbookChannel. If end is 0, then it exports in an unbounded fashion
func StreamOrderbooks(core *ledgerbackend.CaptiveStellarCore, start, end, batchSize uint32, orderbookChannel chan OrderbookBatch, startOrderbook []ingest.Change, env utils.EnvironmentDetails, logger *utils.EtlLogger) {
	// The initial orderbook is at the checkpoint sequence, not the start of the range, so it needs to be updated
	checkpointSeq := utils.GetMostRecentCheckpoint(start)
	UpdateOrderbook(checkpointSeq, start, startOrderbook, core, env, logger)

	if end != 0 {
		totalBatches := uint32(math.Ceil(float64(end-start+1) / float64(batchSize)))
		for currentBatch := uint32(0); currentBatch < totalBatches; currentBatch++ {
			batchStart := start + currentBatch*batchSize
			batchEnd := batchStart + batchSize
			if batchEnd > end+1 {
				batchEnd = end + 1
			}

			exportOrderbookBatch(batchStart, batchEnd, core, orderbookChannel, startOrderbook, env, logger)
		}
	} else {
		batchStart := start
		batchEnd := batchStart + batchSize
		for {
			exportOrderbookBatch(batchStart, batchEnd, core, orderbookChannel, startOrderbook, env, logger)
			batchStart = batchEnd
			batchEnd = batchStart + batchSize
		}
	}
}

// ReceiveParsedOrderbooks reads a batch from the orderbookChannel, parses it using an orderbook parser, and returns the parser.
func ReceiveParsedOrderbooks(orderbookChannel chan OrderbookBatch, logger *utils.EtlLogger) *OrderbookParser {
	batchParser := NewOrderbookParser(logger)
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
