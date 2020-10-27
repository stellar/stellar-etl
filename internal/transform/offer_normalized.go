package transform

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	ingestio "github.com/stellar/go/exp/ingest/io"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformOfferNormalized converts an offer into a normalized form, allowing it to be stored as part of the historical orderbook dataset
func TransformOfferNormalized(ledgerChange ingestio.Change, ledgerSeq uint32) (NormalizedOfferOutput, error) {
	normalized := NormalizedOfferOutput{}
	transformed, err := TransformOffer(ledgerChange)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	if transformed.Deleted {
		return NormalizedOfferOutput{}, nil
	}

	err = modifyOfferAsset(ledgerChange, &transformed)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	normalized.Market, err = extractDimMarket(transformed)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	normalized.Account, err = extractDimAccount(transformed)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	normalized.Offer, err = extractDimOffer(transformed, normalized.Market.ID, normalized.Account.ID)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	normalized.Event = FactOfferEvent{
		LedgerSeq:       ledgerSeq,
		OfferInstanceID: normalized.Offer.DimOfferID,
	}

	return normalized, nil
}

// modifyOfferAsset changes the buying and selling asset of a transformed offer from the asset hash to a string of the format code:issuer
func modifyOfferAsset(ledgerChange ingestio.Change, transformed *OfferOutput) error {
	ledgerEntry, _, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return err
	}

	offerEntry, offerFound := ledgerEntry.Data.GetOffer()
	if !offerFound {
		return fmt.Errorf("Could not extract offer data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	var sellType, sellCode, sellIssuer string
	err = offerEntry.Selling.Extract(&sellType, &sellCode, &sellIssuer)
	if err != nil {
		return err
	}

	var outputSellingAsset string
	if sellType != "native" {
		outputSellingAsset = fmt.Sprintf("%s:%s", sellCode, sellIssuer)
	} else {
		// native assets have an empty issuer
		outputSellingAsset = "native:"
	}

	var buyType, buyCode, buyIssuer string
	err = offerEntry.Buying.Extract(&buyType, &buyCode, &buyIssuer)
	if err != nil {
		return err
	}

	var outputBuyingAsset string
	if buyType != "native" {
		outputBuyingAsset = fmt.Sprintf("%s:%s", buyCode, buyIssuer)
	} else {
		outputBuyingAsset = "native:"
	}

	transformed.BuyingAsset = outputBuyingAsset
	transformed.SellingAsset = outputSellingAsset
	return nil
}

// extractDimMarket gets the DimMarket struct that corresponds to the provided offer
func extractDimMarket(offer OfferOutput) (DimMarket, error) {
	assets := []string{offer.BuyingAsset, offer.SellingAsset}
	// sort in order to ensure markets have consistent base/counter pairs
	// markets are stored as selling/buying == base/counter
	sort.Strings(assets)

	fnvHasher := fnv.New64a()
	if _, err := fnvHasher.Write([]byte(strings.Join(assets, "/"))); err != nil {
		return DimMarket{}, err
	}

	hash := fnvHasher.Sum64()

	sellSplit := strings.Split(assets[0], ":")
	buySplit := strings.Split(assets[1], ":")

	baseCode, baseIssuer := sellSplit[0], sellSplit[1]
	counterCode, counterIssuer := buySplit[0], buySplit[1]

	return DimMarket{
		ID:            hash,
		BaseCode:      baseCode,
		BaseIssuer:    baseIssuer,
		CounterCode:   counterCode,
		CounterIssuer: counterIssuer,
	}, nil
}

// extractDimOffer extracts the DimOffer struct from the provided offer
func extractDimOffer(offer OfferOutput, marketID, makerID uint64) (DimOffer, error) {
	importantFields := fmt.Sprintf("%d/%d/%f", offer.OfferID, offer.Amount, offer.Price)

	fnvHasher := fnv.New64a()
	if _, err := fnvHasher.Write([]byte(importantFields)); err != nil {
		return DimOffer{}, err
	}

	offerHash := fnvHasher.Sum64()

	assets := []string{offer.BuyingAsset, offer.SellingAsset}
	sort.Strings(assets)

	var action string
	if offer.SellingAsset == assets[0] {
		action = "s"
	} else {
		action = "b"
	}

	return DimOffer{
		HorizonID:     offer.OfferID,
		DimOfferID:    offerHash,
		MarketID:      marketID,
		MakerID:       makerID,
		Action:        action,
		BaseAmount:    offer.Amount,
		CounterAmount: float64(offer.Amount) * offer.Price,
		Price:         offer.Price,
	}, nil
}

// extractDimAccount gets the DimAccount struct that corresponds to the provided offer
func extractDimAccount(offer OfferOutput) (DimAccount, error) {
	var fnvHasher = fnv.New64a()
	if _, err := fnvHasher.Write([]byte(offer.SellerID)); err != nil {
		return DimAccount{}, err
	}

	accountID := fnvHasher.Sum64()
	return DimAccount{
		Address: offer.SellerID,
		ID:      accountID,
	}, nil
}
