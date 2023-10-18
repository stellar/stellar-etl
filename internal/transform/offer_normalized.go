package transform

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

// TransformOfferNormalized converts an offer into a normalized form, allowing it to be stored as part of the historical orderbook dataset
func TransformOfferNormalized(ledgerChange ingest.Change, ledgerSeq uint32, lcm xdr.LedgerCloseMeta) (NormalizedOfferOutput, error) {

	transformed, err := TransformOffer(ledgerChange, lcm)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	if transformed.Deleted {
		return NormalizedOfferOutput{}, fmt.Errorf("offer %d is deleted", transformed.OfferID)
	}

	buyingAsset, sellingAsset, err := extractAssets(ledgerChange, transformed)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	outputMarket, err := extractDimMarket(transformed, buyingAsset, sellingAsset)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	outputAccount, err := extractDimAccount(transformed)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	outputOffer, err := extractDimOffer(transformed, buyingAsset, sellingAsset, outputMarket.ID, outputAccount.ID)
	if err != nil {
		return NormalizedOfferOutput{}, err
	}

	return NormalizedOfferOutput{
		Market:  outputMarket,
		Account: outputAccount,
		Offer:   outputOffer,
		Event: FactOfferEvent{
			LedgerSeq:       ledgerSeq,
			OfferInstanceID: outputOffer.DimOfferID,
		},
	}, nil
}

// extractAssets extracts the buying and selling assets as strings of the format code:issuer
func extractAssets(ledgerChange ingest.Change, transformed OfferOutput) (string, string, error) {
	ledgerEntry, _, _, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return "", "", err
	}

	offerEntry, offerFound := ledgerEntry.Data.GetOffer()
	if !offerFound {
		return "", "", fmt.Errorf("Could not extract offer data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	var sellType, sellCode, sellIssuer string
	err = offerEntry.Selling.Extract(&sellType, &sellCode, &sellIssuer)
	if err != nil {
		return "", "", err
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
		return "", "", err
	}

	var outputBuyingAsset string
	if buyType != "native" {
		outputBuyingAsset = fmt.Sprintf("%s:%s", buyCode, buyIssuer)
	} else {
		outputBuyingAsset = "native:"
	}

	return outputBuyingAsset, outputSellingAsset, nil
}

// extractDimMarket gets the DimMarket struct that corresponds to the provided offer and its buying/selling assets
func extractDimMarket(offer OfferOutput, buyingAsset, sellingAsset string) (DimMarket, error) {
	assets := []string{buyingAsset, sellingAsset}
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

	if len(sellSplit) < 2 {
		return DimMarket{}, fmt.Errorf("unable to get sell code and issuer for offer %d", offer.OfferID)
	}

	if len(buySplit) < 2 {
		return DimMarket{}, fmt.Errorf("unable to get buy code and issuer for offer %d", offer.OfferID)
	}

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

// extractDimOffer extracts the DimOffer struct from the provided offer and its buying/selling assets
func extractDimOffer(offer OfferOutput, buyingAsset, sellingAsset string, marketID, makerID uint64) (DimOffer, error) {
	importantFields := fmt.Sprintf("%d/%f/%f", offer.OfferID, offer.Amount, offer.Price)

	fnvHasher := fnv.New64a()
	if _, err := fnvHasher.Write([]byte(importantFields)); err != nil {
		return DimOffer{}, err
	}

	offerHash := fnvHasher.Sum64()

	assets := []string{buyingAsset, sellingAsset}
	sort.Strings(assets)

	var action string
	if sellingAsset == assets[0] {
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
