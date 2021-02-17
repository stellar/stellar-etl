package transform

import (
	"fmt"

	"github.com/stellar/stellar-etl/internal/utils"

	"github.com/stellar/go/ingest"
)

// TransformOffer converts an account from the history archive ingestion system into a form suitable for BigQuery
func TransformOffer(ledgerChange ingest.Change) (OfferOutput, error) {
	ledgerEntry, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return OfferOutput{}, err
	}

	offerEntry, offerFound := ledgerEntry.Data.GetOffer()
	if !offerFound {
		return OfferOutput{}, fmt.Errorf("Could not extract offer data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	outputSellerID, err := offerEntry.SellerId.GetAddress()
	if err != nil {
		return OfferOutput{}, err
	}

	outputOfferID := int64(offerEntry.OfferId)
	if outputOfferID < 0 {
		return OfferOutput{}, fmt.Errorf("OfferID is negative (%d) for offer from account: %s", outputOfferID, outputSellerID)
	}

	outputSellingAsset, err := transformSingleAsset(offerEntry.Selling)
	if err != nil {
		return OfferOutput{}, err
	}

	outputBuyingAsset, err := transformSingleAsset(offerEntry.Buying)
	if err != nil {
		return OfferOutput{}, err
	}

	outputAmount := int64(offerEntry.Amount)
	if outputAmount < 0 {
		return OfferOutput{}, fmt.Errorf("Amount is negative (%d) for offer %d", outputAmount, outputOfferID)
	}

	outputPriceN := int32(offerEntry.Price.N)
	if outputPriceN < 0 {
		return OfferOutput{}, fmt.Errorf("Price numerator is negative (%d) for offer %d", outputPriceN, outputOfferID)
	}

	outputPriceD := int32(offerEntry.Price.D)
	if outputPriceD == 0 {
		return OfferOutput{}, fmt.Errorf("Price denominator is 0 for offer %d", outputOfferID)
	}

	if outputPriceD < 0 {
		return OfferOutput{}, fmt.Errorf("Price denominator is negative (%d) for offer %d", outputPriceD, outputOfferID)
	}

	var outputPrice float64
	if outputPriceN > 0 {
		outputPrice = float64(outputPriceN) / float64(outputPriceD)
	}

	outputFlags := uint32(offerEntry.Flags)

	outputLastModifiedLedger := uint32(ledgerEntry.LastModifiedLedgerSeq)

	transformedOffer := OfferOutput{
		SellerID:           outputSellerID,
		OfferID:            outputOfferID,
		SellingAsset:       outputSellingAsset.AssetID,
		BuyingAsset:        outputBuyingAsset.AssetID,
		Amount:             outputAmount,
		PriceN:             outputPriceN,
		PriceD:             outputPriceD,
		Price:              outputPrice,
		Flags:              outputFlags,
		LastModifiedLedger: outputLastModifiedLedger,
		Deleted:            outputDeleted,
	}
	return transformedOffer, nil
}
