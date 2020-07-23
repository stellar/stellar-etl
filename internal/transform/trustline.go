package transform

import (
	"encoding/base64"
	"fmt"

	"github.com/stellar/go/xdr"
)

//TransformTrustline converts a trustline from the history archive ingestion system into a form suitable for BigQuery
func TransformTrustline(ledgerEntry xdr.LedgerEntry) (TrustlineOutput, error) {
	trustEntry, ok := ledgerEntry.Data.GetTrustLine()
	if !ok {
		return TrustlineOutput{}, fmt.Errorf("Could not extract trustline data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	outputLedgerKey, err := trustLineEntryToLedgerKeyString(trustEntry)
	if err != nil {
		return TrustlineOutput{}, err
	}

	outputAccountID, err := trustEntry.AccountId.GetAddress()
	if err != nil {
		return TrustlineOutput{}, err
	}

	var assetType, outputAssetCode, outputAssetIssuer string

	asset := trustEntry.Asset
	err = asset.Extract(&assetType, &outputAssetCode, &outputAssetIssuer)
	if err != nil {
		return TrustlineOutput{}, err
	}

	outputAssetType := int32(asset.Type)

	outputBalance := int64(trustEntry.Balance)
	if outputBalance < 0 {
		return TrustlineOutput{}, fmt.Errorf("Balance is negative (%d) for trustline", outputBalance)
	}

	outputLimit := int64(trustEntry.Limit)
	if outputLimit < 0 {
		return TrustlineOutput{}, fmt.Errorf("Limit is negative (%d) for trustline", outputLimit)
	}

	//The V1 struct is the first version of the extender from trustlineEntry. It contains information on liabilities, and in the future
	//more extensions may contain extra information
	trustlineExtensionInfo, V1Found := trustEntry.Ext.GetV1()
	var outputBuyingLiabilities, outputSellingLiabilities int64
	if V1Found {
		liabilities := trustlineExtensionInfo.Liabilities
		outputBuyingLiabilities, outputSellingLiabilities = int64(liabilities.Buying), int64(liabilities.Selling)
		if outputBuyingLiabilities < 0 {
			return TrustlineOutput{}, fmt.Errorf("The buying liabilities count is negative (%d) for trustline", outputBuyingLiabilities)
		}

		if outputSellingLiabilities < 0 {
			return TrustlineOutput{}, fmt.Errorf("The selling liabilities count is negative (%d) for trustline", outputSellingLiabilities)
		}
	}

	outputFlags := uint32(trustEntry.Flags)

	outputLastModifiedLedger := uint32(ledgerEntry.LastModifiedLedgerSeq)

	transformedTrustline := TrustlineOutput{
		LedgerKey:          outputLedgerKey,
		AccountID:          outputAccountID,
		AssetType:          outputAssetType,
		AssetCode:          outputAssetCode,
		AssetIssuer:        outputAssetIssuer,
		Balance:            outputBalance,
		TrustlineLimit:     outputLimit,
		BuyingLiabilities:  outputBuyingLiabilities,
		SellingLiabilities: outputSellingLiabilities,
		Flags:              outputFlags,
		LastModifiedLedger: outputLastModifiedLedger,
	}

	return transformedTrustline, nil
}

func trustLineEntryToLedgerKeyString(trustLine xdr.TrustLineEntry) (string, error) {
	ledgerKey := &xdr.LedgerKey{}
	err := ledgerKey.SetTrustline(trustLine.AccountId, trustLine.Asset)
	if err != nil {
		return "", fmt.Errorf("Error running ledgerKey.SetTrustline when calculating ledger key")
	}

	key, err := ledgerKey.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("Error running MarshalBinaryCompress when calculating ledger key")
	}

	return base64.StdEncoding.EncodeToString(key), nil
}
