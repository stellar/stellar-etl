package transform

import (
	"encoding/base64"
	"fmt"

	"github.com/pkg/errors"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

//TransformTrustline converts a trustline from the history archive ingestion system into a form suitable for BigQuery
func TransformTrustline(ledgerChange ingest.Change) (TrustlineOutput, error) {
	ledgerEntry, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return TrustlineOutput{}, err
	}

	trustEntry, ok := ledgerEntry.Data.GetTrustLine()
	if !ok {
		return TrustlineOutput{}, fmt.Errorf("Could not extract trustline data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	outputAccountID, err := trustEntry.AccountId.GetAddress()
	if err != nil {
		return TrustlineOutput{}, err
	}

	var assetType, outputAssetCode, outputAssetIssuer string

	asset := trustEntry.Asset
	err = asset.Extract(&assetType, &outputAssetCode, &outputAssetIssuer)
	if err != nil {
		return TrustlineOutput{}, errors.Wrap(err, fmt.Sprintf("could not parse asset for trustline with account %s", outputAccountID))
	}

	outputLedgerKey, err := trustLineEntryToLedgerKeyString(trustEntry)
	if err != nil {
		return TrustlineOutput{}, errors.Wrap(err, fmt.Sprintf("could not create ledger key string for trustline with account %s and asset %s", outputAccountID, asset))
	}

	outputAssetType := int32(asset.Type)

	outputBalance := int64(trustEntry.Balance)
	if outputBalance < 0 {
		return TrustlineOutput{}, fmt.Errorf("Balance is negative (%d) for trustline (account is %s and asset is %s)", outputBalance, outputAccountID, asset)
	}

	outputLimit := int64(trustEntry.Limit)
	if outputLimit < 0 {
		return TrustlineOutput{}, fmt.Errorf("Limit is negative (%d) for trustline (account is %s and asset is %s)", outputLimit, outputAccountID, asset)
	}

	//The V1 struct is the first version of the extender from trustlineEntry. It contains information on liabilities, and in the future
	//more extensions may contain extra information
	trustlineExtensionInfo, V1Found := trustEntry.Ext.GetV1()
	var outputBuyingLiabilities, outputSellingLiabilities int64
	if V1Found {
		liabilities := trustlineExtensionInfo.Liabilities
		outputBuyingLiabilities, outputSellingLiabilities = int64(liabilities.Buying), int64(liabilities.Selling)
		if outputBuyingLiabilities < 0 {
			return TrustlineOutput{}, fmt.Errorf("The buying liabilities count is negative (%d) for trustline (account is %s and asset is %s)", outputBuyingLiabilities, outputAccountID, asset)
		}

		if outputSellingLiabilities < 0 {
			return TrustlineOutput{}, fmt.Errorf("The selling liabilities count is negative (%d) for trustline (account is %s and asset is %s)", outputSellingLiabilities, outputAccountID, asset)
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
		Deleted:            outputDeleted,
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
