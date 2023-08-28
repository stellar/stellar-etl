package transform

import (
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/internal/utils"
)

// TransformPool converts an liquidity pool ledger change entry into a form suitable for BigQuery
func TransformPool(ledgerChange ingest.Change, ledgerCloseMeta xdr.LedgerCloseMeta) (PoolOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return PoolOutput{}, err
	}

	// LedgerEntryChange must contain a liquidity pool state change to be parsed, otherwise skip
	if ledgerEntry.Data.Type != xdr.LedgerEntryTypeLiquidityPool {
		return PoolOutput{}, nil
	}

	lp, ok := ledgerEntry.Data.GetLiquidityPool()
	if !ok {
		return PoolOutput{}, fmt.Errorf("Could not extract liquidity pool data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	outputCloseTime, err := utils.GetCloseTime(ledgerCloseMeta)
	if err != nil {
		return PoolOutput{}, err
	}

	cp, ok := lp.Body.GetConstantProduct()
	if !ok {
		return PoolOutput{}, fmt.Errorf("Could not extract constant product information for liquidity pool %s", xdr.Hash(lp.LiquidityPoolId).HexString())
	}

	poolType, ok := xdr.LiquidityPoolTypeToString[lp.Body.Type]
	if !ok {
		return PoolOutput{}, fmt.Errorf("Unknown liquidity pool type: %d", lp.Body.Type)
	}

	var assetAType, assetACode, assetAIssuer string
	err = cp.Params.AssetA.Extract(&assetAType, &assetACode, &assetAIssuer)
	if err != nil {
		return PoolOutput{}, err
	}
	assetAID := FarmHashAsset(assetAType, assetACode, assetAIssuer)

	var assetBType, assetBCode, assetBIssuer string
	err = cp.Params.AssetB.Extract(&assetBType, &assetBCode, &assetBIssuer)
	assetBID := FarmHashAsset(assetBType, assetBCode, assetBIssuer)

	transformedPool := PoolOutput{
		PoolID:             PoolIDToString(lp.LiquidityPoolId),
		PoolType:           poolType,
		PoolFee:            uint32(cp.Params.Fee),
		TrustlineCount:     uint64(cp.PoolSharesTrustLineCount),
		PoolShareCount:     utils.ConvertStroopValueToReal(cp.TotalPoolShares),
		AssetAType:         assetAType,
		AssetACode:         assetACode,
		AssetAIssuer:       assetAIssuer,
		AssetAID:           assetAID,
		AssetAReserve:      utils.ConvertStroopValueToReal(cp.ReserveA),
		AssetBType:         assetBType,
		AssetBCode:         assetBCode,
		AssetBIssuer:       assetBIssuer,
		AssetBID:           assetBID,
		AssetBReserve:      utils.ConvertStroopValueToReal(cp.ReserveB),
		LastModifiedLedger: uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:  uint32(changeType),
		Deleted:            outputDeleted,
		ClosedAt:           outputCloseTime,
	}
	return transformedPool, nil
}
