package transform

import (
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// TransformPool converts an liquidity pool ledger change entry into a form suitable for BigQuery
func TransformPool(ledgerChange ingest.Change, header xdr.LedgerHeaderHistoryEntry, passphrase string) (PoolOutput, error) {
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
		return PoolOutput{}, fmt.Errorf("could not extract liquidity pool data from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	cp, ok := lp.Body.GetConstantProduct()
	if !ok {
		return PoolOutput{}, fmt.Errorf("could not extract constant product information for liquidity pool %s", xdr.Hash(lp.LiquidityPoolId).HexString())
	}

	poolType, ok := xdr.LiquidityPoolTypeToString[lp.Body.Type]
	if !ok {
		return PoolOutput{}, fmt.Errorf("unknown liquidity pool type: %d", lp.Body.Type)
	}

	assetAOutput, err := transformSingleAsset(cp.Params.AssetA, passphrase)
	if err != nil {
		return PoolOutput{}, err
	}

	assetBOutput, err := transformSingleAsset(cp.Params.AssetB, passphrase)
	if err != nil {
		return PoolOutput{}, err
	}

	closedAt, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return PoolOutput{}, err
	}

	ledgerSequence := header.Header.LedgerSeq

	var poolIDStrkey string
	poolIDStrkey, err = strkey.Encode(strkey.VersionByteLiquidityPool, lp.LiquidityPoolId[:])
	if err != nil {
		return PoolOutput{}, err
	}

	transformedPool := PoolOutput{
		PoolID:             PoolIDToString(lp.LiquidityPoolId),
		PoolType:           poolType,
		PoolFee:            uint32(cp.Params.Fee),
		TrustlineCount:     uint64(cp.PoolSharesTrustLineCount),
		PoolShareCount:     utils.ConvertStroopValueToReal(cp.TotalPoolShares),
		AssetAType:         assetAOutput.AssetType,
		AssetACode:         assetAOutput.AssetCode,
		AssetAIssuer:       assetAOutput.AssetIssuer,
		AssetAID:           assetAOutput.AssetID,
		AssetAReserve:      utils.ConvertStroopValueToReal(cp.ReserveA),
		AssetBType:         assetBOutput.AssetType,
		AssetBCode:         assetBOutput.AssetCode,
		AssetBIssuer:       assetBOutput.AssetIssuer,
		AssetBID:           assetBOutput.AssetID,
		AssetBReserve:      utils.ConvertStroopValueToReal(cp.ReserveB),
		LastModifiedLedger: uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:  uint32(changeType),
		Deleted:            outputDeleted,
		ClosedAt:           closedAt,
		LedgerSequence:     uint32(ledgerSequence),
		PoolIDStrkey:       poolIDStrkey,
		AssetAContractId:   assetAOutput.ContractId,
		AssetBContractId:   assetBOutput.ContractId,
	}
	return transformedPool, nil
}
