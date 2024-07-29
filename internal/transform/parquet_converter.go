// This file includes interfaces and functions for converting data structures/schemas
// to the appropriate parquet data structure/schema.
//
// Note that uint32 data types need to be converted to int64 due to restrictions
// from the parquet-go package. Conversion is to int64 due to the possible loss of
// data in the conversion from uint32 -> int32.
// This applies to all the ToParquet() functions in this file.

package transform

import (
	"encoding/json"
)

type SchemaParquet interface {
	ToParquet() interface{}
}

func toJSONString(v interface{}) string {
	jsonData, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(jsonData)
}

func (lo LedgerOutput) ToParquet() interface{} {
	return LedgerOutputParquet{
		// Note that uint32 data types need to be converted to int64 due to restrictions
		// from the parquet-go package. Conversion is to int64 due to the possible loss of
		// data in the conversion from uint32 -> int32.
		Sequence:                   int64(lo.Sequence),
		LedgerHash:                 lo.LedgerHash,
		PreviousLedgerHash:         lo.PreviousLedgerHash,
		LedgerHeader:               lo.LedgerHeader,
		TransactionCount:           lo.TransactionCount,
		OperationCount:             lo.OperationCount,
		SuccessfulTransactionCount: lo.SuccessfulTransactionCount,
		FailedTransactionCount:     lo.FailedTransactionCount,
		TxSetOperationCount:        lo.TxSetOperationCount,
		ClosedAt:                   lo.ClosedAt.UnixMilli(),
		TotalCoins:                 lo.TotalCoins,
		FeePool:                    lo.FeePool,
		BaseFee:                    int64(lo.BaseFee),
		BaseReserve:                int64(lo.BaseReserve),
		MaxTxSetSize:               int64(lo.MaxTxSetSize),
		ProtocolVersion:            int64(lo.ProtocolVersion),
		LedgerID:                   lo.LedgerID,
		SorobanFeeWrite1Kb:         lo.SorobanFeeWrite1Kb,
		NodeID:                     lo.NodeID,
		Signature:                  lo.Signature,
	}
}

func (to TransactionOutput) ToParquet() interface{} {
	return TransactionOutputParquet{
		TransactionHash:                      to.TransactionHash,
		LedgerSequence:                       int64(to.LedgerSequence),
		Account:                              to.Account,
		AccountMuxed:                         to.AccountMuxed,
		AccountSequence:                      to.AccountSequence,
		MaxFee:                               int64(to.MaxFee),
		FeeCharged:                           to.FeeCharged,
		OperationCount:                       to.OperationCount,
		TxEnvelope:                           to.TxEnvelope,
		TxResult:                             to.TxResult,
		TxMeta:                               to.TxMeta,
		TxFeeMeta:                            to.TxFeeMeta,
		CreatedAt:                            to.CreatedAt.UnixMilli(),
		MemoType:                             to.MemoType,
		Memo:                                 to.Memo,
		TimeBounds:                           to.TimeBounds,
		Successful:                           to.Successful,
		TransactionID:                        to.TransactionID,
		FeeAccount:                           to.FeeAccount,
		FeeAccountMuxed:                      to.FeeAccountMuxed,
		InnerTransactionHash:                 to.InnerTransactionHash,
		NewMaxFee:                            int64(to.NewMaxFee),
		LedgerBounds:                         to.LedgerBounds,
		MinAccountSequence:                   to.MinAccountSequence.Int64,
		MinAccountSequenceAge:                to.MinAccountSequenceAge.Int64,
		MinAccountSequenceLedgerGap:          to.MinAccountSequenceLedgerGap.Int64,
		ExtraSigners:                         to.ExtraSigners,
		ClosedAt:                             to.ClosedAt.UnixMilli(),
		ResourceFee:                          to.ResourceFee,
		SorobanResourcesInstructions:         int64(to.SorobanResourcesInstructions),
		SorobanResourcesReadBytes:            int64(to.SorobanResourcesReadBytes),
		SorobanResourcesWriteBytes:           int64(to.SorobanResourcesWriteBytes),
		TransactionResultCode:                to.TransactionResultCode,
		InclusionFeeBid:                      to.InclusionFeeBid,
		InclusionFeeCharged:                  to.InclusionFeeCharged,
		ResourceFeeRefund:                    to.ResourceFeeRefund,
		TotalNonRefundableResourceFeeCharged: to.TotalNonRefundableResourceFeeCharged,
		TotalRefundableResourceFeeCharged:    to.TotalRefundableResourceFeeCharged,
		RentFeeCharged:                       to.RentFeeCharged,
	}
}

func (ao AccountOutput) ToParquet() interface{} {
	return AccountOutputParquet{
		AccountID:            ao.AccountID,
		Balance:              ao.Balance,
		BuyingLiabilities:    ao.BuyingLiabilities,
		SellingLiabilities:   ao.SellingLiabilities,
		SequenceNumber:       ao.SequenceNumber,
		SequenceLedger:       ao.SequenceLedger.Int64,
		SequenceTime:         ao.SequenceTime.Int64,
		NumSubentries:        int64(ao.NumSubentries),
		InflationDestination: ao.InflationDestination,
		Flags:                int64(ao.Flags),
		HomeDomain:           ao.HomeDomain,
		MasterWeight:         ao.MasterWeight,
		ThresholdLow:         ao.ThresholdLow,
		ThresholdMedium:      ao.ThresholdMedium,
		ThresholdHigh:        ao.ThresholdHigh,
		Sponsor:              ao.Sponsor.String,
		NumSponsored:         int64(ao.NumSponsored),
		NumSponsoring:        int64(ao.NumSponsoring),
		LastModifiedLedger:   int64(ao.LastModifiedLedger),
		LedgerEntryChange:    int64(ao.LedgerEntryChange),
		Deleted:              ao.Deleted,
		ClosedAt:             ao.ClosedAt.UnixMilli(),
		LedgerSequence:       int64(ao.LedgerSequence),
	}
}

func (aso AccountSignerOutput) ToParquet() interface{} {
	return AccountSignerOutputParquet{
		AccountID:          aso.AccountID,
		Signer:             aso.Signer,
		Weight:             aso.Weight,
		Sponsor:            aso.Sponsor.String,
		LastModifiedLedger: int64(aso.LastModifiedLedger),
		LedgerEntryChange:  int64(aso.LedgerEntryChange),
		Deleted:            aso.Deleted,
		ClosedAt:           aso.ClosedAt.UnixMilli(),
		LedgerSequence:     int64(aso.LedgerSequence),
	}
}

func (oo OperationOutput) ToParquet() interface{} {
	return OperationOutputParquet{
		SourceAccount:       oo.SourceAccount,
		SourceAccountMuxed:  oo.SourceAccountMuxed,
		Type:                oo.Type,
		TypeString:          oo.TypeString,
		OperationDetails:    toJSONString(oo.OperationDetails),
		TransactionID:       oo.TransactionID,
		OperationID:         oo.OperationID,
		ClosedAt:            oo.ClosedAt.UnixMilli(),
		OperationResultCode: oo.OperationResultCode,
		OperationTraceCode:  oo.OperationTraceCode,
		LedgerSequence:      int64(oo.LedgerSequence),
	}
}

func (po PoolOutput) ToParquet() interface{} {
	return PoolOutputParquet{
		PoolID:             po.PoolID,
		PoolType:           po.PoolType,
		PoolFee:            int64(po.PoolFee),
		TrustlineCount:     int64(po.TrustlineCount),
		PoolShareCount:     po.PoolShareCount,
		AssetAType:         po.AssetAType,
		AssetACode:         po.AssetACode,
		AssetAIssuer:       po.AssetAIssuer,
		AssetAReserve:      po.AssetAReserve,
		AssetAID:           po.AssetAID,
		AssetBType:         po.AssetBType,
		AssetBCode:         po.AssetBCode,
		AssetBIssuer:       po.AssetBIssuer,
		AssetBReserve:      po.AssetBReserve,
		AssetBID:           po.AssetBID,
		LastModifiedLedger: int64(po.LastModifiedLedger),
		LedgerEntryChange:  int64(po.LedgerEntryChange),
		Deleted:            po.Deleted,
		ClosedAt:           po.ClosedAt.UnixMilli(),
		LedgerSequence:     int64(po.LedgerSequence),
	}
}

func (ao AssetOutput) ToParquet() interface{} {
	return AssetOutputParquet{
		AssetCode:      ao.AssetCode,
		AssetIssuer:    ao.AssetIssuer,
		AssetType:      ao.AssetType,
		AssetID:        ao.AssetID,
		ClosedAt:       ao.ClosedAt.UnixMilli(),
		LedgerSequence: int64(ao.LedgerSequence),
	}
}

func (to TrustlineOutput) ToParquet() interface{} {
	return TrustlineOutputParquet{
		LedgerKey:          to.LedgerKey,
		AccountID:          to.AccountID,
		AssetCode:          to.AssetCode,
		AssetIssuer:        to.AssetIssuer,
		AssetType:          to.AssetType,
		AssetID:            to.AssetID,
		Balance:            to.Balance,
		TrustlineLimit:     to.TrustlineLimit,
		LiquidityPoolID:    to.LiquidityPoolID,
		BuyingLiabilities:  to.BuyingLiabilities,
		SellingLiabilities: to.SellingLiabilities,
		Flags:              int64(to.Flags),
		LastModifiedLedger: int64(to.LastModifiedLedger),
		LedgerEntryChange:  int64(to.LedgerEntryChange),
		Sponsor:            to.Sponsor.String,
		Deleted:            to.Deleted,
		ClosedAt:           to.ClosedAt.UnixMilli(),
		LedgerSequence:     int64(to.LedgerSequence),
	}
}

func (oo OfferOutput) ToParquet() interface{} {
	return OfferOutputParquet{
		SellerID:           oo.SellerID,
		OfferID:            oo.OfferID,
		SellingAssetType:   oo.SellingAssetType,
		SellingAssetCode:   oo.SellingAssetCode,
		SellingAssetIssuer: oo.SellingAssetIssuer,
		SellingAssetID:     oo.SellingAssetID,
		BuyingAssetType:    oo.BuyingAssetType,
		BuyingAssetCode:    oo.BuyingAssetCode,
		BuyingAssetIssuer:  oo.BuyingAssetIssuer,
		BuyingAssetID:      oo.BuyingAssetID,
		Amount:             oo.Amount,
		PriceN:             oo.PriceN,
		PriceD:             oo.PriceD,
		Price:              oo.Price,
		Flags:              int64(oo.Flags),
		LastModifiedLedger: int64(oo.LastModifiedLedger),
		LedgerEntryChange:  int64(oo.LedgerEntryChange),
		Deleted:            oo.Deleted,
		Sponsor:            oo.Sponsor.String,
		ClosedAt:           oo.ClosedAt.UnixMilli(),
		LedgerSequence:     int64(oo.LedgerSequence),
	}
}

func (to TradeOutput) ToParquet() interface{} {
	return TradeOutputParquet{
		Order:                  to.Order,
		LedgerClosedAt:         to.LedgerClosedAt.UnixMilli(),
		SellingAccountAddress:  to.SellingAccountAddress,
		SellingAssetCode:       to.SellingAssetCode,
		SellingAssetIssuer:     to.SellingAssetIssuer,
		SellingAssetType:       to.SellingAssetType,
		SellingAssetID:         to.SellingAssetID,
		SellingAmount:          to.SellingAmount,
		BuyingAccountAddress:   to.BuyingAccountAddress,
		BuyingAssetCode:        to.BuyingAssetCode,
		BuyingAssetIssuer:      to.BuyingAssetIssuer,
		BuyingAssetType:        to.BuyingAssetType,
		BuyingAssetID:          to.BuyingAssetID,
		BuyingAmount:           to.BuyingAmount,
		PriceN:                 to.PriceN,
		PriceD:                 to.PriceD,
		SellingOfferID:         to.SellingOfferID.Int64,
		BuyingOfferID:          to.BuyingOfferID.Int64,
		SellingLiquidityPoolID: to.SellingLiquidityPoolID.String,
		LiquidityPoolFee:       to.LiquidityPoolFee.Int64,
		HistoryOperationID:     to.HistoryOperationID,
		TradeType:              to.TradeType,
		RoundingSlippage:       to.RoundingSlippage.Int64,
		SellerIsExact:          to.SellerIsExact.Bool,
	}
}

func (eo EffectOutput) ToParquet() interface{} {
	return EffectOutputParquet{
		Address:        eo.Address,
		AddressMuxed:   eo.AddressMuxed.String,
		OperationID:    eo.OperationID,
		Details:        toJSONString(eo.Details),
		Type:           eo.Type,
		TypeString:     eo.TypeString,
		LedgerClosed:   eo.LedgerClosed.UnixMilli(),
		LedgerSequence: int64(eo.LedgerSequence),
		EffectIndex:    int64(eo.EffectIndex),
		EffectId:       eo.EffectId,
	}
}

func (cdo ContractDataOutput) ToParquet() interface{} {
	return ContractDataOutputParquet{
		ContractId:                cdo.ContractId,
		ContractKeyType:           cdo.ContractKeyType,
		ContractDurability:        cdo.ContractDurability,
		ContractDataAssetCode:     cdo.ContractDataAssetCode,
		ContractDataAssetIssuer:   cdo.ContractDataAssetIssuer,
		ContractDataAssetType:     cdo.ContractDataAssetType,
		ContractDataBalanceHolder: cdo.ContractDataBalanceHolder,
		ContractDataBalance:       cdo.ContractDataBalance,
		LastModifiedLedger:        int64(cdo.LastModifiedLedger),
		LedgerEntryChange:         int64(cdo.LedgerEntryChange),
		Deleted:                   cdo.Deleted,
		ClosedAt:                  cdo.ClosedAt.UnixMilli(),
		LedgerSequence:            int64(cdo.LedgerSequence),
		LedgerKeyHash:             cdo.LedgerKeyHash,
		Key:                       cdo.Key,
		KeyDecoded:                cdo.KeyDecoded,
		Val:                       cdo.Val,
		ValDecoded:                cdo.ValDecoded,
		ContractDataXDR:           cdo.ContractDataXDR,
	}
}

func (cco ContractCodeOutput) ToParquet() interface{} {
	return ContractCodeOutputParquet{
		ContractCodeHash:   cco.ContractCodeHash,
		ContractCodeExtV:   cco.ContractCodeExtV,
		LastModifiedLedger: int64(cco.LastModifiedLedger),
		LedgerEntryChange:  int64(cco.LedgerEntryChange),
		Deleted:            cco.Deleted,
		ClosedAt:           cco.ClosedAt.UnixMilli(),
		LedgerSequence:     int64(cco.LedgerSequence),
		LedgerKeyHash:      cco.LedgerKeyHash,
		NInstructions:      int64(cco.NInstructions),
		NFunctions:         int64(cco.NFunctions),
		NGlobals:           int64(cco.NGlobals),
		NTableEntries:      int64(cco.NTableEntries),
		NTypes:             int64(cco.NTypes),
		NDataSegments:      int64(cco.NDataSegments),
		NElemSegments:      int64(cco.NElemSegments),
		NImports:           int64(cco.NImports),
		NExports:           int64(cco.NExports),
		NDataSegmentBytes:  int64(cco.NDataSegmentBytes),
	}
}

func (cso ConfigSettingOutput) ToParquet() interface{} {
	// Convert []uint64 to []int64
	BucketListSizeWindowInt := make([]int64, len(cso.BucketListSizeWindow))
	for i, v := range cso.BucketListSizeWindow {
		// This can cause a drop in precision of data
		BucketListSizeWindowInt[i] = int64(v)
	}

	return ConfigSettingOutputParquet{
		ConfigSettingId:                 cso.ConfigSettingId,
		ContractMaxSizeBytes:            int64(cso.ContractMaxSizeBytes),
		LedgerMaxInstructions:           cso.LedgerMaxInstructions,
		TxMaxInstructions:               cso.TxMaxInstructions,
		FeeRatePerInstructionsIncrement: cso.FeeRatePerInstructionsIncrement,
		TxMemoryLimit:                   int64(cso.TxMemoryLimit),
		LedgerMaxReadLedgerEntries:      int64(cso.LedgerMaxReadLedgerEntries),
		LedgerMaxReadBytes:              int64(cso.LedgerMaxReadBytes),
		LedgerMaxWriteLedgerEntries:     int64(cso.LedgerMaxWriteLedgerEntries),
		LedgerMaxWriteBytes:             int64(cso.LedgerMaxWriteBytes),
		TxMaxReadLedgerEntries:          int64(cso.TxMaxReadLedgerEntries),
		TxMaxReadBytes:                  int64(cso.TxMaxReadBytes),
		TxMaxWriteLedgerEntries:         int64(cso.TxMaxWriteLedgerEntries),
		TxMaxWriteBytes:                 int64(cso.TxMaxWriteBytes),
		FeeReadLedgerEntry:              cso.FeeReadLedgerEntry,
		FeeWriteLedgerEntry:             cso.FeeWriteLedgerEntry,
		FeeRead1Kb:                      cso.FeeRead1Kb,
		BucketListTargetSizeBytes:       cso.BucketListTargetSizeBytes,
		WriteFee1KbBucketListLow:        cso.WriteFee1KbBucketListLow,
		WriteFee1KbBucketListHigh:       cso.WriteFee1KbBucketListHigh,
		BucketListWriteFeeGrowthFactor:  int64(cso.BucketListWriteFeeGrowthFactor),
		FeeHistorical1Kb:                cso.FeeHistorical1Kb,
		TxMaxContractEventsSizeBytes:    int64(cso.TxMaxContractEventsSizeBytes),
		FeeContractEvents1Kb:            cso.FeeContractEvents1Kb,
		LedgerMaxTxsSizeBytes:           int64(cso.LedgerMaxTxsSizeBytes),
		TxMaxSizeBytes:                  int64(cso.TxMaxSizeBytes),
		FeeTxSize1Kb:                    cso.FeeTxSize1Kb,
		ContractCostParamsCpuInsns:      toJSONString(cso.ContractCostParamsCpuInsns),
		ContractCostParamsMemBytes:      toJSONString(cso.ContractCostParamsMemBytes),
		ContractDataKeySizeBytes:        int64(cso.ContractDataKeySizeBytes),
		ContractDataEntrySizeBytes:      int64(cso.ContractDataEntrySizeBytes),
		MaxEntryTtl:                     int64(cso.MaxEntryTtl),
		MinTemporaryTtl:                 int64(cso.MinTemporaryTtl),
		MinPersistentTtl:                int64(cso.MinPersistentTtl),
		AutoBumpLedgers:                 int64(cso.AutoBumpLedgers),
		PersistentRentRateDenominator:   cso.PersistentRentRateDenominator,
		TempRentRateDenominator:         cso.TempRentRateDenominator,
		MaxEntriesToArchive:             int64(cso.MaxEntriesToArchive),
		BucketListSizeWindowSampleSize:  int64(cso.BucketListSizeWindowSampleSize),
		EvictionScanSize:                int64(cso.EvictionScanSize),
		StartingEvictionScanLevel:       int64(cso.StartingEvictionScanLevel),
		LedgerMaxTxCount:                int64(cso.LedgerMaxTxCount),
		BucketListSizeWindow:            BucketListSizeWindowInt,
		LastModifiedLedger:              int64(cso.LastModifiedLedger),
		LedgerEntryChange:               int64(cso.LedgerEntryChange),
		Deleted:                         cso.Deleted,
		ClosedAt:                        cso.ClosedAt.UnixMilli(),
		LedgerSequence:                  int64(cso.LedgerSequence),
	}
}

func (to TtlOutput) ToParquet() interface{} {
	return TtlOutputParquet{
		KeyHash:            to.KeyHash,
		LiveUntilLedgerSeq: int64(to.LiveUntilLedgerSeq),
		LastModifiedLedger: int64(to.LastModifiedLedger),
		LedgerEntryChange:  int64(to.LedgerEntryChange),
		Deleted:            to.Deleted,
		ClosedAt:           to.ClosedAt.UnixMilli(),
		LedgerSequence:     int64(to.LedgerSequence),
	}
}

func (ceo ContractEventOutput) ToParquet() interface{} {
	return ContractEventOutputParquet{
		TransactionHash:          ceo.TransactionHash,
		TransactionID:            ceo.TransactionID,
		Successful:               ceo.Successful,
		LedgerSequence:           int64(ceo.LedgerSequence),
		ClosedAt:                 ceo.ClosedAt.UnixMilli(),
		InSuccessfulContractCall: ceo.InSuccessfulContractCall,
		ContractId:               ceo.ContractId,
		Type:                     ceo.Type,
		TypeString:               ceo.TypeString,
		Topics:                   toJSONString(ceo.Topics),
		TopicsDecoded:            toJSONString(ceo.TopicsDecoded),
		Data:                     toJSONString(ceo.Data),
		DataDecoded:              toJSONString(ceo.DataDecoded),
		ContractEventXDR:         ceo.ContractEventXDR,
	}
}
