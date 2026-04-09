package transform

import (
	"fmt"
	"strconv"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
	"github.com/stellar/stellar-etl/v2/internal/utils"
)

// TransformConfigSetting converts an config setting ledger change entry into a form suitable for BigQuery
func TransformConfigSetting(ledgerChange ingest.Change, header xdr.LedgerHeaderHistoryEntry) (ConfigSettingOutput, error) {
	ledgerEntry, changeType, outputDeleted, err := utils.ExtractEntryFromChange(ledgerChange)
	if err != nil {
		return ConfigSettingOutput{}, err
	}

	configSetting, ok := ledgerEntry.Data.GetConfigSetting()
	if !ok {
		return ConfigSettingOutput{}, fmt.Errorf("could not extract config setting from ledger entry; actual type is %s", ledgerEntry.Data.Type)
	}

	configSettingId := configSetting.ConfigSettingId

	contractMaxSizeBytes, _ := configSetting.GetContractMaxSizeBytes()

	contractCompute, _ := configSetting.GetContractCompute()
	ledgerMaxInstructions := contractCompute.LedgerMaxInstructions
	txMaxInstructions := contractCompute.TxMaxInstructions
	feeRatePerInstructionsIncrement := contractCompute.FeeRatePerInstructionsIncrement
	txMemoryLimit := contractCompute.TxMemoryLimit

	contractLedgerCost, _ := configSetting.GetContractLedgerCost()
	contractLedgerCostV0, _ := configSetting.GetContractLedgerCostExt()
	ledgerMaxReadLedgerEntries := contractLedgerCost.LedgerMaxDiskReadEntries
	ledgerMaxDiskReadEntries := contractLedgerCost.LedgerMaxDiskReadEntries
	ledgerMaxReadBytes := contractLedgerCost.LedgerMaxDiskReadBytes
	ledgerMaxDiskReadBytes := contractLedgerCost.LedgerMaxDiskReadBytes
	ledgerMaxWriteLedgerEntries := contractLedgerCost.LedgerMaxWriteLedgerEntries
	ledgerMaxWriteBytes := contractLedgerCost.LedgerMaxWriteBytes
	txMaxReadLedgerEntries := contractLedgerCost.TxMaxDiskReadEntries
	txMaxDiskReadEntries := contractLedgerCost.TxMaxDiskReadEntries
	txMaxReadBytes := contractLedgerCost.TxMaxDiskReadBytes
	txMaxDiskReadBytes := contractLedgerCost.TxMaxDiskReadBytes
	txMaxWriteLedgerEntries := contractLedgerCost.TxMaxWriteLedgerEntries
	txMaxWriteBytes := contractLedgerCost.TxMaxWriteBytes
	feeReadLedgerEntry := contractLedgerCost.FeeDiskReadLedgerEntry
	feeDiskReadLedgerEntry := contractLedgerCost.FeeDiskReadLedgerEntry
	feeWriteLedgerEntry := contractLedgerCost.FeeWriteLedgerEntry
	feeRead1Kb := contractLedgerCost.FeeDiskRead1Kb
	feeWrite1Kb := contractLedgerCostV0.FeeWrite1Kb
	feeDiskRead1Kb := contractLedgerCost.FeeDiskRead1Kb
	bucketListTargetSizeBytes := contractLedgerCost.SorobanStateTargetSizeBytes
	sorobanStateTargetSizeBytes := contractLedgerCost.SorobanStateTargetSizeBytes
	writeFee1KbBucketListLow := contractLedgerCost.RentFee1KbSorobanStateSizeLow
	rentFee1KBSorobanStateSizeLow := contractLedgerCost.RentFee1KbSorobanStateSizeLow
	writeFee1KbBucketListHigh := contractLedgerCost.RentFee1KbSorobanStateSizeHigh
	rentFee1KBSorobanStateSizeHigh := contractLedgerCost.RentFee1KbSorobanStateSizeHigh
	bucketListWriteFeeGrowthFactor := contractLedgerCost.SorobanStateRentFeeGrowthFactor
	sorobanStateRentFeeGrowthFactor := contractLedgerCost.SorobanStateRentFeeGrowthFactor

	contractHistoricalData, _ := configSetting.GetContractHistoricalData()
	feeHistorical1Kb := contractHistoricalData.FeeHistorical1Kb

	contractMetaData, _ := configSetting.GetContractEvents()
	txMaxContractEventsSizeBytes := contractMetaData.TxMaxContractEventsSizeBytes
	feeContractEvents1Kb := contractMetaData.FeeContractEvents1Kb

	contractBandwidth, _ := configSetting.GetContractBandwidth()
	ledgerMaxTxsSizeBytes := contractBandwidth.LedgerMaxTxsSizeBytes
	txMaxSizeBytes := contractBandwidth.TxMaxSizeBytes
	feeTxSize1Kb := contractBandwidth.FeeTxSize1Kb

	paramsCpuInsns, _ := configSetting.GetContractCostParamsCpuInsns()
	contractCostParamsCpuInsns := serializeParams(paramsCpuInsns)

	paramsMemBytes, _ := configSetting.GetContractCostParamsMemBytes()
	contractCostParamsMemBytes := serializeParams(paramsMemBytes)

	contractDataKeySizeBytes, _ := configSetting.GetContractDataKeySizeBytes()

	contractDataEntrySizeBytes, _ := configSetting.GetContractDataEntrySizeBytes()

	stateArchivalSettings, _ := configSetting.GetStateArchivalSettings()
	maxEntryTtl := stateArchivalSettings.MaxEntryTtl
	minTemporaryTtl := stateArchivalSettings.MinTemporaryTtl
	minPersistentTtl := stateArchivalSettings.MinPersistentTtl
	persistentRentRateDenominator := stateArchivalSettings.PersistentRentRateDenominator
	tempRentRateDenominator := stateArchivalSettings.TempRentRateDenominator
	maxEntriesToArchive := stateArchivalSettings.MaxEntriesToArchive
	bucketListSizeWindowSampleSize := stateArchivalSettings.LiveSorobanStateSizeWindowSampleSize
	liveSorobanStateSizeWindowSampleSize := stateArchivalSettings.LiveSorobanStateSizeWindowSampleSize
	liveSorobanStateSizeWindowSamplePeriod := stateArchivalSettings.LiveSorobanStateSizeWindowSamplePeriod
	evictionScanSize := stateArchivalSettings.EvictionScanSize
	startingEvictionScanLevel := stateArchivalSettings.StartingEvictionScanLevel

	contractExecutionLanes, _ := configSetting.GetContractExecutionLanes()
	ledgerMaxTxCount := contractExecutionLanes.LedgerMaxTxCount

	bucketList, _ := configSetting.GetLiveSorobanStateSizeWindow()
	bucketListSizeWindow := make([]uint64, 0, len(bucketList))
	liveSorobanStateSizeWindow := make([]uint64, 0, len(bucketList))
	for _, sizeWindow := range bucketList {
		bucketListSizeWindow = append(bucketListSizeWindow, uint64(sizeWindow))
		liveSorobanStateSizeWindow = append(liveSorobanStateSizeWindow, uint64(sizeWindow))
	}

	// P23: ContractParallelCompute (ID=14)
	contractParallelCompute, _ := configSetting.GetContractParallelCompute()
	ledgerMaxDependentTxClusters := contractParallelCompute.LedgerMaxDependentTxClusters

	// P23: ContractLedgerCostExt (ID=15) - TxMaxFootprintEntries
	txMaxFootprintEntries := contractLedgerCostV0.TxMaxFootprintEntries

	// P23: ContractScpTiming (ID=16)
	contractScpTiming, _ := configSetting.GetContractScpTiming()
	ledgerTargetCloseTimeMilliseconds := contractScpTiming.LedgerTargetCloseTimeMilliseconds
	nominationTimeoutInitialMilliseconds := contractScpTiming.NominationTimeoutInitialMilliseconds
	nominationTimeoutIncrementMilliseconds := contractScpTiming.NominationTimeoutIncrementMilliseconds
	ballotTimeoutInitialMilliseconds := contractScpTiming.BallotTimeoutInitialMilliseconds
	ballotTimeoutIncrementMilliseconds := contractScpTiming.BallotTimeoutIncrementMilliseconds

	// P26 CAP-77: Frozen ledger keys (IDs 17-20)
	frozenLedgerKeys, _ := configSetting.GetFrozenLedgerKeys()
	frozenLedgerKeysBase64, err := marshalEncodedLedgerKeys(frozenLedgerKeys.Keys)
	if err != nil {
		return ConfigSettingOutput{}, err
	}

	frozenLedgerKeysDelta, _ := configSetting.GetFrozenLedgerKeysDelta()
	frozenLedgerKeysToFreeze, err := marshalEncodedLedgerKeys(frozenLedgerKeysDelta.KeysToFreeze)
	if err != nil {
		return ConfigSettingOutput{}, err
	}
	frozenLedgerKeysToUnfreeze, err := marshalEncodedLedgerKeys(frozenLedgerKeysDelta.KeysToUnfreeze)
	if err != nil {
		return ConfigSettingOutput{}, err
	}

	freezeBypassTxs, _ := configSetting.GetFreezeBypassTxs()
	freezeBypassTxHashes := hashesToHexStrings(freezeBypassTxs.TxHashes)

	freezeBypassTxsDelta, _ := configSetting.GetFreezeBypassTxsDelta()
	freezeBypassTxsToAdd := hashesToHexStrings(freezeBypassTxsDelta.AddTxs)
	freezeBypassTxsToRemove := hashesToHexStrings(freezeBypassTxsDelta.RemoveTxs)

	closedAt, err := utils.TimePointToUTCTimeStamp(header.Header.ScpValue.CloseTime)
	if err != nil {
		return ConfigSettingOutput{}, err
	}

	ledgerSequence := header.Header.LedgerSeq

	transformedConfigSetting := ConfigSettingOutput{
		ConfigSettingId:                        int32(configSettingId),
		ContractMaxSizeBytes:                   uint32(contractMaxSizeBytes),
		LedgerMaxInstructions:                  int64(ledgerMaxInstructions),
		TxMaxInstructions:                      int64(txMaxInstructions),
		FeeRatePerInstructionsIncrement:        int64(feeRatePerInstructionsIncrement),
		TxMemoryLimit:                          uint32(txMemoryLimit),
		LedgerMaxReadLedgerEntries:             uint32(ledgerMaxReadLedgerEntries),
		LedgerMaxDiskReadEntries:               uint32(ledgerMaxDiskReadEntries),
		LedgerMaxReadBytes:                     uint32(ledgerMaxReadBytes),
		LedgerMaxDiskReadBytes:                 uint32(ledgerMaxDiskReadBytes),
		LedgerMaxWriteLedgerEntries:            uint32(ledgerMaxWriteLedgerEntries),
		LedgerMaxWriteBytes:                    uint32(ledgerMaxWriteBytes),
		TxMaxReadLedgerEntries:                 uint32(txMaxReadLedgerEntries),
		TxMaxDiskReadEntries:                   uint32(txMaxDiskReadEntries),
		TxMaxReadBytes:                         uint32(txMaxReadBytes),
		TxMaxDiskReadBytes:                     uint32(txMaxDiskReadBytes),
		TxMaxWriteLedgerEntries:                uint32(txMaxWriteLedgerEntries),
		TxMaxWriteBytes:                        uint32(txMaxWriteBytes),
		FeeReadLedgerEntry:                     int64(feeReadLedgerEntry),
		FeeDiskReadLedgerEntry:                 int64(feeDiskReadLedgerEntry),
		FeeWriteLedgerEntry:                    int64(feeWriteLedgerEntry),
		FeeRead1Kb:                             int64(feeRead1Kb),
		FeeWrite1Kb:                            int64(feeWrite1Kb),
		FeeDiskRead1Kb:                         int64(feeDiskRead1Kb),
		BucketListTargetSizeBytes:              int64(bucketListTargetSizeBytes),
		SorobanStateTargetSizeBytes:            int64(sorobanStateTargetSizeBytes),
		WriteFee1KbBucketListLow:               int64(writeFee1KbBucketListLow),
		RentFee1KBSorobanStateSizeLow:          int64(rentFee1KBSorobanStateSizeLow),
		WriteFee1KbBucketListHigh:              int64(writeFee1KbBucketListHigh),
		RentFee1KBSorobanStateSizeHigh:         int64(rentFee1KBSorobanStateSizeHigh),
		BucketListWriteFeeGrowthFactor:         uint32(bucketListWriteFeeGrowthFactor),
		SorobanStateRentFeeGrowthFactor:        uint32(sorobanStateRentFeeGrowthFactor),
		FeeHistorical1Kb:                       int64(feeHistorical1Kb),
		TxMaxContractEventsSizeBytes:           uint32(txMaxContractEventsSizeBytes),
		FeeContractEvents1Kb:                   int64(feeContractEvents1Kb),
		LedgerMaxTxsSizeBytes:                  uint32(ledgerMaxTxsSizeBytes),
		TxMaxSizeBytes:                         uint32(txMaxSizeBytes),
		FeeTxSize1Kb:                           int64(feeTxSize1Kb),
		ContractCostParamsCpuInsns:             contractCostParamsCpuInsns,
		ContractCostParamsMemBytes:             contractCostParamsMemBytes,
		ContractDataKeySizeBytes:               uint32(contractDataKeySizeBytes),
		ContractDataEntrySizeBytes:             uint32(contractDataEntrySizeBytes),
		MaxEntryTtl:                            uint32(maxEntryTtl),
		MinTemporaryTtl:                        uint32(minTemporaryTtl),
		MinPersistentTtl:                       uint32(minPersistentTtl),
		PersistentRentRateDenominator:          int64(persistentRentRateDenominator),
		TempRentRateDenominator:                int64(tempRentRateDenominator),
		MaxEntriesToArchive:                    uint32(maxEntriesToArchive),
		BucketListSizeWindowSampleSize:         uint32(bucketListSizeWindowSampleSize),
		LiveSorobanStateSizeWindowSampleSize:   uint32(liveSorobanStateSizeWindowSampleSize),
		LiveSorobanStateSizeWindowSamplePeriod: uint32(liveSorobanStateSizeWindowSamplePeriod),
		EvictionScanSize:                       uint64(evictionScanSize),
		StartingEvictionScanLevel:              uint32(startingEvictionScanLevel),
		LedgerMaxTxCount:                       uint32(ledgerMaxTxCount),
		BucketListSizeWindow:                   bucketListSizeWindow,
		LiveSorobanStateSizeWindow:             liveSorobanStateSizeWindow,
		// P23 config settings
		LedgerMaxDependentTxClusters:           uint32(ledgerMaxDependentTxClusters),
		TxMaxFootprintEntries:                  uint32(txMaxFootprintEntries),
		LedgerTargetCloseTimeMilliseconds:      uint32(ledgerTargetCloseTimeMilliseconds),
		NominationTimeoutInitialMilliseconds:   uint32(nominationTimeoutInitialMilliseconds),
		NominationTimeoutIncrementMilliseconds: uint32(nominationTimeoutIncrementMilliseconds),
		BallotTimeoutInitialMilliseconds:       uint32(ballotTimeoutInitialMilliseconds),
		BallotTimeoutIncrementMilliseconds:     uint32(ballotTimeoutIncrementMilliseconds),
		// P26 CAP-77 frozen ledger keys
		FrozenLedgerKeys:                       frozenLedgerKeysBase64,
		FrozenLedgerKeysToFreeze:               frozenLedgerKeysToFreeze,
		FrozenLedgerKeysToUnfreeze:             frozenLedgerKeysToUnfreeze,
		FreezeBypassTxs:                        freezeBypassTxHashes,
		FreezeBypassTxsToAdd:                   freezeBypassTxsToAdd,
		FreezeBypassTxsToRemove:                freezeBypassTxsToRemove,
		LastModifiedLedger:                     uint32(ledgerEntry.LastModifiedLedgerSeq),
		LedgerEntryChange:                      uint32(changeType),
		Deleted:                                outputDeleted,
		ClosedAt:                               closedAt,
		LedgerSequence:                         uint32(ledgerSequence),
	}
	return transformedConfigSetting, nil
}

func serializeParams(costParams xdr.ContractCostParams) []map[string]string {
	params := make([]map[string]string, 0, len(costParams))
	for _, contractCostParam := range costParams {
		serializedParam := map[string]string{}
		serializedParam["ExtV"] = strconv.Itoa(int(contractCostParam.Ext.V))
		serializedParam["ConstTerm"] = strconv.Itoa(int(contractCostParam.ConstTerm))
		serializedParam["LinearTerm"] = strconv.Itoa(int(contractCostParam.LinearTerm))
		params = append(params, serializedParam)
	}

	return params
}

// marshalEncodedLedgerKeys converts a slice of EncodedLedgerKey (XDR-encoded opaque bytes)
// to a slice of base64 strings using xdr.MarshalBase64, matching the format used
// by transformLedgerKeys in ledger.go.
func marshalEncodedLedgerKeys(keys []xdr.EncodedLedgerKey) ([]string, error) {
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		var ledgerKey xdr.LedgerKey
		if err := xdr.SafeUnmarshal(key, &ledgerKey); err != nil {
			return nil, fmt.Errorf("could not unmarshal encoded ledger key: %v", err)
		}
		b64, err := xdr.MarshalBase64(ledgerKey)
		if err != nil {
			return nil, fmt.Errorf("could not marshal ledger key to base64: %v", err)
		}
		result = append(result, b64)
	}
	return result, nil
}

// hashesToHexStrings converts a slice of xdr.Hash to a slice of hex-encoded strings,
// matching the format used by utils.HashToHexString for transaction hashes.
func hashesToHexStrings(hashes []xdr.Hash) []string {
	result := make([]string, 0, len(hashes))
	for _, h := range hashes {
		result = append(result, utils.HashToHexString(h))
	}
	return result
}
