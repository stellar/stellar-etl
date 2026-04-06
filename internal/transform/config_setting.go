package transform

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	frozenLedgerKeysJSON := serializeEncodedLedgerKeys(frozenLedgerKeys.Keys)

	frozenLedgerKeysDelta, _ := configSetting.GetFrozenLedgerKeysDelta()
	frozenLedgerKeysDeltaJSON := serializeFrozenKeysDelta(frozenLedgerKeysDelta)

	freezeBypassTxs, _ := configSetting.GetFreezeBypassTxs()
	freezeBypassTxsJSON := serializeHashes(freezeBypassTxs.TxHashes)

	freezeBypassTxsDelta, _ := configSetting.GetFreezeBypassTxsDelta()
	freezeBypassTxsDeltaJSON := serializeFreezeBypassTxsDelta(freezeBypassTxsDelta)

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
		FrozenLedgerKeys:                       frozenLedgerKeysJSON,
		FrozenLedgerKeysDelta:                  frozenLedgerKeysDeltaJSON,
		FreezeBypassTxs:                        freezeBypassTxsJSON,
		FreezeBypassTxsDelta:                   freezeBypassTxsDeltaJSON,
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

// serializeEncodedLedgerKeys converts a slice of EncodedLedgerKey (opaque bytes) to a JSON array of base64 strings.
func serializeEncodedLedgerKeys(keys []xdr.EncodedLedgerKey) string {
	if len(keys) == 0 {
		return ""
	}
	encoded := make([]string, 0, len(keys))
	for _, key := range keys {
		encoded = append(encoded, base64.StdEncoding.EncodeToString(key))
	}
	result, _ := json.Marshal(encoded)
	return string(result)
}

// serializeFrozenKeysDelta converts a FrozenLedgerKeysDelta to a JSON object with keysToFreeze and keysToUnfreeze arrays.
func serializeFrozenKeysDelta(delta xdr.FrozenLedgerKeysDelta) string {
	if len(delta.KeysToFreeze) == 0 && len(delta.KeysToUnfreeze) == 0 {
		return ""
	}
	freeze := make([]string, 0, len(delta.KeysToFreeze))
	for _, key := range delta.KeysToFreeze {
		freeze = append(freeze, base64.StdEncoding.EncodeToString(key))
	}
	unfreeze := make([]string, 0, len(delta.KeysToUnfreeze))
	for _, key := range delta.KeysToUnfreeze {
		unfreeze = append(unfreeze, base64.StdEncoding.EncodeToString(key))
	}
	result, _ := json.Marshal(map[string][]string{
		"keys_to_freeze":   freeze,
		"keys_to_unfreeze": unfreeze,
	})
	return string(result)
}

// serializeHashes converts a slice of Hash ([32]byte) to a JSON array of hex strings.
func serializeHashes(hashes []xdr.Hash) string {
	if len(hashes) == 0 {
		return ""
	}
	encoded := make([]string, 0, len(hashes))
	for _, h := range hashes {
		encoded = append(encoded, hex.EncodeToString(h[:]))
	}
	result, _ := json.Marshal(encoded)
	return string(result)
}

// serializeFreezeBypassTxsDelta converts a FreezeBypassTxsDelta to a JSON object with addTxs and removeTxs arrays.
func serializeFreezeBypassTxsDelta(delta xdr.FreezeBypassTxsDelta) string {
	if len(delta.AddTxs) == 0 && len(delta.RemoveTxs) == 0 {
		return ""
	}
	addTxs := make([]string, 0, len(delta.AddTxs))
	for _, h := range delta.AddTxs {
		addTxs = append(addTxs, hex.EncodeToString(h[:]))
	}
	removeTxs := make([]string, 0, len(delta.RemoveTxs))
	for _, h := range delta.RemoveTxs {
		removeTxs = append(removeTxs, hex.EncodeToString(h[:]))
	}
	result, _ := json.Marshal(map[string][]string{
		"add_txs":    addTxs,
		"remove_txs": removeTxs,
	})
	return string(result)
}
