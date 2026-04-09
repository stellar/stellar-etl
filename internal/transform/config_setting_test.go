package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTransformConfigSetting(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput ConfigSettingOutput
		wantErr    error
	}

	hardCodedInput := makeConfigSettingTestInput()
	hardCodedOutput := makeConfigSettingTestOutput()
	tests := []transformTest{
		{
			ingest.Change{
				ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
				Type:       xdr.LedgerEntryTypeOffer,
				Pre:        nil,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeOffer,
					},
				},
			},
			ConfigSettingOutput{}, fmt.Errorf("could not extract config setting from ledger entry; actual type is LedgerEntryTypeOffer"),
		},
	}

	for i := range hardCodedInput {
		tests = append(tests, transformTest{
			input:      hardCodedInput[i],
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		header := xdr.LedgerHeaderHistoryEntry{
			Header: xdr.LedgerHeader{
				ScpValue: xdr.StellarValue{
					CloseTime: 1000,
				},
				LedgerSeq: 10,
			},
		}
		actualOutput, actualError := TransformConfigSetting(test.input, header)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeConfigSettingTestInput() []ingest.Change {
	var contractMaxByte xdr.Uint32 = 0

	contractDataLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:      xdr.ConfigSettingIdConfigSettingContractMaxSizeBytes,
				ContractMaxSizeBytes: &contractMaxByte,
			},
		},
	}

	// P23: ContractParallelCompute (ID=14)
	parallelCompute := xdr.ConfigSettingContractParallelComputeV0{
		LedgerMaxDependentTxClusters: 5,
	}
	parallelComputeEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229504,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:         xdr.ConfigSettingIdConfigSettingContractParallelComputeV0,
				ContractParallelCompute: &parallelCompute,
			},
		},
	}

	// P23: ContractLedgerCostExt (ID=15)
	ledgerCostExt := xdr.ConfigSettingContractLedgerCostExtV0{
		TxMaxFootprintEntries: 100,
		FeeWrite1Kb:           2000,
	}
	ledgerCostExtEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229505,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:     xdr.ConfigSettingIdConfigSettingContractLedgerCostExtV0,
				ContractLedgerCostExt: &ledgerCostExt,
			},
		},
	}

	// P23: ContractScpTiming (ID=16)
	scpTiming := xdr.ConfigSettingScpTiming{
		LedgerTargetCloseTimeMilliseconds:      5000,
		NominationTimeoutInitialMilliseconds:   1000,
		NominationTimeoutIncrementMilliseconds: 500,
		BallotTimeoutInitialMilliseconds:       1000,
		BallotTimeoutIncrementMilliseconds:     1000,
	}
	scpTimingEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229506,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId:   xdr.ConfigSettingIdConfigSettingScpTiming,
				ContractScpTiming: &scpTiming,
			},
		},
	}

	// P26 CAP-77: FrozenLedgerKeys (ID=17)
	// Create valid XDR-encoded LedgerKey bytes for testing
	testLedgerKey1 := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: xdr.MustAddress("GAAZI4TCR3TY5OJHCTJC2A4QSY6CJWJH5IAJTGKIN2ER7LBNVKOCCWN7"),
		},
	}
	testLedgerKey2 := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: xdr.MustAddress("GCO2IP3MJNUOKS4PUDI4C7LGGMQDJGXG3COYX3WSB4HHNAHKYV5YL3VC"),
		},
	}
	encodedKey1, _ := testLedgerKey1.MarshalBinary()
	encodedKey2, _ := testLedgerKey2.MarshalBinary()
	frozenKeys := xdr.FrozenLedgerKeys{
		Keys: []xdr.EncodedLedgerKey{
			xdr.EncodedLedgerKey(encodedKey1),
			xdr.EncodedLedgerKey(encodedKey2),
		},
	}
	frozenKeysEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229507,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingFrozenLedgerKeys,
				FrozenLedgerKeys: &frozenKeys,
			},
		},
	}

	// P26 CAP-77: FreezeBypassTxs (ID=19)
	var txHash1, txHash2 xdr.Hash
	copy(txHash1[:], []byte("aaaabbbbccccddddeeeeffffgggghhhh"))
	copy(txHash2[:], []byte("11112222333344445555666677778888"))
	bypassTxs := xdr.FreezeBypassTxs{
		TxHashes: []xdr.Hash{txHash1, txHash2},
	}
	bypassTxsEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229508,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeConfigSetting,
			ConfigSetting: &xdr.ConfigSettingEntry{
				ConfigSettingId: xdr.ConfigSettingIdConfigSettingFreezeBypassTxs,
				FreezeBypassTxs: &bypassTxs,
			},
		},
	}

	return []ingest.Change{
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
			Type:       xdr.LedgerEntryTypeConfigSetting,
			Pre:        &xdr.LedgerEntry{},
			Post:       &contractDataLedgerEntry,
		},
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Type:       xdr.LedgerEntryTypeConfigSetting,
			Pre:        nil,
			Post:       &parallelComputeEntry,
		},
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Type:       xdr.LedgerEntryTypeConfigSetting,
			Pre:        nil,
			Post:       &ledgerCostExtEntry,
		},
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Type:       xdr.LedgerEntryTypeConfigSetting,
			Pre:        nil,
			Post:       &scpTimingEntry,
		},
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Type:       xdr.LedgerEntryTypeConfigSetting,
			Pre:        nil,
			Post:       &frozenKeysEntry,
		},
		{
			ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryCreated,
			Type:       xdr.LedgerEntryTypeConfigSetting,
			Pre:        nil,
			Post:       &bypassTxsEntry,
		},
	}
}

func makeConfigSettingTestOutput() []ConfigSettingOutput {
	contractMapType := make([]map[string]string, 0)
	bucket := make([]uint64, 0)
	emptyStrSlice := make([]string, 0)

	// Base output with all zero values (for ContractMaxSizeBytes test)
	baseOutput := ConfigSettingOutput{
		ConfigSettingId:                        0,
		ContractMaxSizeBytes:                   0,
		LedgerMaxInstructions:                  0,
		TxMaxInstructions:                      0,
		FeeRatePerInstructionsIncrement:        0,
		TxMemoryLimit:                          0,
		LedgerMaxReadLedgerEntries:             0,
		LedgerMaxDiskReadEntries:               0,
		LedgerMaxReadBytes:                     0,
		LedgerMaxDiskReadBytes:                 0,
		LedgerMaxWriteLedgerEntries:            0,
		LedgerMaxWriteBytes:                    0,
		TxMaxReadLedgerEntries:                 0,
		TxMaxDiskReadEntries:                   0,
		TxMaxReadBytes:                         0,
		TxMaxDiskReadBytes:                     0,
		TxMaxWriteLedgerEntries:                0,
		TxMaxWriteBytes:                        0,
		FeeReadLedgerEntry:                     0,
		FeeDiskReadLedgerEntry:                 0,
		FeeWriteLedgerEntry:                    0,
		FeeRead1Kb:                             0,
		FeeWrite1Kb:                            0,
		FeeDiskRead1Kb:                         0,
		BucketListTargetSizeBytes:              0,
		SorobanStateTargetSizeBytes:            0,
		WriteFee1KbBucketListLow:               0,
		RentFee1KBSorobanStateSizeLow:          0,
		WriteFee1KbBucketListHigh:              0,
		RentFee1KBSorobanStateSizeHigh:         0,
		BucketListWriteFeeGrowthFactor:         0,
		SorobanStateRentFeeGrowthFactor:        0,
		FeeHistorical1Kb:                       0,
		TxMaxContractEventsSizeBytes:           0,
		FeeContractEvents1Kb:                   0,
		LedgerMaxTxsSizeBytes:                  0,
		TxMaxSizeBytes:                         0,
		FeeTxSize1Kb:                           0,
		ContractCostParamsCpuInsns:             contractMapType,
		ContractCostParamsMemBytes:             contractMapType,
		ContractDataKeySizeBytes:               0,
		ContractDataEntrySizeBytes:             0,
		MaxEntryTtl:                            0,
		MinTemporaryTtl:                        0,
		MinPersistentTtl:                       0,
		AutoBumpLedgers:                        0,
		PersistentRentRateDenominator:          0,
		TempRentRateDenominator:                0,
		MaxEntriesToArchive:                    0,
		BucketListSizeWindowSampleSize:         0,
		LiveSorobanStateSizeWindowSampleSize:   0,
		LiveSorobanStateSizeWindowSamplePeriod: 0,
		EvictionScanSize:                       0,
		StartingEvictionScanLevel:              0,
		LedgerMaxTxCount:                       0,
		BucketListSizeWindow:                   bucket,
		LiveSorobanStateSizeWindow:             bucket,
		LedgerMaxDependentTxClusters:           0,
		TxMaxFootprintEntries:                  0,
		LedgerTargetCloseTimeMilliseconds:      0,
		NominationTimeoutInitialMilliseconds:   0,
		NominationTimeoutIncrementMilliseconds: 0,
		BallotTimeoutInitialMilliseconds:       0,
		BallotTimeoutIncrementMilliseconds:     0,
		FrozenLedgerKeys:                       emptyStrSlice,
		FrozenLedgerKeysToFreeze:               emptyStrSlice,
		FrozenLedgerKeysToUnfreeze:             emptyStrSlice,
		FreezeBypassTxs:                        emptyStrSlice,
		FreezeBypassTxsToAdd:                   emptyStrSlice,
		FreezeBypassTxsToRemove:                emptyStrSlice,
		LastModifiedLedger:                     24229503,
		LedgerEntryChange:                      1,
		Deleted:                                false,
		LedgerSequence:                         10,
		ClosedAt:                               time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
	}

	// P23: ContractParallelCompute output
	parallelComputeOutput := baseOutput
	parallelComputeOutput.ConfigSettingId = 14
	parallelComputeOutput.LedgerMaxDependentTxClusters = 5
	parallelComputeOutput.LastModifiedLedger = 24229504
	parallelComputeOutput.LedgerEntryChange = 0
	parallelComputeOutput.ContractCostParamsCpuInsns = contractMapType
	parallelComputeOutput.ContractCostParamsMemBytes = contractMapType
	parallelComputeOutput.BucketListSizeWindow = bucket
	parallelComputeOutput.LiveSorobanStateSizeWindow = bucket
	parallelComputeOutput.FrozenLedgerKeys = emptyStrSlice
	parallelComputeOutput.FrozenLedgerKeysToFreeze = emptyStrSlice
	parallelComputeOutput.FrozenLedgerKeysToUnfreeze = emptyStrSlice
	parallelComputeOutput.FreezeBypassTxs = emptyStrSlice
	parallelComputeOutput.FreezeBypassTxsToAdd = emptyStrSlice
	parallelComputeOutput.FreezeBypassTxsToRemove = emptyStrSlice

	// P23: ContractLedgerCostExt output
	ledgerCostExtOutput := baseOutput
	ledgerCostExtOutput.ConfigSettingId = 15
	ledgerCostExtOutput.TxMaxFootprintEntries = 100
	ledgerCostExtOutput.FeeWrite1Kb = 2000
	ledgerCostExtOutput.LastModifiedLedger = 24229505
	ledgerCostExtOutput.LedgerEntryChange = 0
	ledgerCostExtOutput.ContractCostParamsCpuInsns = contractMapType
	ledgerCostExtOutput.ContractCostParamsMemBytes = contractMapType
	ledgerCostExtOutput.BucketListSizeWindow = bucket
	ledgerCostExtOutput.LiveSorobanStateSizeWindow = bucket
	ledgerCostExtOutput.FrozenLedgerKeys = emptyStrSlice
	ledgerCostExtOutput.FrozenLedgerKeysToFreeze = emptyStrSlice
	ledgerCostExtOutput.FrozenLedgerKeysToUnfreeze = emptyStrSlice
	ledgerCostExtOutput.FreezeBypassTxs = emptyStrSlice
	ledgerCostExtOutput.FreezeBypassTxsToAdd = emptyStrSlice
	ledgerCostExtOutput.FreezeBypassTxsToRemove = emptyStrSlice

	// P23: ContractScpTiming output
	scpTimingOutput := baseOutput
	scpTimingOutput.ConfigSettingId = 16
	scpTimingOutput.LedgerTargetCloseTimeMilliseconds = 5000
	scpTimingOutput.NominationTimeoutInitialMilliseconds = 1000
	scpTimingOutput.NominationTimeoutIncrementMilliseconds = 500
	scpTimingOutput.BallotTimeoutInitialMilliseconds = 1000
	scpTimingOutput.BallotTimeoutIncrementMilliseconds = 1000
	scpTimingOutput.LastModifiedLedger = 24229506
	scpTimingOutput.LedgerEntryChange = 0
	scpTimingOutput.ContractCostParamsCpuInsns = contractMapType
	scpTimingOutput.ContractCostParamsMemBytes = contractMapType
	scpTimingOutput.BucketListSizeWindow = bucket
	scpTimingOutput.LiveSorobanStateSizeWindow = bucket
	scpTimingOutput.FrozenLedgerKeys = emptyStrSlice
	scpTimingOutput.FrozenLedgerKeysToFreeze = emptyStrSlice
	scpTimingOutput.FrozenLedgerKeysToUnfreeze = emptyStrSlice
	scpTimingOutput.FreezeBypassTxs = emptyStrSlice
	scpTimingOutput.FreezeBypassTxsToAdd = emptyStrSlice
	scpTimingOutput.FreezeBypassTxsToRemove = emptyStrSlice

	// P26 CAP-77: FrozenLedgerKeys output (ID=17)
	frozenKeysOutput := baseOutput
	frozenKeysOutput.ConfigSettingId = 17
	frozenKeysOutput.LastModifiedLedger = 24229507
	frozenKeysOutput.LedgerEntryChange = 0
	frozenKeysOutput.ContractCostParamsCpuInsns = contractMapType
	frozenKeysOutput.ContractCostParamsMemBytes = contractMapType
	frozenKeysOutput.BucketListSizeWindow = bucket
	frozenKeysOutput.LiveSorobanStateSizeWindow = bucket
	frozenKeysOutput.FrozenLedgerKeys = []string{"AAAAAAAAAAABlHJijueOuScU0i0DkJY8JNkn6gCZmUhuiR+sLaqcIQ==", "AAAAAAAAAACdpD9sS2jlS4+g0cF9ZjMgNJrm2J2L7tIPDnaA6sV7hQ=="}
	frozenKeysOutput.FrozenLedgerKeysToFreeze = emptyStrSlice
	frozenKeysOutput.FrozenLedgerKeysToUnfreeze = emptyStrSlice
	frozenKeysOutput.FreezeBypassTxs = emptyStrSlice
	frozenKeysOutput.FreezeBypassTxsToAdd = emptyStrSlice
	frozenKeysOutput.FreezeBypassTxsToRemove = emptyStrSlice

	// P26 CAP-77: FreezeBypassTxs output (ID=19)
	bypassTxsOutput := baseOutput
	bypassTxsOutput.ConfigSettingId = 19
	bypassTxsOutput.LastModifiedLedger = 24229508
	bypassTxsOutput.LedgerEntryChange = 0
	bypassTxsOutput.ContractCostParamsCpuInsns = contractMapType
	bypassTxsOutput.ContractCostParamsMemBytes = contractMapType
	bypassTxsOutput.BucketListSizeWindow = bucket
	bypassTxsOutput.LiveSorobanStateSizeWindow = bucket
	bypassTxsOutput.FrozenLedgerKeys = emptyStrSlice
	bypassTxsOutput.FrozenLedgerKeysToFreeze = emptyStrSlice
	bypassTxsOutput.FrozenLedgerKeysToUnfreeze = emptyStrSlice
	bypassTxsOutput.FreezeBypassTxs = []string{
		"6161616162626262636363636464646465656565666666666767676768686868",
		"3131313132323232333333333434343435353535363636363737373738383838",
	}
	bypassTxsOutput.FreezeBypassTxsToAdd = emptyStrSlice
	bypassTxsOutput.FreezeBypassTxsToRemove = emptyStrSlice

	return []ConfigSettingOutput{
		baseOutput,
		parallelComputeOutput,
		ledgerCostExtOutput,
		scpTimingOutput,
		frozenKeysOutput,
		bypassTxsOutput,
	}
}
