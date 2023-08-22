package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformConfigSetting(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		header     xdr.LedgerHeaderHistoryEntry
		wantOutput ConfigSettingOutput
		wantErr    error
	}

	hardCodedInput := makeConfigSettingTestInput()
	hardCodedOutput := makeConfigSettingTestOutput()
	tests := []transformTest{
		{
			ingest.Change{
				Type: xdr.LedgerEntryTypeOffer,
				Pre:  nil,
				Post: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeOffer,
					},
				},
			},
			xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: 0,
					},
				},
			},
			ConfigSettingOutput{}, fmt.Errorf("Could not extract config setting from ledger entry; actual type is LedgerEntryTypeOffer"),
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
		actualOutput, actualError := TransformConfigSetting(test.input, test.header)
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

	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeConfigSetting,
			Pre:  &xdr.LedgerEntry{},
			Post: &contractDataLedgerEntry,
		},
	}
}

func makeConfigSettingTestOutput() []ConfigSettingOutput {
	contractMapType := make([]map[string]string, 0, 0)
	bucket := make([]uint64, 0, 0)

	return []ConfigSettingOutput{
		{
			ConfigSettingId:                 0,
			ContractMaxSizeBytes:            0,
			LedgerMaxInstructions:           0,
			TxMaxInstructions:               0,
			FeeRatePerInstructionsIncrement: 0,
			TxMemoryLimit:                   0,
			LedgerMaxReadLedgerEntries:      0,
			LedgerMaxReadBytes:              0,
			LedgerMaxWriteLedgerEntries:     0,
			LedgerMaxWriteBytes:             0,
			TxMaxReadLedgerEntries:          0,
			TxMaxReadBytes:                  0,
			TxMaxWriteLedgerEntries:         0,
			TxMaxWriteBytes:                 0,
			FeeReadLedgerEntry:              0,
			FeeWriteLedgerEntry:             0,
			FeeRead1Kb:                      0,
			FeeWrite1Kb:                     0,
			BucketListSizeBytes:             0,
			BucketListFeeRateLow:            0,
			BucketListFeeRateHigh:           0,
			BucketListGrowthFactor:          0,
			FeeHistorical1Kb:                0,
			TxMaxExtendedMetaDataSizeBytes:  0,
			FeeExtendedMetaData1Kb:          0,
			LedgerMaxPropagateSizeBytes:     0,
			TxMaxSizeBytes:                  0,
			FeePropagateData1Kb:             0,
			ContractCostParamsCpuInsns:      contractMapType,
			ContractCostParamsMemBytes:      contractMapType,
			ContractDataKeySizeBytes:        0,
			ContractDataEntrySizeBytes:      0,
			MaxEntryExpiration:              0,
			MinTempEntryExpiration:          0,
			MinPersistentEntryExpiration:    0,
			AutoBumpLedgers:                 0,
			PersistentRentRateDenominator:   0,
			TempRentRateDenominator:         0,
			MaxEntriesToExpire:              0,
			BucketListSizeWindowSampleSize:  0,
			EvictionScanSize:                0,
			LedgerMaxTxCount:                0,
			BucketListSizeWindow:            bucket,
			LastModifiedLedger:              24229503,
			LedgerEntryChange:               1,
			Deleted:                         false,
		},
	}
}
