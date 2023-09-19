package transform

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformContractData(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		passphrase string
		wantOutput ContractDataOutput
		wantErr    error
	}

	hardCodedInput := makeContractDataTestInput()
	hardCodedOutput := makeContractDataTestOutput()
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
			"unit test",
			ContractDataOutput{}, fmt.Errorf("Could not extract contract data from ledger entry; actual type is LedgerEntryTypeOffer"),
		},
	}

	for i := range hardCodedInput {
		tests = append(tests, transformTest{
			input:      hardCodedInput[i],
			passphrase: "unit test",
			wantOutput: hardCodedOutput[i],
			wantErr:    nil,
		})
	}

	for _, test := range tests {
		TransformContractData := NewTransformContractDataStruct(MockAssetFromContractData, MockContractBalanceFromContractData)
		actualOutput, actualError, _ := TransformContractData.TransformContractData(test.input, test.passphrase)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func MockAssetFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) *xdr.Asset {
	return &xdr.Asset{
		Type: xdr.AssetTypeAssetTypeNative,
	}
}

func MockContractBalanceFromContractData(ledgerEntry xdr.LedgerEntry, passphrase string) ([32]byte, *big.Int, bool) {
	var holder [32]byte
	return holder, big.NewInt(0), true
}

func makeContractDataTestInput() []ingest.Change {
	var hash xdr.Hash
	var scStr xdr.ScString = "a"

	contractDataLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &hash,
				},
				Key: xdr.ScVal{
					Type: xdr.ScValTypeScvContractInstance,
					Instance: &xdr.ScContractInstance{
						Executable: xdr.ContractExecutable{
							Type:     xdr.ContractExecutableTypeContractExecutableWasm,
							WasmHash: &hash,
						},
						Storage: &xdr.ScMap{
							xdr.ScMapEntry{
								Key: xdr.ScVal{
									Type: xdr.ScValTypeScvString,
									Str:  &scStr,
								},
								Val: xdr.ScVal{
									Type: xdr.ScValTypeScvString,
									Str:  &scStr,
								},
							},
						},
					},
				},
				Durability: xdr.ContractDataDurabilityPersistent,
			},
		},
	}

	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeContractData,
			Pre:  &xdr.LedgerEntry{},
			Post: &contractDataLedgerEntry,
		},
	}
}

func makeContractDataTestOutput() []ContractDataOutput {
	return []ContractDataOutput{
		{
			ContractId:                "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
			ContractKeyType:           "ScValTypeScvContractInstance",
			ContractDurability:        "ContractDataDurabilityPersistent",
			ContractDataAssetCode:     "",
			ContractDataAssetIssuer:   "",
			ContractDataAssetType:     "AssetTypeAssetTypeNative",
			ContractDataBalanceHolder: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
			ContractDataBalance:       "0",
			LastModifiedLedger:        24229503,
			LedgerEntryChange:         1,
			Deleted:                   false,
		},
	}
}
