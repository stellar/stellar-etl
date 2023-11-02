package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformContractCode(t *testing.T) {
	type transformTest struct {
		input      ingest.Change
		wantOutput ContractCodeOutput
		wantErr    error
	}

	hardCodedInput := makeContractCodeTestInput()
	hardCodedOutput := makeContractCodeTestOutput()
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
			ContractCodeOutput{}, fmt.Errorf("Could not extract contract code from ledger entry; actual type is LedgerEntryTypeOffer"),
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
		var closedAt time.Time
		actualOutput, actualError := TransformContractCode(test.input, closedAt)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeContractCodeTestInput() []ingest.Change {
	var hash [32]byte

	contractCodeLedgerEntry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 24229503,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{
				Hash: hash,
				Ext: xdr.ExtensionPoint{
					V: 1,
				},
			},
		},
	}

	return []ingest.Change{
		{
			Type: xdr.LedgerEntryTypeContractCode,
			Pre:  &xdr.LedgerEntry{},
			Post: &contractCodeLedgerEntry,
		},
	}
}

func makeContractCodeTestOutput() []ContractCodeOutput {
	return []ContractCodeOutput{
		{
			ContractCodeHash:   "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4",
			ContractCodeExtV:   1,
			LastModifiedLedger: 24229503,
			LedgerEntryChange:  1,
			Deleted:            false,
		},
	}
}
