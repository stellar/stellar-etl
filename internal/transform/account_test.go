package transform

import (
	"fmt"
	"testing"

	"github.com/stellar/go/xdr"
	"github.com/stretchr/testify/assert"
)

func TestTransformAccount(t *testing.T) {
	type transformTest struct {
		input      xdr.LedgerEntry
		wantOutput AccountOutput
		wantErr    error
	}

	hardCodedInput, err := makeAccountTestInput()
	assert.NoError(t, err)
	hardCodedOutput := makeAccountTestOutput()

	tests := []transformTest{
		{
			xdr.LedgerEntry{
				Data: xdr.LedgerEntryData{
					Type: xdr.LedgerEntryTypeOffer,
				},
			},
			AccountOutput{}, fmt.Errorf("Could not extract account data from ledger entry; actual type is LedgerEntryTypeOffer"),
		},
		{
			wrapAccountEntry(xdr.AccountEntry{
				AccountId: genericAccountID,
				Balance:   -1,
			}, 0),
			AccountOutput{}, fmt.Errorf("Balance is negative (-1) for account: %s", genericAccountAddress),
		},
		{
			wrapAccountEntry(xdr.AccountEntry{
				AccountId: genericAccountID,
				Ext: xdr.AccountEntryExt{
					V: 1,
					V1: &xdr.AccountEntryV1{
						Liabilities: xdr.Liabilities{
							Buying: -1,
						},
					},
				},
			}, 0),
			AccountOutput{}, fmt.Errorf("The buying liabilities count is negative (-1) for account: %s", genericAccountAddress),
		},
		{
			wrapAccountEntry(xdr.AccountEntry{
				AccountId: genericAccountID,
				Ext: xdr.AccountEntryExt{
					V: 1,
					V1: &xdr.AccountEntryV1{
						Liabilities: xdr.Liabilities{
							Selling: -2,
						},
					},
				},
			}, 0),
			AccountOutput{}, fmt.Errorf("The selling liabilities count is negative (-2) for account: %s", genericAccountAddress),
		},
		{
			wrapAccountEntry(xdr.AccountEntry{
				AccountId: genericAccountID,
				SeqNum:    -3,
			}, 0),
			AccountOutput{}, fmt.Errorf("Account sequence number is negative (-3) for account: %s", genericAccountAddress),
		},
		{
			hardCodedInput,
			hardCodedOutput, nil,
		},
	}

	for _, test := range tests {
		actualOutput, actualError := TransformAccount(test.input)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func wrapAccountEntry(accountEntry xdr.AccountEntry, lastModified int) xdr.LedgerEntry {
	return xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(lastModified),
		Data: xdr.LedgerEntryData{
			Type:    xdr.LedgerEntryTypeAccount,
			Account: &accountEntry,
		},
	}
}

func makeAccountTestInput() (ledgerEntry xdr.LedgerEntry, err error) {
	ledgerEntry = xdr.LedgerEntry{
		LastModifiedLedgerSeq: 30705278,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId:     testAccount1ID,
				Balance:       10959979,
				SeqNum:        117801117454198833,
				NumSubEntries: 141,
				InflationDest: &testAccount2ID,
				Flags:         4,
				HomeDomain:    "examplehome.com",
				Thresholds:    xdr.Thresholds([4]byte{2, 1, 3, 5}),
				Ext: xdr.AccountEntryExt{
					V: 1,
					V1: &xdr.AccountEntryV1{
						Liabilities: xdr.Liabilities{
							Buying:  1000,
							Selling: 1500,
						},
					},
				},
			},
		},
	}
	return
}

func makeAccountTestOutput() AccountOutput {
	return AccountOutput{
		AccountID:            testAccount1Address,
		Balance:              10959979,
		BuyingLiabilities:    1000,
		SellingLiabilities:   1500,
		SequenceNumber:       117801117454198833,
		NumSubentries:        141,
		InflationDestination: testAccount2Address,
		Flags:                4,
		HomeDomain:           "examplehome.com",
		MasterWeight:         2,
		ThresholdLow:         1,
		ThresholdMedium:      3,
		ThresholdHigh:        5,
		LastModifiedLedger:   30705278,
	}
}
