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
	genericAccountID, err := xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, xdr.Uint256([32]byte{}))
	assert.NoError(t, err)
	genericAccountAddress, err := genericAccountID.GetAddress()
	assert.NoError(t, err)

	hardCodedInput, err := prepareHardcodedAccountTestInput()
	assert.NoError(t, err)
	hardCodedOutput := prepareHardcodedAccountTestOutput()

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

func prepareHardcodedAccountTestInput() (ledgerEntry xdr.LedgerEntry, err error) {
	hardCodedAccount, err := xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, xdr.Uint256([32]byte{0x88, 0xe1, 0xa6, 0xb4, 0xa5, 0x71, 0x52, 0x8f, 0xaa, 0x9f, 0x26, 0xe2, 0xe, 0x6c, 0xbc, 0x3f, 0xe7, 0x3c, 0xae, 0x80, 0xb4, 0x46, 0x63, 0xc, 0x5b, 0xef, 0x1, 0x52, 0xaf, 0x70, 0x7d, 0x78}))
	if err != nil {
		return
	}

	hardCodedInflationDest, err := xdr.NewMuxedAccount(xdr.CryptoKeyTypeKeyTypeEd25519, xdr.Uint256([32]byte{0x1c, 0x47, 0x41, 0x97, 0x18, 0xee, 0xfa, 0xa4, 0x5b, 0x38, 0xcb, 0x7f, 0x2f, 0x25, 0x50, 0x1a, 0x9e, 0x39, 0xcb, 0x83, 0x87, 0xa6, 0x36, 0xe9, 0xfb, 0xcc, 0xc, 0x74, 0xa4, 0x77, 0x3, 0x18}))
	hardCodedInflationDestID := hardCodedInflationDest.ToAccountId()
	if err != nil {
		return
	}

	ledgerEntry = xdr.LedgerEntry{
		LastModifiedLedgerSeq: 30705278,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId:     hardCodedAccount.ToAccountId(),
				Balance:       10959979,
				SeqNum:        117801117454198833,
				NumSubEntries: 141,
				InflationDest: &hardCodedInflationDestID,
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

func prepareHardcodedAccountTestOutput() AccountOutput {
	return AccountOutput{
		AccountID:            "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ",
		Balance:              10959979,
		BuyingLiabilities:    1000,
		SellingLiabilities:   1500,
		SequenceNumber:       117801117454198833,
		NumSubentries:        141,
		InflationDestination: "GAOEOQMXDDXPVJC3HDFX6LZFKANJ4OOLQOD2MNXJ7PGAY5FEO4BRRAQU",
		Flags:                4,
		HomeDomain:           "examplehome.com",
		MasterWeight:         2,
		ThresholdLow:         1,
		ThresholdMedium:      3,
		ThresholdHigh:        5,
		LastModifiedLedger:   30705278,
	}
}
