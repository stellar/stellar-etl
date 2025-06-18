package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/guregu/null"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/xdr"
)

func TestTransformAccountSigner(t *testing.T) {
	type inputStruct struct {
		ingest ingest.Change
	}

	type transformTest struct {
		input      inputStruct
		wantOutput []AccountSignerOutput
		wantErr    error
	}

	hardCodedInput := makeSignersTestInput()
	hardCodedOutput := makeSignersTestOutput()

	tests := []transformTest{
		{
			inputStruct{
				ingest.Change{
					Type: xdr.LedgerEntryTypeOffer,
					Pre:  nil,
					Post: &xdr.LedgerEntry{
						Data: xdr.LedgerEntryData{
							Type: xdr.LedgerEntryTypeOffer,
						},
					},
				},
			},
			nil, fmt.Errorf("could not extract signer data from ledger entry of type: LedgerEntryTypeOffer"),
		},
		{
			inputStruct{
				hardCodedInput,
			},
			hardCodedOutput, nil,
		},
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
		actualOutput, actualError := TransformSigners(test.input.ingest, header)
		assert.Equal(t, test.wantErr, actualError)
		assert.Equal(t, test.wantOutput, actualOutput)
	}
}

func makeSignersTestInput() ingest.Change {
	sponsor, _ := xdr.AddressToAccountId("GBADGWKHSUFOC4C7E3KXKINZSRX5KPHUWHH67UGJU77LEORGVLQ3BN3B")

	var ledgerEntry = xdr.LedgerEntry{
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
					V1: &xdr.AccountEntryExtensionV1{
						Liabilities: xdr.Liabilities{
							Buying:  1000,
							Selling: 1500,
						},
						Ext: xdr.AccountEntryExtensionV1Ext{
							V: 2,
							V2: &xdr.AccountEntryExtensionV2{
								SignerSponsoringIDs: []xdr.SponsorshipDescriptor{
									&sponsor,
									nil,
								},
							},
						},
					},
				},
				Signers: []xdr.Signer{
					{
						Key: xdr.SignerKey{
							Type:      xdr.SignerKeyTypeSignerKeyTypeEd25519,
							Ed25519:   &xdr.Uint256{4, 5, 6},
							PreAuthTx: nil,
							HashX:     nil,
						},
						Weight: 10.0,
					}, {
						Key: xdr.SignerKey{
							Type:      xdr.SignerKeyTypeSignerKeyTypeEd25519,
							Ed25519:   &xdr.Uint256{10, 11, 12},
							PreAuthTx: nil,
							HashX:     nil,
						},
						Weight: 20.0,
					},
				},
			},
		},
	}
	return ingest.Change{
		ChangeType: xdr.LedgerEntryChangeTypeLedgerEntryRemoved,
		Type:       xdr.LedgerEntryTypeAccount,
		Pre:        &ledgerEntry,
		Post:       nil,
	}
}

func makeSignersTestOutput() []AccountSignerOutput {
	return []AccountSignerOutput{
		{
			AccountID:          testAccount1ID.Address(),
			Signer:             "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ",
			Weight:             2.0,
			Sponsor:            null.String{},
			LastModifiedLedger: 30705278,
			LedgerEntryChange:  2,
			Deleted:            true,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		}, {
			AccountID:          testAccount1ID.Address(),
			Signer:             "GACAKBQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB3BQ",
			Weight:             10.0,
			Sponsor:            null.StringFrom("GBADGWKHSUFOC4C7E3KXKINZSRX5KPHUWHH67UGJU77LEORGVLQ3BN3B"),
			LastModifiedLedger: 30705278,
			LedgerEntryChange:  2,
			Deleted:            true,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		}, {
			AccountID:          testAccount1ID.Address(),
			Signer:             "GAFAWDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABNDC",
			Weight:             20.0,
			Sponsor:            null.String{},
			LastModifiedLedger: 30705278,
			LedgerEntryChange:  2,
			Deleted:            true,
			LedgerSequence:     10,
			ClosedAt:           time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC),
		},
	}
}
