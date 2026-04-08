package transform

import (
	"fmt"
	"testing"
	"time"

	"github.com/guregu/null"

	"github.com/stretchr/testify/assert"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
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

	hardCodedInputRemovedEntry, _ := makeSignersTestInput(xdr.LedgerEntryChangeTypeLedgerEntryRemoved)
	hardCodedOutputRemovedEntry, _ := makeSignersTestOutput(xdr.LedgerEntryChangeTypeLedgerEntryRemoved)
	hardCodedInputRestoredEntry, _ := makeSignersTestInput(xdr.LedgerEntryChangeTypeLedgerEntryRestored)
	hardCodedOutputRestoredEntry, _ := makeSignersTestOutput(xdr.LedgerEntryChangeTypeLedgerEntryRestored)
	hardCodedInputUpdatedWithDeletion, _ := makeSignersTestInput(xdr.LedgerEntryChangeTypeLedgerEntryUpdated)
	hardCodedOutputUpdatedWithDeletion, _ := makeSignersTestOutput(xdr.LedgerEntryChangeTypeLedgerEntryUpdated)

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
				hardCodedInputRemovedEntry,
			},
			hardCodedOutputRemovedEntry, nil,
		},
		{
			inputStruct{
				hardCodedInputRestoredEntry,
			},
			hardCodedOutputRestoredEntry, nil,
		},
		// Tests that signer deletions embedded in an account UPDATE are emitted correctly.
		// Signers present in Pre but absent from Post must appear with Weight=0, Deleted=true,
		// LedgerEntryChange=REMOVED, and LastModifiedLedger from the Pre entry.
		{
			inputStruct{
				hardCodedInputUpdatedWithDeletion,
			},
			hardCodedOutputUpdatedWithDeletion, nil,
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

func makeSignersTestInput(changeType xdr.LedgerEntryChangeType) (ingest.Change, error) {
	ledgerEntry := makeTestLedgerEntry()

	switch changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return ingest.Change{
			ChangeType: changeType,
			Type:       xdr.LedgerEntryTypeAccount,
			Pre:        &ledgerEntry,
			Post:       nil,
		}, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRestored:
		return ingest.Change{
			ChangeType: changeType,
			Type:       xdr.LedgerEntryTypeAccount,
			Pre:        nil,
			Post:       &ledgerEntry,
		}, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		// Pre has 2 extra signers (A: Ed25519{4,5,6} weight=10, B: Ed25519{10,11,12} weight=20) + master.
		// Post has only signer A + master; signer B has been deleted.
		// Different LastModifiedLedgerSeq values let us verify which one lands on each output row.
		preLedgerEntry := makeTestLedgerEntry() // LastModifiedLedgerSeq=30705278, both signers present
		postLedgerEntry := makeTestLedgerEntry()
		postLedgerEntry.LastModifiedLedgerSeq = 30705279
		postLedgerEntry.Data.Account.Signers = postLedgerEntry.Data.Account.Signers[:1]
		postLedgerEntry.Data.Account.Ext.V1.Ext.V2.SignerSponsoringIDs = postLedgerEntry.Data.Account.Ext.V1.Ext.V2.SignerSponsoringIDs[:1]
		return ingest.Change{
			ChangeType: changeType,
			Type:       xdr.LedgerEntryTypeAccount,
			Pre:        &preLedgerEntry,
			Post:       &postLedgerEntry,
		}, nil
	default:
		return ingest.Change{}, fmt.Errorf("unexpected changeType: %v", changeType)
	}
}

func makeTestLedgerEntry() xdr.LedgerEntry {
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
	return ledgerEntry
}

func makeSignersTestOutput(changeType xdr.LedgerEntryChangeType) ([]AccountSignerOutput, error) {
	closedAt := time.Date(1970, time.January, 1, 0, 16, 40, 0, time.UTC)

	switch changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
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
				ClosedAt:           closedAt,
			}, {
				AccountID:          testAccount1ID.Address(),
				Signer:             "GACAKBQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB3BQ",
				Weight:             10.0,
				Sponsor:            null.StringFrom("GBADGWKHSUFOC4C7E3KXKINZSRX5KPHUWHH67UGJU77LEORGVLQ3BN3B"),
				LastModifiedLedger: 30705278,
				LedgerEntryChange:  2,
				Deleted:            true,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			}, {
				AccountID:          testAccount1ID.Address(),
				Signer:             "GAFAWDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABNDC",
				Weight:             20.0,
				Sponsor:            null.String{},
				LastModifiedLedger: 30705278,
				LedgerEntryChange:  2,
				Deleted:            true,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			},
		}, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRestored:
		return []AccountSignerOutput{
			{
				AccountID:          testAccount1ID.Address(),
				Signer:             "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ",
				Weight:             2.0,
				Sponsor:            null.String{},
				LastModifiedLedger: 30705278,
				LedgerEntryChange:  4,
				Deleted:            false,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			}, {
				AccountID:          testAccount1ID.Address(),
				Signer:             "GACAKBQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB3BQ",
				Weight:             10.0,
				Sponsor:            null.StringFrom("GBADGWKHSUFOC4C7E3KXKINZSRX5KPHUWHH67UGJU77LEORGVLQ3BN3B"),
				LastModifiedLedger: 30705278,
				LedgerEntryChange:  4,
				Deleted:            false,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			}, {
				AccountID:          testAccount1ID.Address(),
				Signer:             "GAFAWDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABNDC",
				Weight:             20.0,
				Sponsor:            null.String{},
				LastModifiedLedger: 30705278,
				LedgerEntryChange:  4,
				Deleted:            false,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			},
		}, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		// Post has signer A (weight=10) + master (weight=2); signer B (weight=20) was deleted.
		// Deleted signers use Pre's LastModifiedLedgerSeq (30705278); remaining signers use Post's (30705279).
		// Rows are sorted ascending by weight: deleted B (0), master (2), signer A (10).
		return []AccountSignerOutput{
			{
				// Signer B deleted: Weight=0, LedgerEntryChange=REMOVED, LastModifiedLedger from Pre
				AccountID:          testAccount1ID.Address(),
				Signer:             "GAFAWDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABNDC",
				Weight:             0,
				Sponsor:            null.String{},
				LastModifiedLedger: 30705278,
				LedgerEntryChange:  2,
				Deleted:            true,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			}, {
				// Master key remains: LedgerEntryChange=UPDATED, LastModifiedLedger from Post
				AccountID:          testAccount1ID.Address(),
				Signer:             "GCEODJVUUVYVFD5KT4TOEDTMXQ76OPFOQC2EMYYMLPXQCUVPOB6XRWPQ",
				Weight:             2,
				Sponsor:            null.String{},
				LastModifiedLedger: 30705279,
				LedgerEntryChange:  1,
				Deleted:            false,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			}, {
				// Signer A remains: LedgerEntryChange=UPDATED, LastModifiedLedger from Post
				AccountID:          testAccount1ID.Address(),
				Signer:             "GACAKBQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB3BQ",
				Weight:             10,
				Sponsor:            null.StringFrom("GBADGWKHSUFOC4C7E3KXKINZSRX5KPHUWHH67UGJU77LEORGVLQ3BN3B"),
				LastModifiedLedger: 30705279,
				LedgerEntryChange:  1,
				Deleted:            false,
				LedgerSequence:     10,
				ClosedAt:           closedAt,
			},
		}, nil
	default:
		return []AccountSignerOutput{}, fmt.Errorf("unexpected changeType: %v", changeType)
	}
}
