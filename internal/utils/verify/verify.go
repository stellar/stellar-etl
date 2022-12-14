// lucas zanotelli dos santos 2022-11-18
// this code was mostly copied from https://github.com/stellar/go/blob/master/services/horizon/internal/ingest/verify.go

package verify

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/guregu/null"
	"github.com/stellar/stellar-etl/internal/transform"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/verify"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// stateVerifierExpectedIngestionVersion defines a version of ingestion system
// required by state verifier. This is done to prevent situations where
// ingestion has been updated with new features but state verifier does not
// check them.
// There is a test that checks it, to fix it: update the actual `verifyState`
// method instead of just updating this value!
// const stateVerifierExpectedIngestionVersion = 15

// verifyState is called as a go routine from pipeline post hook every 64
// ledgers. It checks if the state is correct. If another go routine is already
// running it exits.
func VerifyState(
	ctx context.Context,
	transformedOutput transform.TransformedOutput,
	archive historyarchive.ArchiveInterface,
	ledgerSequence uint32,
	verifyBatchSize int) (bool, error) {

	stateReader, _ := ingest.NewCheckpointChangeReader(ctx, archive, ledgerSequence)
	verifier := NewStateVerifier(stateReader, nil)

	var keys []xdr.LedgerKey
	keys, err := verifier.GetLedgerKeys(verifyBatchSize)
	if err != nil {
		return false, errors.Wrap(err, "verifier.GetLedgerKeys")
	}

	if len(keys) == 0 {
		return false, errors.Wrap(err, "no keys")
	}

	err = addAccountsToStateVerifier(ctx, verifier, transformedOutput.Accounts, transformedOutput.Signers)
	if err != nil {
		return false, errors.Wrap(err, "addAccountsToStateVerifier failed")
	}

	err = addOffersToStateVerifier(ctx, verifier, transformedOutput.Offers)
	if err != nil {
		return false, errors.Wrap(err, "addOffersToStateVerifier failed")
	}

	err = addTrustLinesToStateVerifier(ctx, verifier, transformedOutput.Trustlines)
	if err != nil {
		return false, errors.Wrap(err, "addTrustLinesToStateVerifier failed")
	}

	err = addClaimableBalanceToStateVerifier(ctx, verifier, transformedOutput.Claimable_balances)
	if err != nil {
		return false, errors.Wrap(err, "addClaimableBalanceToStateVerifier failed")
	}

	err = addLiquidityPoolsToStateVerifier(ctx, verifier, transformedOutput.Liquidity_pools)
	if err != nil {
		return false, errors.Wrap(err, "addLiquidityPoolsToStateVerifier failed")
	}

	return true, nil
}

func doesElementExist(s map[string]int32, str string) bool {
	for v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func addAccountsToStateVerifier(ctx context.Context,
	verifier *verify.StateVerifier,
	accounts []transform.AccountOutput,
	signers []transform.AccountSignerOutput,
) error {
	if len(accounts) == 0 && len(signers) == 0 {
		return nil
	}

	masterWeightMap := make(map[string]int32)
	signersMap := make(map[string][]xdr.Signer)

	sponsoringSignersMap := make(map[string]map[string]string)
	for _, row := range signers {
		if row.AccountID == row.Signer {
			masterWeightMap[row.AccountID] = row.Weight
		} else {
			signersMap[row.AccountID] = append(
				signersMap[row.AccountID],
				xdr.Signer{
					Key:    xdr.MustSigner(row.Signer),
					Weight: xdr.Uint32(row.Weight),
				},
			)
			if sponsoringSignersMap[row.AccountID] == nil {
				sponsoringSignersMap[row.AccountID] = make(map[string]string)
			}
			sponsoringSignersMap[row.AccountID][row.Signer] = row.Sponsor.String
		}
	}

	for _, row := range accounts {
		if row.Deleted {
			break
		}
		var inflationDest *xdr.AccountId
		if row.InflationDestination != "" {
			t := xdr.MustAddress(row.InflationDestination)
			inflationDest = &t
		}

		// Ensure master weight matches, if not it's a state error!
		if int32(row.MasterWeight) != masterWeightMap[row.AccountID] && doesElementExist(masterWeightMap, row.AccountID) {
			return ingest.NewStateError(
				fmt.Errorf(
					"master key weight in account %s does not match (expected=%d, actual=%d)",
					row.AccountID,
					masterWeightMap[row.AccountID],
					int32(row.MasterWeight),
				),
			)
		}

		signers := xdr.SortSignersByKey(signersMap[row.AccountID])
		signerSponsoringIDs := make([]xdr.SponsorshipDescriptor, len(signers))
		for i, signer := range signers {
			sponsor := sponsoringSignersMap[row.AccountID][signer.Key.Address()]
			if sponsor != "" {
				signerSponsoringIDs[i] = xdr.MustAddressPtr(sponsor)
			}
		}

		// Accounts that haven't done anything since Protocol 19 will not have a
		// V3 extension, so we need to check whether or not this extension needs
		// to be filled out.
		v3extension := xdr.AccountEntryExtensionV2Ext{V: 0}
		if row.SequenceLedger.Valid && row.SequenceTime.Valid {
			v3extension.V = 3
			v3extension.V3 = &xdr.AccountEntryExtensionV3{
				SeqLedger: xdr.Uint32(row.SequenceLedger.Int64),
				SeqTime:   xdr.TimePoint(row.SequenceTime.Int64),
			}
		}

		account := &xdr.AccountEntry{
			AccountId:     xdr.MustAddress(row.AccountID),
			Balance:       row.RawBalance,
			SeqNum:        xdr.SequenceNumber(row.SequenceNumber),
			NumSubEntries: xdr.Uint32(row.NumSubentries),
			InflationDest: inflationDest,
			Flags:         xdr.Uint32(row.Flags),
			HomeDomain:    xdr.String32(row.HomeDomain),
			Thresholds: xdr.Thresholds{
				row.RawMasterWeight,
				row.RawThresholdLow,
				row.RawThresholdMedium,
				row.RawThresholdHigh,
			},
			Signers: signers,
			Ext: xdr.AccountEntryExt{
				V: 1,
				V1: &xdr.AccountEntryExtensionV1{
					Liabilities: xdr.Liabilities{
						Buying:  row.RawBuyingLiabilities,
						Selling: row.RawSellingLiabilities,
					},
					Ext: xdr.AccountEntryExtensionV1Ext{
						V: 2,
						V2: &xdr.AccountEntryExtensionV2{
							NumSponsored:        xdr.Uint32(row.NumSponsored),
							NumSponsoring:       xdr.Uint32(row.NumSponsoring),
							SignerSponsoringIDs: signerSponsoringIDs,
							Ext:                 v3extension,
						},
					},
				},
			},
		}

		entry := xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(row.LastModifiedLedger),
			Data: xdr.LedgerEntryData{
				Type:    xdr.LedgerEntryTypeAccount,
				Account: account,
			},
		}
		addLedgerEntrySponsor(&entry, row.Sponsor)
		err := verifier.Write(entry)
		if err != nil {
			return err
		}
	}

	return nil
}

func addOffersToStateVerifier(
	ctx context.Context,
	verifier *verify.StateVerifier,
	offers []transform.OfferOutput,
) error {
	if len(offers) == 0 {
		return nil
	}

	for _, row := range offers {
		if row.Deleted {
			break
		}
		offerXDR := offerToXDR(row)
		entry := xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(row.LastModifiedLedger),
			Data: xdr.LedgerEntryData{
				Type:  xdr.LedgerEntryTypeOffer,
				Offer: &offerXDR,
			},
		}
		addLedgerEntrySponsor(&entry, row.Sponsor)
		err := verifier.Write(entry)
		if err != nil {
			return err
		}
	}

	return nil
}

func offerToXDR(row transform.OfferOutput) xdr.OfferEntry {
	return xdr.OfferEntry{
		SellerId: xdr.MustAddress(row.SellerID),
		OfferId:  xdr.Int64(row.OfferID),
		Selling:  row.SellingAsset,
		Buying:   row.BuyingAsset,
		Amount:   row.RawAmount,
		Price: xdr.Price{
			N: xdr.Int32(row.PriceN),
			D: xdr.Int32(row.PriceD),
		},
		Flags: xdr.Uint32(row.Flags),
	}
}

func addClaimableBalanceToStateVerifier(
	ctx context.Context,
	verifier *verify.StateVerifier,
	claims []transform.ClaimableBalanceOutput,
) error {
	if len(claims) == 0 {
		return nil
	}

	for _, row := range claims {
		if row.Deleted {
			break
		}
		claimants := []xdr.Claimant{}
		for _, claimant := range row.Claimants {
			claimants = append(claimants, xdr.Claimant{
				Type: xdr.ClaimantTypeClaimantTypeV0,
				V0: &xdr.ClaimantV0{
					Destination: xdr.MustAddress(claimant.Destination),
					Predicate:   claimant.Predicate,
				},
			})
		}
		claimants = xdr.SortClaimantsByDestination(claimants)
		var balanceID xdr.ClaimableBalanceId
		if err := xdr.SafeUnmarshalHex(row.BalanceID, &balanceID); err != nil {
			return err
		}
		cBalance := xdr.ClaimableBalanceEntry{
			BalanceId: balanceID,
			Claimants: claimants,
			Asset:     row.Asset,
			Amount:    xdr.Int64(row.RawAssetAmount),
		}
		if row.Flags != 0 {
			cBalance.Ext = xdr.ClaimableBalanceEntryExt{
				V: 1,
				V1: &xdr.ClaimableBalanceEntryExtensionV1{
					Flags: xdr.Uint32(row.Flags),
				},
			}
		}
		entry := xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(row.LastModifiedLedger),
			Data: xdr.LedgerEntryData{
				Type:             xdr.LedgerEntryTypeClaimableBalance,
				ClaimableBalance: &cBalance,
			},
		}
		addLedgerEntrySponsor(&entry, row.Sponsor)
		if err := verifier.Write(entry); err != nil {
			return err
		}
	}

	return nil
}

func addLiquidityPoolsToStateVerifier(
	ctx context.Context,
	verifier *verify.StateVerifier,
	lPools []transform.PoolOutput,
) error {
	if len(lPools) == 0 {
		return nil
	}

	for _, row := range lPools {
		if row.Deleted {
			break
		}
		lPoolEntry, err := liquidityPoolToXDR(row)
		if err != nil {
			return errors.Wrap(err, "Invalid liquidity pool row")
		}

		entry := xdr.LedgerEntry{
			LastModifiedLedgerSeq: xdr.Uint32(row.LastModifiedLedger),
			Data: xdr.LedgerEntryData{
				Type:          xdr.LedgerEntryTypeLiquidityPool,
				LiquidityPool: &lPoolEntry,
			},
		}
		if err := verifier.Write(entry); err != nil {
			return err
		}
	}

	return nil
}

func liquidityPoolToXDR(row transform.PoolOutput) (xdr.LiquidityPoolEntry, error) {
	id, err := hex.DecodeString(row.PoolID)
	if err != nil {
		return xdr.LiquidityPoolEntry{}, errors.Wrap(err, "Error decoding pool ID")
	}
	var poolID xdr.PoolId
	if len(id) != len(poolID) {
		return xdr.LiquidityPoolEntry{}, fmt.Errorf("error decoding pool ID, incorrect length (%d)", len(id))
	}
	copy(poolID[:], id)

	var lPoolEntry = xdr.LiquidityPoolEntry{
		LiquidityPoolId: poolID,
		Body: xdr.LiquidityPoolEntryBody{
			Type: row.RawPoolType,
			ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
				Params: xdr.LiquidityPoolConstantProductParameters{
					AssetA: row.AssetA,
					AssetB: row.AssetB,
					Fee:    xdr.Int32(row.PoolFee),
				},
				ReserveA:                 xdr.Int64(row.RawAssetAReserve),
				ReserveB:                 xdr.Int64(row.RawAssetBReserve),
				TotalPoolShares:          xdr.Int64(row.RawPoolShareCount),
				PoolSharesTrustLineCount: xdr.Int64(row.TrustlineCount),
			},
		},
	}
	return lPoolEntry, nil
}

func addLedgerEntrySponsor(entry *xdr.LedgerEntry, sponsor null.String) {
	ledgerEntrySponsor := xdr.SponsorshipDescriptor(nil)

	if !sponsor.IsZero() {
		ledgerEntrySponsor = xdr.MustAddressPtr(sponsor.String)
	}
	entry.Ext = xdr.LedgerEntryExt{
		V: 1,
		V1: &xdr.LedgerEntryExtensionV1{
			SponsoringId: ledgerEntrySponsor,
		},
	}
}

func addTrustLinesToStateVerifier(
	ctx context.Context,
	verifier *verify.StateVerifier,
	trusts []transform.TrustlineOutput,
) error {
	if len(trusts) == 0 {
		return nil
	}

	for _, row := range trusts {
		if row.Deleted {
			break
		}
		var entry xdr.LedgerEntry
		entry, err := trustLineToXDR(row)
		if err != nil {
			return err
		}

		if err = verifier.Write(entry); err != nil {
			return err
		}
	}

	return nil
}

func trustLineToXDR(row transform.TrustlineOutput) (xdr.LedgerEntry, error) {
	var asset xdr.TrustLineAsset
	switch row.AssetType {
	case int32(xdr.AssetTypeAssetTypePoolShare):
		asset = xdr.TrustLineAsset{
			Type:            xdr.AssetTypeAssetTypePoolShare,
			LiquidityPoolId: &xdr.PoolId{},
		}
		_, err := hex.Decode((*asset.LiquidityPoolId)[:], []byte(row.LiquidityPoolID))
		if err != nil {
			return xdr.LedgerEntry{}, errors.Wrap(err, "Error decoding liquidity pool id")
		}
	case int32(xdr.AssetTypeAssetTypeNative):
		asset = xdr.MustNewNativeAsset().ToTrustLineAsset()
	default:
		creditAsset, err := xdr.NewCreditAsset(row.AssetCode, row.AssetIssuer)
		if err != nil {
			return xdr.LedgerEntry{}, errors.Wrap(err, "Error decoding credit asset")
		}
		asset = creditAsset.ToTrustLineAsset()
	}

	trustline := xdr.TrustLineEntry{
		AccountId: xdr.MustAddress(row.AccountID),
		Asset:     asset,
		Balance:   row.RawBalance,
		Limit:     xdr.Int64(row.TrustlineLimit),
		Flags:     xdr.Uint32(row.Flags),
		Ext: xdr.TrustLineEntryExt{
			V: 1,
			V1: &xdr.TrustLineEntryV1{
				Liabilities: xdr.Liabilities{
					Buying:  row.RawBuying,
					Selling: row.RawSelling,
				},
			},
		},
	}
	entry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: xdr.Uint32(row.LastModifiedLedger),
		Data: xdr.LedgerEntryData{
			Type:      xdr.LedgerEntryTypeTrustline,
			TrustLine: &trustline,
		},
	}
	addLedgerEntrySponsor(&entry, row.Sponsor)
	return entry, nil
}
