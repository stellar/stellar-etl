package verify

import (
	"context"
	"encoding/hex"
	"fmt"

	// "github.com/aws/aws-sdk-go/service/swf"
	"github.com/guregu/null"
	"github.com/stellar/stellar-etl/internal/transform"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// const verifyBatchSize = 50000
// const assetStatsBatchSize = 500

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
	transformedOutput transform.TransformedOutputType,
	archive historyarchive.ArchiveInterface,
	ledgerSequence uint32,
	verifyBatchSize int) (bool, error) {
	// s.stateVerificationMutex.Lock()
	// if s.stateVerificationRunning {
	// 	log.Warn("State verification is already running...")
	// 	s.stateVerificationMutex.Unlock()
	// 	return nil
	// }
	// s.stateVerificationRunning = true
	// s.stateVerificationMutex.Unlock()
	// defer func() {
	// 	s.stateVerificationMutex.Lock()
	// 	s.stateVerificationRunning = false
	// 	s.stateVerificationMutex.Unlock()
	// }()

	// updateMetrics := false

	// if stateVerifierExpectedIngestionVersion != CurrentVersion {
	// 	log.Errorf(
	// 		"State verification expected version is %d but actual is: %d",
	// 		stateVerifierExpectedIngestionVersion,
	// 		CurrentVersion,
	// 	)
	// 	return nil
	// }

	// historyQ := s.historyQ.CloneIngestionQ()
	// defer historyQ.Rollback()
	// err := historyQ.BeginTx(&sql.TxOptions{
	// 	Isolation: sql.LevelRepeatableRead,
	// 	ReadOnly:  true,
	// })
	// if err != nil {
	// 	return errors.Wrap(err, "Error starting transaction")
	// }

	// // Ensure the ledger is a checkpoint ledger
	// ledgerSequence, err := historyQ.GetLastLedgerIngestNonBlocking(s.ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running historyQ.GetLastLedgerIngestNonBlocking")
	// }

	// localLog := log.WithFields(logpkg.F{
	// 	"subservice": "state",
	// 	"sequence":   ledgerSequence,
	// })

	// if !s.checkpointManager.IsCheckpoint(ledgerSequence) {
	// 	localLog.Info("Current ledger is not a checkpoint ledger. Canceling...")
	// 	return nil
	// }

	// localLog.Info("Starting state verification")

	// if verifyAgainstLatestCheckpoint {
	// 	retries := 0
	// 	for {
	// 		// Get root HAS to check if we're checking one of the latest ledgers or
	// 		// Horizon is catching up. It doesn't make sense to verify old ledgers as
	// 		// we want to check the latest state.
	// 		var historyLatestSequence uint32
	// 		historyLatestSequence, err = s.historyAdapter.GetLatestLedgerSequence()
	// 		if err != nil {
	// 			return errors.Wrap(err, "Error getting the latest ledger sequence")
	// 		}

	// 		if ledgerSequence < historyLatestSequence {
	// 			localLog.Info("Current ledger is old. Canceling...")
	// 			return nil
	// 		}

	// 		if ledgerSequence == historyLatestSequence {
	// 			break
	// 		}

	// 		localLog.Info("Waiting for stellar-core to publish HAS...")
	// 		select {
	// 		case <-s.ctx.Done():
	// 			localLog.Info("State verifier shut down...")
	// 			return nil
	// 		case <-time.After(5 * time.Second):
	// 			// Wait for stellar-core to publish HAS
	// 			retries++
	// 			if retries == 12 {
	// 				localLog.Info("Checkpoint not published. Canceling...")
	// 				return nil
	// 			}
	// 		}
	// 	}
	// }

	// totalByType := map[string]int64{}

	// startTime := time.Now()
	// defer func() {
	// 	duration := time.Since(startTime).Seconds()
	// 	if updateMetrics {
	// 		// Don't update metrics if context canceled.
	// 		if s.ctx.Err() != context.Canceled {
	// 			s.Metrics().StateVerifyDuration.Observe(float64(duration))
	// 			for typ, tot := range totalByType {
	// 				s.Metrics().StateVerifyLedgerEntriesCount.
	// 					With(prometheus.Labels{"type": typ}).Set(float64(tot))
	// 			}
	// 		}
	// 	}
	// 	log.WithField("duration", duration).Info("State verification finished")

	// }()

	// localLog.Info("Creating state reader...")

	stateReader, _ := ingest.NewCheckpointChangeReader(ctx, archive, ledgerSequence)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running GetState")
	// }
	// defer stateReader.Close()

	verifier := NewStateVerifier(stateReader, nil)

	// assetStats := transform.AssetStatSet{}
	// total := int64(0)

	// values := reflect.ValueOf(transformedOutput)
	// typesOf := values.Type()
	// for i := 0; i < values.NumField(); i++ {
	var keys []xdr.LedgerKey
	keys, err := verifier.GetLedgerKeys(verifyBatchSize)
	if err != nil {
		return false, errors.Wrap(err, "verifier.GetLedgerKeys")
	}

	if len(keys) == 0 {
		return false, errors.Wrap(err, "no keys")
	}

	// accounts := make([]string, 0, verifyBatchSize)
	// data := make([]xdr.LedgerKeyData, 0, verifyBatchSize)
	// offers := make([]int64, 0, verifyBatchSize)
	// var trustLines []transform.TrustlineOutput
	// cBalances := make([]xdr.ClaimableBalanceId, 0, verifyBatchSize)
	// lPools := make([]xdr.PoolId, 0, verifyBatchSize)
	// for _, key := range output {

	// switch typesOf.Field(i).Name {

	// case xdr.LedgerEntryTypeAccount:
	// 	accounts = append(accounts, key.Account.AccountId.Address())
	// 	totalByType["accounts"]++
	// case xdr.LedgerEntryTypeData:
	// 	data = append(data, *key.Data)
	// 	totalByType["data"]++
	// case xdr.LedgerEntryTypeOffer:
	// 	offers = append(offers, int64(key.Offer.OfferId))
	// 	totalByType["offers"]++
	// case "Trustlines":
	// 	t := transformedOutput.Trustlines
	// 	trustLines = append(trustLines, t)
	// totalByType["trustlines"]++
	// case xdr.LedgerEntryTypeClaimableBalance:
	// 	cBalances = append(cBalances, key.ClaimableBalance.BalanceId)
	// 	totalByType["claimable_balances"]++
	// case xdr.LedgerEntryTypeLiquidityPool:
	// 	lPools = append(lPools, key.LiquidityPool.LiquidityPoolId)
	// 	totalByType["liquidity_pools"]++
	// default:
	// 	return errors.New("GetLedgerKeys return unexpected type")
	// }
	// }

	err = addAccountsToStateVerifier(ctx, verifier, transformedOutput.Accounts, transformedOutput.Signers)
	if err != nil {
		return false, errors.Wrap(err, "addAccountsToStateVerifier failed")
	}

	// err = addDataToStateVerifier(s.ctx, verifier, historyQ, data)
	// if err != nil {
	// 	return errors.Wrap(err, "addDataToStateVerifier failed")
	// }

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

	// 	total += int64(len(keys))
	// 	localLog.WithField("total", total).Info("Batch added to StateVerifier")
	// }
	// }

	// localLog.WithField("total", total).Info("Finished writing to StateVerifier")

	// countAccounts, err := historyQ.CountAccounts(s.ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running historyQ.CountAccounts")
	// }

	// countData, err := historyQ.CountAccountsData(s.ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running historyQ.CountData")
	// }

	// countOffers, err := historyQ.CountOffers(s.ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running historyQ.CountOffers")
	// }

	// countTrustLines, err := historyQ.CountTrustLines(s.ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running historyQ.CountTrustLines")
	// }

	// countClaimableBalances, err := historyQ.CountClaimableBalances(s.ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running historyQ.CountClaimableBalances")
	// }

	// countLiquidityPools, err := historyQ.CountLiquidityPools(s.ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running historyQ.CountLiquidityPools")
	// }

	// err = verifier.Verify(countAccounts + countData + countOffers + countTrustLines + countClaimableBalances + countLiquidityPools)
	// if err != nil {
	// 	return errors.Wrap(err, "verifier.Verify failed")
	// }

	// err = checkAssetStats(s.ctx, assetStats, historyQ)
	// if err != nil {
	// 	return errors.Wrap(err, "checkAssetStats failed")
	// }

	// localLog.Info("State correct")
	// updateMetrics = true
	return true, nil
}

// func checkAssetStats(ctx context.Context, set transform.AssetStatSet, q transform.IngestionrOfferOutput) error {
// 	page := db2.PageQuery{
// 		Order: "asc",
// 		Limit: assetStatsBatchSize,
// 	}

// 	for {
// 		assetStats, err := q.GetAssetStats(ctx, "", "", page)
// 		if err != nil {
// 			return errors.Wrap(err, "could not fetch asset stats from db")
// 		}
// 		if len(assetStats) == 0 {
// 			break
// 		}

// 		for _, assetStat := range assetStats {
// 			fromSet, removed := set.Remove(assetStat.AssetType, assetStat.AssetCode, assetStat.AssetIssuer)
// 			if !removed {
// 				return ingest.NewStateError(
// 					fmt.Errorf(
// 						"db contains asset stat with code %s issuer %s which is missing from HAS",
// 						assetStat.AssetCode, assetStat.AssetIssuer,
// 					),
// 				)
// 			}

// 			if fromSet != assetStat {
// 				return ingest.NewStateError(
// 					fmt.Errorf(
// 						"db asset stat with code %s issuer %s does not match asset stat from HAS: expected=%v actual=%v",
// 						assetStat.AssetCode, assetStat.AssetIssuer, fromSet, assetStat,
// 					),
// 				)
// 			}
// 		}

// 		page.Cursor = assetStats[len(assetStats)-1].PagingToken()
// 	}

// 	if len(set) > 0 {
// 		return ingest.NewStateError(
// 			fmt.Errorf(
// 				"HAS contains %d more asset stats than db",
// 				len(set),
// 			),
// 		)
// 	}
// 	return nil
// }

func doesElementExist(s map[string]int32, str string) bool {
	for v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func addAccountsToStateVerifier(ctx context.Context,
	verifier *StateVerifier,
	accounts []transform.AccountOutput,
	signers []transform.AccountSignerOutput,
) error {
	if len(accounts) == 0 && len(signers) == 0 {
		return nil
	}

	// accounts, err := q.GetAccountsByIDs(ctx, ids)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running transform.rOfferOutput.GetAccountsByIDs")
	// }

	// signers, err := q.SignersForAccounts(ctx, ids)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running transform.rOfferOutput.SignersForAccounts")
	// }

	masterWeightMap := make(map[string]int32)
	signersMap := make(map[string][]xdr.Signer)
	// map[accountID]map[signerKey]sponsor
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

// func addDataToStateVerifier(ctx context.Context, verifier *StateVerifier, q transform.IngestionrOfferOutput, lkeys []xdr.LedgerKeyData) error {
// 	if len(lkeys) == 0 {
// 		return nil
// 	}
// 	var keys []transform.AccountDataKerOfferOutput
// 	for _, k := range lkeys {
// 		keys = append(keys, transform.AccountDataKerOfferOutput{
// 			AccountID: k.AccountId.Address(),
// 			DataName:  string(k.DataName),
// 		})
// 	}
// 	data, err := q.GetAccountDataByKeys(ctx, keys)
// 	if err != nil {
// 		return errors.Wrap(err, "Error running transform.rOfferOutput.GetAccountDataByKeys")
// 	}

// 	for _, row := range data {
// 		entry := xdr.LedgerEntry{
// 			LastModifiedLedgerSeq: xdr.Uint32(row.LastModifiedLedger),
// 			Data: xdr.LedgerEntryData{
// 				Type: xdr.LedgerEntryTypeData,
// 				Data: &xdr.DataEntry{
// 					AccountId: xdr.MustAddress(row.AccountID),
// 					DataName:  xdr.String64(row.Name),
// 					DataValue: xdr.DataValue(row.Value),
// 				},
// 			},
// 		}
// 		addLedgerEntrySponsor(&entry, row.Sponsor)
// 		err := verifier.Write(entry)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

func addOffersToStateVerifier(
	ctx context.Context,
	verifier *StateVerifier,
	// q transform.IngestionrOfferOutput,
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
	verifier *StateVerifier,
	// assetStats transform.ClaimableBalanceOutput,
	// q transform.IngestionrOfferOutput,
	claims []transform.ClaimableBalanceOutput,
) error {
	if len(claims) == 0 {
		return nil
	}

	// var idStrings []string
	// e := xdr.NewEncodingBuffer()
	// for _, id := range claims {
	// 	idString, err := e.MarshalHex(id)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	idStrings = append(idStrings, idString)
	// }
	// cBalances, err := q.GetClaimableBalancesByID(ctx, idStrings)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running transform.rOfferOutput.GetClaimableBalancesByID")
	// }

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

		// if err := assetStats.AddClaimableBalance(
		// 	ingest.Change{
		// 		Post: &entry,
		// 	},
		// ); err != nil {
		// 	return ingest.NewStateError(
		// 		errors.Wrap(err, "could not add claimable balance to asset stats"),
		// 	)
		// }
	}

	return nil
}

func addLiquidityPoolsToStateVerifier(
	ctx context.Context,
	verifier *StateVerifier,
	// assetStats transform.AssetStatSet,
	// q transform.IngestionrOfferOutput,
	lPools []transform.PoolOutput,
) error {
	if len(lPools) == 0 {
		return nil
	}
	// var idsHex = make([]string, len(ids))
	// for i, id := range ids {
	// 	idsHex[i] = transform.PoolIDToString(id)

	// }
	// lPools, err := q.GetLiquidityPoolsByID(ctx, idsHex)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running transform.rOfferOutput.GetLiquidityPoolsByID")
	// }

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

		// if err := assetStats.AddLiquidityPool(
		// 	ingest.Change{
		// 		Post: &entry,
		// 	},
		// ); err != nil {
		// 	return ingest.NewStateError(
		// 		errors.Wrap(err, "could not add claimable balance to asset stats"),
		// 	)
		// }
	}

	return nil
}

func liquidityPoolToXDR(row transform.PoolOutput) (xdr.LiquidityPoolEntry, error) {
	// if len(row.AssetReserves) != 2 {
	// 	return xdr.LiquidityPoolEntry{}, fmt.Errorf("unexpected number of asset reserves (%d), expected %d", len(row.AssetReserves), 2)
	// }
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
	verifier *StateVerifier,
	// assetStats transform.AssetStatSet,
	// q transform.IngestionrOfferOutput,
	trusts []transform.TrustlineOutput,
) error {
	if len(trusts) == 0 {
		return nil
	}

	// var ledgerKeyStrings []string
	// for _, key := range keys {
	// 	var ledgerKey xdr.LedgerKey
	// 	if err := ledgerKey.SetTrustline(key.AccountId, key.Asset); err != nil {
	// 		return errors.Wrap(err, "Error running ledgerKey.SetTrustline")
	// 	}
	// 	b64, err := ledgerKey.MarshalBinaryBase64()
	// 	if err != nil {
	// 		return errors.Wrap(err, "Error running ledgerKey.MarshalBinaryBase64")
	// 	}
	// 	ledgerKeyStrings = append(ledgerKeyStrings, b64)
	// }

	// trustLines, err := q.GetTrustLinesByKeys(ctx, ledgerKeyStrings)
	// if err != nil {
	// 	return errors.Wrap(err, "Error running transform.rOfferOutput.GetTrustLinesByKeys")
	// }

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
		// if err = assetStats.AddTrustline(
		// 	ingest.Change{
		// 		Post: &entry,
		// 	},
		// ); err != nil {
		// 	return ingest.NewStateError(
		// 		errors.Wrap(err, "could not add trustline to asset stats"),
		// 	)
		// }
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
