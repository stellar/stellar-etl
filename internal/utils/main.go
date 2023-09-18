package utils

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/spf13/pflag"

	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

// PanicOnError is a function that panics if the provided error is not nil
func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

// HashToHexString is utility function that converts and xdr.Hash type to a hex string
func HashToHexString(inputHash xdr.Hash) string {
	sliceHash := inputHash[:]
	hexString := hex.EncodeToString(sliceHash)
	return hexString
}

// TimePointToUTCTimeStamp takes in an xdr TimePoint and converts it to a time.Time struct in UTC. It returns an error for negative timepoints
func TimePointToUTCTimeStamp(providedTime xdr.TimePoint) (time.Time, error) {
	intTime := int64(providedTime)
	if intTime < 0 {
		return time.Now(), errors.New("the timepoint is negative")
	}
	return time.Unix(intTime, 0).UTC(), nil
}

// GetAccountAddressFromMuxedAccount takes in a muxed account and returns the address of the account
func GetAccountAddressFromMuxedAccount(account xdr.MuxedAccount) (string, error) {
	providedID := account.ToAccountId()
	pointerToID := &providedID
	return pointerToID.GetAddress()
}

// CreateSampleTx creates a transaction with a single operation (BumpSequence), the min base fee, and infinite timebounds
func CreateSampleTx(sequence int64, operationCount int) xdr.TransactionEnvelope {
	kp, err := keypair.Random()
	PanicOnError(err)

	operations := []txnbuild.Operation{}
	operationType := &txnbuild.BumpSequence{
		BumpTo: 0,
	}
	for i := 0; i < operationCount; i++ {
		operations = append(operations, operationType)
	}

	sourceAccount := txnbuild.NewSimpleAccount(kp.Address(), int64(0))
	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &sourceAccount,
			Operations:    operations,
			BaseFee:       txnbuild.MinBaseFee,
			Preconditions: txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
		},
	)
	PanicOnError(err)

	env := tx.ToXDR()
	return env
}

// ConvertStroopValueToReal converts a value in stroops, the smallest amount unit, into real units
func ConvertStroopValueToReal(input xdr.Int64) float64 {
	output, _ := big.NewRat(int64(input), int64(10000000)).Float64()
	return output
}

// CreateSampleResultMeta creates Transaction results with the desired success flag and number of sub operation results
func CreateSampleResultMeta(successful bool, subOperationCount int) xdr.TransactionResultMeta {
	resultCode := xdr.TransactionResultCodeTxFailed
	if successful {
		resultCode = xdr.TransactionResultCodeTxSuccess
	}
	operationResults := []xdr.OperationResult{}
	operationResultTr := &xdr.OperationResultTr{
		Type: xdr.OperationTypeCreateAccount,
		CreateAccountResult: &xdr.CreateAccountResult{
			Code: 0,
		},
	}

	for i := 0; i < subOperationCount; i++ {
		operationResults = append(operationResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr:   operationResultTr,
		})
	}

	return xdr.TransactionResultMeta{
		Result: xdr.TransactionResultPair{
			Result: xdr.TransactionResult{
				Result: xdr.TransactionResultResult{
					Code:    resultCode,
					Results: &operationResults,
				},
			},
		},
	}
}

func CreateSampleResultPair(successful bool, subOperationCount int) xdr.TransactionResultPair {
	resultCode := xdr.TransactionResultCodeTxFailed
	if successful {
		resultCode = xdr.TransactionResultCodeTxSuccess
	}
	operationResults := []xdr.OperationResult{}
	operationResultTr := &xdr.OperationResultTr{
		Type: xdr.OperationTypeCreateAccount,
		CreateAccountResult: &xdr.CreateAccountResult{
			Code: 0,
		},
	}

	for i := 0; i < subOperationCount; i++ {
		operationResults = append(operationResults, xdr.OperationResult{
			Code: xdr.OperationResultCodeOpInner,
			Tr:   operationResultTr,
		})
	}

	return xdr.TransactionResultPair{
		Result: xdr.TransactionResult{
			Result: xdr.TransactionResultResult{
				Code:    resultCode,
				Results: &operationResults,
			},
		},
	}
}

func CreateSampleTxMeta(subOperationCount int, AssetA, AssetB xdr.Asset) *xdr.TransactionMetaV1 {
	operationMeta := []xdr.OperationMeta{}
	for i := 0; i < subOperationCount; i++ {
		operationMeta = append(operationMeta, xdr.OperationMeta{
			Changes: xdr.LedgerEntryChanges{},
		})
	}

	operationMeta = AddLPOperations(operationMeta, AssetA, AssetB)
	operationMeta = AddLPOperations(operationMeta, AssetA, AssetB)

	operationMeta = append(operationMeta, xdr.OperationMeta{
		Changes: xdr.LedgerEntryChanges{},
	})

	return &xdr.TransactionMetaV1{
		Operations: operationMeta,
	}
}

func AddLPOperations(txMeta []xdr.OperationMeta, AssetA, AssetB xdr.Asset) []xdr.OperationMeta {
	txMeta = append(txMeta, xdr.OperationMeta{
		Changes: xdr.LedgerEntryChanges{
			xdr.LedgerEntryChange{
				Type: xdr.LedgerEntryChangeTypeLedgerEntryState,
				State: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeLiquidityPool,
						LiquidityPool: &xdr.LiquidityPoolEntry{
							LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6, 7, 8, 9},
							Body: xdr.LiquidityPoolEntryBody{
								Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
								ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
									Params: xdr.LiquidityPoolConstantProductParameters{
										AssetA: AssetA,
										AssetB: AssetB,
										Fee:    30,
									},
									ReserveA:                 100000,
									ReserveB:                 1000,
									TotalPoolShares:          500,
									PoolSharesTrustLineCount: 25,
								},
							},
						},
					},
				},
			},
			xdr.LedgerEntryChange{
				Type: xdr.LedgerEntryChangeTypeLedgerEntryUpdated,
				Updated: &xdr.LedgerEntry{
					Data: xdr.LedgerEntryData{
						Type: xdr.LedgerEntryTypeLiquidityPool,
						LiquidityPool: &xdr.LiquidityPoolEntry{
							LiquidityPoolId: xdr.PoolId{1, 2, 3, 4, 5, 6, 7, 8, 9},
							Body: xdr.LiquidityPoolEntryBody{
								Type: xdr.LiquidityPoolTypeLiquidityPoolConstantProduct,
								ConstantProduct: &xdr.LiquidityPoolEntryConstantProduct{
									Params: xdr.LiquidityPoolConstantProductParameters{
										AssetA: AssetA,
										AssetB: AssetB,
										Fee:    30,
									},
									ReserveA:                 101000,
									ReserveB:                 1100,
									TotalPoolShares:          502,
									PoolSharesTrustLineCount: 26,
								},
							},
						},
					},
				},
			},
		}})

	return txMeta
}

// AddCommonFlags adds the flags common to all commands: end-ledger, stdout, and strict-export
func AddCommonFlags(flags *pflag.FlagSet) {
	flags.Uint32P("end-ledger", "e", 0, "The ledger sequence number for the end of the export range")
	flags.Bool("strict-export", false, "If set, transform errors will be fatal.")
	flags.Bool("testnet", false, "If set, will connect to Testnet instead of Mainnet.")
	flags.Bool("futurenet", false, "If set, will connect to Futurenet instead of Mainnet.")
	flags.StringToStringP("extra-fields", "u", map[string]string{}, "Additional fields to append to output jsons. Used for appending metadata")
}

// AddArchiveFlags adds the history archive specific flags: start-ledger, output, and limit
func AddArchiveFlags(objectName string, flags *pflag.FlagSet) {
	flags.Uint32P("start-ledger", "s", 2, "The ledger sequence number for the beginning of the export period. Defaults to genesis ledger")
	flags.StringP("output", "o", "exported_"+objectName+".txt", "Filename of the output file")
	flags.Int64P("limit", "l", -1, "Maximum number of "+objectName+" to export. If the limit is set to a negative number, all the objects in the provided range are exported")
}

// AddBucketFlags adds the bucket list specifc flags: output
func AddBucketFlags(objectName string, flags *pflag.FlagSet) {
	flags.StringP("output", "o", "exported_"+objectName+".txt", "Filename of the output file")
}

// AddGcsFlags adds the gcs-related flags: gcs-bucket, gcp-credentials
func AddGcsFlags(flags *pflag.FlagSet) {
	flags.String("gcs-bucket", "stellar-etl-cli", "GCS bucket to export to.")
	flags.StringP("gcp-credentials", "g", "", "Path to GOOGLE_APPLICATION_CREDENTIALS, service account json. Only used for local/dev purposes. "+
		"When run on GCP, credentials should be inferred by service account.")
}

// AddCoreFlags adds the captive core specific flags: core-executable, core-config, batch-size, and output flags
func AddCoreFlags(flags *pflag.FlagSet, defaultFolder string) {
	flags.StringP("core-executable", "x", "", "Filepath to the stellar-core executable")
	flags.StringP("core-config", "c", "", "Filepath to the config file for stellar-core")

	flags.Uint32P("batch-size", "b", 64, "number of ledgers to export changes from in each batches")
	flags.StringP("output", "o", defaultFolder, "Folder that will contain the output files")

	flags.Uint32P("start-ledger", "s", 2, "The ledger sequence number for the beginning of the export period. Defaults to genesis ledger")
}

// AddExportTypeFlags adds the captive core specifc flags: export-{type} flags
func AddExportTypeFlags(flags *pflag.FlagSet) {
	flags.BoolP("export-accounts", "a", false, "set in order to export account changes")
	flags.BoolP("export-trustlines", "t", false, "set in order to export trustline changes")
	flags.BoolP("export-offers", "f", false, "set in order to export offer changes")
	flags.BoolP("export-pools", "p", false, "set in order to export liquidity pool changes")
	flags.BoolP("export-balances", "l", false, "set in order to export claimable balance changes")
	flags.BoolP("export-contract-code", "", false, "set in order to export contract code changes")
	flags.BoolP("export-contract-data", "", false, "set in order to export contract data changes")
	flags.BoolP("export-config-settings", "", false, "set in order to export config settings changes")
	flags.BoolP("export-expiration", "", false, "set in order to export expiration changes")
}

// MustCommonFlags gets the values of the the flags common to all commands: end-ledger and strict-export. If any do not exist, it stops the program fatally using the logger
func MustCommonFlags(flags *pflag.FlagSet, logger *EtlLogger) (endNum uint32, strictExport, isTest bool, isFuture bool, extra map[string]string) {
	endNum, err := flags.GetUint32("end-ledger")
	if err != nil {
		logger.Fatal("could not get end sequence number: ", err)
	}

	strictExport, err = flags.GetBool("strict-export")
	if err != nil {
		logger.Fatal("could not get strict-export boolean: ", err)
	}

	isTest, err = flags.GetBool("testnet")
	if err != nil {
		logger.Fatal("could not get testnet boolean: ", err)
	}

	isFuture, err = flags.GetBool("futurenet")
	if err != nil {
		logger.Fatal("could not get futurenet boolean: ", err)
	}

	extra, err = flags.GetStringToString("extra-fields")
	if err != nil {
		logger.Fatal("could not get extra fields string: ", err)
	}
	return
}

// MustArchiveFlags gets the values of the the history archive specific flags: start-ledger, output, and limit
func MustArchiveFlags(flags *pflag.FlagSet, logger *EtlLogger) (startNum uint32, path string, limit int64) {
	startNum, err := flags.GetUint32("start-ledger")
	if err != nil {
		logger.Fatal("could not get start sequence number: ", err)
	}

	path, err = flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	limit, err = flags.GetInt64("limit")
	if err != nil {
		logger.Fatal("could not get limit: ", err)
	}

	return
}

// MustBucketFlags gets the values of the bucket list specific flags: output
func MustBucketFlags(flags *pflag.FlagSet, logger *EtlLogger) (path string) {
	path, err := flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	return
}

// MustGcsFlags gets the values of the bucket list specific flags: gcp-project and gcs-bucket
func MustGcsFlags(flags *pflag.FlagSet, logger *EtlLogger) (bucket, credentials string) {
	bucket, err := flags.GetString("gcs-bucket")
	if err != nil {
		logger.Fatal("could not get gcs bucket: ", err)
	}
	credentials, err = flags.GetString("gcp-credentials")
	if err != nil {
		logger.Fatal("could not get GOOGLE_APPLICATION_CREDENTIALS file: ", err)
	}
	return
}

// MustCoreFlags gets the values for the core-executable, core-config, start ledger batch-size, and output flags. If any do not exist, it stops the program fatally using the logger
func MustCoreFlags(flags *pflag.FlagSet, logger *EtlLogger) (execPath, configPath string, startNum, batchSize uint32, path string) {
	execPath, err := flags.GetString("core-executable")
	if err != nil {
		logger.Fatal("could not get path to stellar-core executable, which is mandatory when not starting at the genesis ledger (ledger 1): ", err)
	}

	configPath, err = flags.GetString("core-config")
	if err != nil {
		logger.Fatal("could not get path to stellar-core config file, is mandatory when not starting at the genesis ledger (ledger 1): ", err)
	}

	path, err = flags.GetString("output")
	if err != nil {
		logger.Fatal("could not get output filename: ", err)
	}

	startNum, err = flags.GetUint32("start-ledger")
	if err != nil {
		logger.Fatal("could not get start sequence number: ", err)
	}

	batchSize, err = flags.GetUint32("batch-size")
	if err != nil {
		logger.Fatal("could not get batch size: ", err)
	}

	return
}

// MustExportTypeFlags gets the values for the export-accounts, export-offers, and export-trustlines flags. If any do not exist, it stops the program fatally using the logger
// func MustExportTypeFlags(flags *pflag.FlagSet, logger *EtlLogger) (exportAccounts, exportOffers, exportTrustlines, exportPools, exportBalances, exportContractCode, exportContractData, exportConfigSettings, exportExpiration bool) {
func MustExportTypeFlags(flags *pflag.FlagSet, logger *EtlLogger) map[string]bool {
	var err error
	exports := map[string]bool{
		"export-accounts":        false,
		"export-trustlines":      false,
		"export-offers":          false,
		"export-pools":           false,
		"export-balances":        false,
		"export-contract-code":   false,
		"export-contract-data":   false,
		"export-config-settings": false,
		"export-expiration":      false,
	}

	for export_name, _ := range exports {
		exports[export_name], err = flags.GetBool(export_name)
		if err != nil {
			logger.Fatalf("could not get %s flag: %v", export_name, err)
		}
	}

	return exports
}

type historyArchiveBackend struct {
	client  historyarchive.ArchiveInterface
	ledgers map[uint32]*historyarchive.Ledger
}

func (h historyArchiveBackend) GetLatestLedgerSequence(ctx context.Context) (sequence uint32, err error) {
	root, err := h.client.GetRootHAS()
	if err != nil {
		return 0, err
	}
	return root.CurrentLedger, nil
}

func (h historyArchiveBackend) GetLedgers(ctx context.Context) (map[uint32]*historyarchive.Ledger, error) {

	return h.ledgers, nil
}

func (h historyArchiveBackend) GetLedgerArchive(ctx context.Context, sequence uint32) (historyarchive.Ledger, error) {
	ledger, ok := h.ledgers[sequence]
	if !ok {
		return historyarchive.Ledger{}, fmt.Errorf("ledger %d is missing from map", sequence)
	}

	historyLedger := historyarchive.Ledger{
		Header:            ledger.Header,
		Transaction:       ledger.Transaction,
		TransactionResult: ledger.TransactionResult,
	}

	return historyLedger, nil
}

func (h historyArchiveBackend) GetLedger(ctx context.Context, sequence uint32) (xdr.LedgerCloseMeta, error) {
	ledger, ok := h.ledgers[sequence]
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("ledger %d is missing from map", sequence)
	}

	lcm := xdr.LedgerCloseMeta{
		V: 0,
		V0: &xdr.LedgerCloseMetaV0{
			LedgerHeader: ledger.Header,
			TxSet:        ledger.Transaction.TxSet,
		},
	}
	lcm.V0.TxProcessing = make([]xdr.TransactionResultMeta, len(ledger.TransactionResult.TxResultSet.Results))
	for i, result := range ledger.TransactionResult.TxResultSet.Results {
		lcm.V0.TxProcessing[i].Result = result
	}

	return lcm, nil
}

func (h historyArchiveBackend) PrepareRange(ctx context.Context, ledgerRange ledgerbackend.Range) error {
	return nil
}

func (h historyArchiveBackend) IsPrepared(ctx context.Context, ledgerRange ledgerbackend.Range) (bool, error) {
	return true, nil
}

func (h historyArchiveBackend) Close() error {
	return nil
}

// ValidateLedgerRange validates the given ledger range
func ValidateLedgerRange(start, end, latestNum uint32) error {
	if start == 0 {
		return fmt.Errorf("start sequence number equal to 0. There is no ledger 0 (genesis ledger is ledger 1)")
	}

	if end == 0 {
		return fmt.Errorf("end sequence number equal to 0. There is no ledger 0 (genesis ledger is ledger 1)")
	}

	if end < start {
		return fmt.Errorf("end sequence number is less than start (%d < %d)", end, start)
	}

	if latestNum < start {
		return fmt.Errorf("latest sequence number is less than start sequence number (%d < %d)", latestNum, start)
	}

	if latestNum < end {
		return fmt.Errorf("latest sequence number is less than end sequence number (%d < %d)", latestNum, end)
	}

	return nil
}

func CreateBackend(start, end uint32, archiveURLs []string) (historyArchiveBackend, error) {
	client, err := CreateHistoryArchiveClient(archiveURLs)
	if err != nil {
		return historyArchiveBackend{}, err
	}

	root, err := client.GetRootHAS()
	if err != nil {
		return historyArchiveBackend{}, err
	}
	if err = ValidateLedgerRange(start, end, root.CurrentLedger); err != nil {
		return historyArchiveBackend{}, err
	}

	ledgers, err := client.GetLedgers(start, end)
	if err != nil {
		return historyArchiveBackend{}, err
	}
	return historyArchiveBackend{client: client, ledgers: ledgers}, nil
}

// mainnet history archive URLs
var mainArchiveURLs = []string{
	"https://history.stellar.org/prd/core-live/core_live_001",
	"https://history.stellar.org/prd/core-live/core_live_002",
	//"https://history.stellar.org/prd/core-live/core_live_003",
}

// testnet is only used for local testing with new Protocol features
var testArchiveURLs = []string{
	"https://history.stellar.org/prd/core-testnet/core_testnet_001",
	"https://history.stellar.org/prd/core-testnet/core_testnet_002",
	"https://history.stellar.org/prd/core-testnet/core_testnet_003",
}

// futrenet is used for testing new Protocol features
var futureArchiveURLs = []string{
	"https://history-futurenet.stellar.org/",
}

func CreateHistoryArchiveClient(archiveURLS []string) (historyarchive.ArchiveInterface, error) {
	return historyarchive.NewArchivePool(archiveURLS, historyarchive.ConnectOptions{})
}

// GetLatestLedgerSequence returns the latest ledger sequence
func GetLatestLedgerSequence(archiveURLs []string) (uint32, error) {
	client, err := CreateHistoryArchiveClient(archiveURLs)
	if err != nil {
		return 0, err
	}

	root, err := client.GetRootHAS()
	if err != nil {
		return 0, err
	}

	return root.CurrentLedger, nil
}

// GetCheckpointNum gets the ledger sequence number of the checkpoint containing the provided ledger. If the checkpoint does not exist, an error is returned
func GetCheckpointNum(seq, maxSeq uint32) (uint32, error) {
	/*
		Checkpoints are made "every 64 ledgers", when LCL is one-less-than a multiple
		of 64. In other words, at LCL=63, 127, 191, 255, etc. or in other other words
		checkpoint K covers the inclusive ledger range [K*64, ((K+1)*64)-1], and each
		of those ranges should contain exactly 64 ledgers, with the exception of the
		first checkpoint, which has only 63 ledgers: there is no ledger 0.
	*/
	remainder := (seq + 1) % 64
	if remainder == 0 {
		return seq, nil
	}

	checkpoint := seq + 64 - remainder
	if checkpoint > maxSeq {
		return 0, fmt.Errorf("the checkpoint ledger %d is greater than the max ledger number %d", checkpoint, maxSeq)
	}

	return checkpoint, nil
}

// ExtractLedgerCloseTime gets the close time of the provided ledger
func ExtractLedgerCloseTime(ledger xdr.LedgerHeaderHistoryEntry) (time.Time, error) {
	return TimePointToUTCTimeStamp(ledger.Header.ScpValue.CloseTime)
}

// ExtractEntryFromChange gets the most recent state of an entry from an ingestio change, as well as if the entry was deleted
func ExtractEntryFromChange(change ingest.Change) (xdr.LedgerEntry, xdr.LedgerEntryChangeType, bool, error) {
	switch changeType := change.LedgerEntryChangeType(); changeType {
	case xdr.LedgerEntryChangeTypeLedgerEntryCreated, xdr.LedgerEntryChangeTypeLedgerEntryUpdated:
		return *change.Post, changeType, false, nil
	case xdr.LedgerEntryChangeTypeLedgerEntryRemoved:
		return *change.Pre, changeType, true, nil
	default:
		return xdr.LedgerEntry{}, changeType, false, fmt.Errorf("unable to extract ledger entry type from change")
	}
}

// GetMostRecentCheckpoint returns the most recent checkpoint before the provided ledger
func GetMostRecentCheckpoint(seq uint32) uint32 {
	remainder := (seq + 1) % 64
	if remainder == 0 {
		return seq
	}
	return seq - remainder
}

type EnvironmentDetails struct {
	NetworkPassphrase string
	ArchiveURLs       []string
	BinaryPath        string
	CoreConfig        string
}

// GetPassphrase returns the correct Network Passphrase based on env preference
func GetEnvironmentDetails(isTest bool, isFuture bool) (details EnvironmentDetails) {
	if isTest {
		// testnet passphrase to be used for testing
		details.NetworkPassphrase = network.TestNetworkPassphrase
		details.ArchiveURLs = testArchiveURLs
		details.BinaryPath = "/usr/bin/stellar-core"
		details.CoreConfig = "docker/stellar-core_testnet.cfg"
		return details
	} else if isFuture {
		// details.NetworkPassphrase = network.FutureNetworkPassphrase
		details.NetworkPassphrase = "Test SDF Future Network ; October 2022"
		details.ArchiveURLs = futureArchiveURLs
		details.BinaryPath = "/usr/bin/stellar-core"
		details.CoreConfig = "docker/stellar-core_futurenet.cfg"
		return details
	} else {
		// default: mainnet
		details.NetworkPassphrase = network.PublicNetworkPassphrase
		details.ArchiveURLs = mainArchiveURLs
		details.BinaryPath = "/usr/bin/stellar-core"
		details.CoreConfig = "docker/stellar-core.cfg"
		return details
	}
}

type CaptiveCore interface {
	CreateCaptiveCoreBackend() (ledgerbackend.CaptiveStellarCore, error)
}

func (e EnvironmentDetails) CreateCaptiveCoreBackend() (*ledgerbackend.CaptiveStellarCore, error) {
	captiveCoreToml, err := ledgerbackend.NewCaptiveCoreTomlFromFile(
		e.CoreConfig,
		ledgerbackend.CaptiveCoreTomlParams{
			NetworkPassphrase:  e.NetworkPassphrase,
			HistoryArchiveURLs: e.ArchiveURLs,
			Strict:             true,
			UseDB:              true,
		},
	)
	if err != nil {
		return &ledgerbackend.CaptiveStellarCore{}, err
	}
	backend, err := ledgerbackend.NewCaptive(
		ledgerbackend.CaptiveCoreConfig{
			BinaryPath:         e.BinaryPath,
			Toml:               captiveCoreToml,
			NetworkPassphrase:  e.NetworkPassphrase,
			HistoryArchiveURLs: e.ArchiveURLs,
			UseDB:              true,
		},
	)
	return backend, err
}

func (e EnvironmentDetails) GetUnboundedLedgerCloseMeta(end uint32) (xdr.LedgerCloseMeta, error) {
	ctx := context.Background()

	backend, err := e.CreateCaptiveCoreBackend()

	ledgerRange := ledgerbackend.UnboundedRange(end)

	err = backend.PrepareRange(ctx, ledgerRange)
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	ledgerCloseMeta, err := backend.GetLedger(ctx, end)
	if err != nil {
		return xdr.LedgerCloseMeta{}, err
	}

	return ledgerCloseMeta, nil
}

func GetCloseTime(lcm xdr.LedgerCloseMeta) (time.Time, error) {
	switch lcm.V {
	case 0:
		return ExtractLedgerCloseTime(lcm.MustV0().LedgerHeader)
	default:
		panic(fmt.Sprintf("Unsupported LedgerCloseMeta.V: %d", lcm.V))
	}
}
