package transform

// LedgerOutputParquet is a representation of a ledger that aligns with the BigQuery table history_ledgers
type LedgerOutputParquet struct {
	Sequence                   int64  `parquet:"name=sequence, type=INT64, convertedtype=UINT_64"`
	LedgerHash                 string `parquet:"name=ledger_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PreviousLedgerHash         string `parquet:"name=previous_ledger_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LedgerHeader               string `parquet:"name=ledger_header, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionCount           int32  `parquet:"name=transaction_count, type=INT32"`
	OperationCount             int32  `parquet:"name=operation_count, type=INT32"`
	SuccessfulTransactionCount int32  `parquet:"name=successful_transaction_count, type=INT32"`
	FailedTransactionCount     int32  `parquet:"name=failed_transaction_count, type=INT32"`
	TxSetOperationCount        string `parquet:"name=tx_set_operation_count, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ClosedAt                   int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TotalCoins                 int64  `parquet:"name=total_coins, type=INT64"`
	FeePool                    int64  `parquet:"name=fee_pool, type=INT64"`
	BaseFee                    int64  `parquet:"name=base_fee, type=INT64, convertedtype=UINT_64"`
	BaseReserve                int64  `parquet:"name=base_reserve, type=INT64, convertedtype=UINT_64"`
	MaxTxSetSize               int64  `parquet:"name=max_tx_set_size, type=INT64, convertedtype=UINT_64"`
	ProtocolVersion            int64  `parquet:"name=protocol_version, type=INT64, convertedtype=UINT_64"`
	LedgerID                   int64  `parquet:"name=id, type=INT64"`
	SorobanFeeWrite1Kb         int64  `parquet:"name=soroban_fee_write_1kb, type=INT64"`
	NodeID                     string `parquet:"name=node_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Signature                  string `parquet:"name=signature, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TotalByteSizeOfBucketList  int64  `parquet:"name=total_byte_size_of_bucket_list, type=INT64, convertedtype=UINT_64"`
}

// TransactionOutputParquet is a representation of a transaction that aligns with the BigQuery table history_transactions
type TransactionOutputParquet struct {
	TransactionHash                      string   `parquet:"name=transaction_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LedgerSequence                       int64    `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
	Account                              string   `parquet:"name=account, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AccountMuxed                         string   `parquet:"name=account_muxed, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AccountSequence                      int64    `parquet:"name=account_sequence, type=INT64"`
	MaxFee                               int64    `parquet:"name=max_fee, type=INT64, convertedtype=UINT_64"`
	FeeCharged                           int64    `parquet:"name=fee_charged, type=INT64"`
	OperationCount                       int32    `parquet:"name=operation_count, type=INT32"`
	TxEnvelope                           string   `parquet:"name=tx_envelope, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TxResult                             string   `parquet:"name=tx_result, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TxMeta                               string   `parquet:"name=tx_meta, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TxFeeMeta                            string   `parquet:"name=tx_fee_meta, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CreatedAt                            int64    `parquet:"name=created_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MemoType                             string   `parquet:"name=memo_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Memo                                 string   `parquet:"name=memo, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TimeBounds                           string   `parquet:"name=time_bounds, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Successful                           bool     `parquet:"name=successful, type=BOOLEAN"`
	TransactionID                        int64    `parquet:"name=id, type=INT64"`
	FeeAccount                           string   `parquet:"name=fee_account, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	FeeAccountMuxed                      string   `parquet:"name=fee_account_muxed, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	InnerTransactionHash                 string   `parquet:"name=inner_transaction_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	NewMaxFee                            int64    `parquet:"name=new_max_fee, type=INT64, convertedtype=UINT_64"`
	LedgerBounds                         string   `parquet:"name=ledger_bounds, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MinAccountSequence                   int64    `parquet:"name=min_account_sequence, type=INT64"`
	MinAccountSequenceAge                int64    `parquet:"name=min_account_sequence_age, type=INT64"`
	MinAccountSequenceLedgerGap          int64    `parquet:"name=min_account_sequence_ledger_gap, type=INT64"`
	ExtraSigners                         []string `parquet:"name=extra_signers, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	ClosedAt                             int64    `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	ResourceFee                          int64    `parquet:"name=resource_fee, type=INT64"`
	SorobanResourcesInstructions         int64    `parquet:"name=soroban_resources_instructions, type=INT64, convertedtype=UINT_64"`
	SorobanResourcesReadBytes            int64    `parquet:"name=soroban_resources_read_bytes, type=INT64, convertedtype=UINT_64"`
	SorobanResourcesWriteBytes           int64    `parquet:"name=soroban_resources_write_bytes, type=INT64, convertedtype=UINT_64"`
	TransactionResultCode                string   `parquet:"name=transaction_result_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	InclusionFeeBid                      int64    `parquet:"name=inclusion_fee_bid, type=INT64"`
	InclusionFeeCharged                  int64    `parquet:"name=inclusion_fee_charged, type=INT64"`
	ResourceFeeRefund                    int64    `parquet:"name=resource_fee_refund, type=INT64"`
	TotalNonRefundableResourceFeeCharged int64    `parquet:"name=non_refundable_resource_fee_charged, type=INT64"`
	TotalRefundableResourceFeeCharged    int64    `parquet:"name=refundable_resource_fee_charged, type=INT64"`
	RentFeeCharged                       int64    `parquet:"name=rent_fee_charged, type=INT64"`
}

// AccountOutputParquet is a representation of an account that aligns with the BigQuery table accounts
type AccountOutputParquet struct {
	AccountID            string  `parquet:"name=account_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Balance              float64 `parquet:"name=balance, type=DOUBLE"`
	BuyingLiabilities    float64 `parquet:"name=buying_liabilities, type=DOUBLE"`
	SellingLiabilities   float64 `parquet:"name=selling_liabilities, type=DOUBLE"`
	SequenceNumber       int64   `parquet:"name=sequence_number, type=INT64"`
	SequenceLedger       int64   `parquet:"name=sequence_ledger, type=INT64"`
	SequenceTime         int64   `parquet:"name=sequence_time, type=INT64"`
	NumSubentries        int64   `parquet:"name=num_subentries, type=INT64, convertedtype=UINT_64"`
	InflationDestination string  `parquet:"name=inflation_destination, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Flags                int64   `parquet:"name=flags, type=INT64, convertedtype=UINT_64"`
	HomeDomain           string  `parquet:"name=home_domain, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MasterWeight         int32   `parquet:"name=master_weight, type=INT32"`
	ThresholdLow         int32   `parquet:"name=threshold_low, type=INT32"`
	ThresholdMedium      int32   `parquet:"name=threshold_medium, type=INT32"`
	ThresholdHigh        int32   `parquet:"name=threshold_high, type=INT32"`
	Sponsor              string  `parquet:"name=sponsor, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	NumSponsored         int64   `parquet:"name=num_sponsored, type=INT64, convertedtype=UINT_64"`
	NumSponsoring        int64   `parquet:"name=num_sponsoring, type=INT64, convertedtype=UINT_64"`
	LastModifiedLedger   int64   `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange    int64   `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Deleted              bool    `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt             int64   `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence       int64   `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
}

// AccountSignerOutputParquet is a representation of an account signer that aligns with the BigQuery table account_signers
type AccountSignerOutputParquet struct {
	AccountID          string `parquet:"name=account_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Signer             string `parquet:"name=signer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Weight             int32  `parquet:"name=weight, type=INT32"`
	Sponsor            string `parquet:"name=sponsor, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LastModifiedLedger int64  `parquet:"name=last_modified_ledger, type=INT64, convertedtype=INT64, convertedtype=UINT_64"`
	LedgerEntryChange  int64  `parquet:"name=ledger_entry_change, type=INT64, convertedtype=INT64, convertedtype=UINT_64"`
	Deleted            bool   `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt           int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence     int64  `parquet:"name=ledger_sequence, type=INT64, convertedtype=INT64, convertedtype=UINT_64"`
}

// OperationOutputParquet is a representation of an operation that aligns with the BigQuery table history_operations
type OperationOutputParquet struct {
	SourceAccount       string `parquet:"name=source_account, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SourceAccountMuxed  string `parquet:"name=source_account_muxed, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Type                int32  `parquet:"name=type, type=INT32"`
	TypeString          string `parquet:"name=type_string, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OperationDetails    string `parquet:"name=details, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionID       int64  `parquet:"name=transaction_id, type=INT64"`
	OperationID         int64  `parquet:"name=id, type=INT64"`
	ClosedAt            int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	OperationResultCode string `parquet:"name=operation_result_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OperationTraceCode  string `parquet:"name=operation_trace_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LedgerSequence      int64  `parquet:"name=ledger_sequence, type=INT64, convertedtype=INT64, convertedtype=UINT_64"`
}

//// Skipping ClaimableBalanceOutputParquet because it is not needed in the current scope of work
//// Note that ClaimableBalanceOutputParquet uses nested structs that will need to be handled
//// for parquet conversion
//type ClaimableBalanceOutputParquet struct {
//}

// PoolOutputParquet is a representation of a liquidity pool that aligns with the Bigquery table liquidity_pools
type PoolOutputParquet struct {
	PoolID             string  `parquet:"name=liquidity_pool_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PoolType           string  `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PoolFee            int64   `parquet:"name=fee, type=INT64, convertedtype=UINT_64"`
	TrustlineCount     int64   `parquet:"name=trustline_count, type=INT64, convertedtype=UINT_64"`
	PoolShareCount     float64 `parquet:"name=pool_share_count, type=DOUBLE"`
	AssetAType         string  `parquet:"name=asset_a_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetACode         string  `parquet:"name=asset_a_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetAIssuer       string  `parquet:"name=asset_a_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetAReserve      float64 `parquet:"name=asset_a_amount, type=DOUBLE"`
	AssetAID           int64   `parquet:"name=asset_a_id, type=INT64"`
	AssetBType         string  `parquet:"name=asset_b_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetBCode         string  `parquet:"name=asset_b_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetBIssuer       string  `parquet:"name=asset_b_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetBReserve      float64 `parquet:"name=asset_b_amount, type=DOUBLE"`
	AssetBID           int64   `parquet:"name=asset_b_id, type=INT64"`
	LastModifiedLedger int64   `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange  int64   `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Deleted            bool    `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt           int64   `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence     int64   `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
}

// AssetOutputParquet is a representation of an asset that aligns with the BigQuery table history_assets
type AssetOutputParquet struct {
	AssetCode      string `parquet:"name=asset_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetIssuer    string `parquet:"name=asset_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType      string `parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetID        int64  `parquet:"name=asset_id, type=INT64"`
	ClosedAt       int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence int64  `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
}

// TrustlineOutputParquet is a representation of a trustline that aligns with the BigQuery table trust_lines
type TrustlineOutputParquet struct {
	LedgerKey          string  `parquet:"name=ledger_key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AccountID          string  `parquet:"name=account_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetCode          string  `parquet:"name=asset_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetIssuer        string  `parquet:"name=asset_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType          string  `parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetID            int64   `parquet:"name=asset_id, type=INT64"`
	Balance            float64 `parquet:"name=balance, type=DOUBLE"`
	TrustlineLimit     int64   `parquet:"name=trust_line_limit, type=INT64"`
	LiquidityPoolID    string  `parquet:"name=liquidity_pool_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingLiabilities  float64 `parquet:"name=buying_liabilities, type=DOUBLE"`
	SellingLiabilities float64 `parquet:"name=selling_liabilities, type=DOUBLE"`
	Flags              int64   `parquet:"name=flags, type=INT64, convertedtype=UINT_64"`
	LastModifiedLedger int64   `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange  int64   `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Sponsor            string  `parquet:"name=sponsor, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Deleted            bool    `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt           int64   `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence     int64   `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
}

// OfferOutputParquet is a representation of an offer that aligns with the BigQuery table offers
type OfferOutputParquet struct {
	SellerID           string  `parquet:"name=seller_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OfferID            int64   `parquet:"name=offer_id, type=INT64"`
	SellingAssetType   string  `parquet:"name=selling_asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellingAssetCode   string  `parquet:"name=selling_asset_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellingAssetIssuer string  `parquet:"name=selling_asset_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellingAssetID     int64   `parquet:"name=selling_asset_id, type=INT64"`
	BuyingAssetType    string  `parquet:"name=buying_asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingAssetCode    string  `parquet:"name=buying_asset_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingAssetIssuer  string  `parquet:"name=buying_asset_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingAssetID      int64   `parquet:"name=buying_asset_id, type=INT64"`
	Amount             float64 `parquet:"name=amount, type=DOUBLE"`
	PriceN             int32   `parquet:"name=pricen, type=INT32"`
	PriceD             int32   `parquet:"name=priced, type=INT32"`
	Price              float64 `parquet:"name=price, type=DOUBLE"`
	Flags              int64   `parquet:"name=flags, type=INT64, convertedtype=UINT_64"`
	LastModifiedLedger int64   `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange  int64   `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Deleted            bool    `parquet:"name=deleted, type=BOOLEAN"`
	Sponsor            string  `parquet:"name=sponsor, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ClosedAt           int64   `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence     int64   `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
}

// TradeOutputParquet is a representation of a trade that aligns with the BigQuery table history_trades
type TradeOutputParquet struct {
	Order                  int32   `parquet:"name=order, type=INT32"`
	LedgerClosedAt         int64   `parquet:"name=ledger_closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	SellingAccountAddress  string  `parquet:"name=selling_account_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellingAssetCode       string  `parquet:"name=selling_asset_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellingAssetIssuer     string  `parquet:"name=selling_asset_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellingAssetType       string  `parquet:"name=selling_asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SellingAssetID         int64   `parquet:"name=selling_asset_id, type=INT64"`
	SellingAmount          float64 `parquet:"name=selling_amount, type=DOUBLE"`
	BuyingAccountAddress   string  `parquet:"name=buying_account_address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingAssetCode        string  `parquet:"name=buying_asset_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingAssetIssuer      string  `parquet:"name=buying_asset_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingAssetType        string  `parquet:"name=buying_asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BuyingAssetID          int64   `parquet:"name=buying_asset_id, type=INT64"`
	BuyingAmount           float64 `parquet:"name=buying_amount, type=DOUBLE"`
	PriceN                 int64   `parquet:"name=price_n, type=INT64"`
	PriceD                 int64   `parquet:"name=price_d, type=INT64"`
	SellingOfferID         int64   `parquet:"name=selling_offer_id, type=INT64"`
	BuyingOfferID          int64   `parquet:"name=buying_offer_id, type=INT64"`
	SellingLiquidityPoolID string  `parquet:"name=selling_liquidity_pool_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LiquidityPoolFee       int64   `parquet:"name=liquidity_pool_fee, type=INT64"`
	HistoryOperationID     int64   `parquet:"name=history_operation_id, type=INT64"`
	TradeType              int32   `parquet:"name=trade_type, type=INT32"`
	RoundingSlippage       int64   `parquet:"name=rounding_slippage, type=INT64"`
	SellerIsExact          bool    `parquet:"name=seller_is_exact, type=BOOLEAN"`
}

// EffectOutputParquet is a representation of an operation that aligns with the BigQuery table history_effects
type EffectOutputParquet struct {
	Address        string `parquet:"name=address, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AddressMuxed   string `parquet:"name=address_muxed, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	OperationID    int64  `parquet:"name=operation_id, type=INT64"`
	Details        string `parquet:"name=details, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Type           int32  `parquet:"name=type, type=INT32"`
	TypeString     string `parquet:"name=type_string, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LedgerClosed   int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence int64  `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
	EffectIndex    int64  `parquet:"name=index, type=INT64, convertedtype=UINT_64"`
	EffectId       string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// ContractDataOutputParquet is a representation of contract data that aligns with the Bigquery table soroban_contract_data
type ContractDataOutputParquet struct {
	ContractId                string            `parquet:"name=contract_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractKeyType           string            `parquet:"name=contract_key_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractDurability        string            `parquet:"name=contract_durability, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractDataAssetCode     string            `parquet:"name=asset_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractDataAssetIssuer   string            `parquet:"name=asset_issuer, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractDataAssetType     string            `parquet:"name=asset_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractDataBalanceHolder string            `parquet:"name=balance_holder, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractDataBalance       string            `parquet:"name=balance, type=BYTE_ARRAY, convertedtype=UTF8"`
	LastModifiedLedger        int64             `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange         int64             `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Deleted                   bool              `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt                  int64             `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence            int64             `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
	LedgerKeyHash             string            `parquet:"name=ledger_key_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Key                       map[string]string `parquet:"name=key, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRING"`
	KeyDecoded                map[string]string `parquet:"name=key_decoded, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRING"`
	Val                       map[string]string `parquet:"name=val, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRING"`
	ValDecoded                map[string]string `parquet:"name=val_decoded, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRING"`
	ContractDataXDR           string            `parquet:"name=contract_data_xdr, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// ContractCodeOutputParquet is a representation of contract code that aligns with the Bigquery table soroban_contract_code
type ContractCodeOutputParquet struct {
	ContractCodeHash   string `parquet:"name=contract_code_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractCodeExtV   int32  `parquet:"name=contract_code_ext_v, type=INT32"`
	LastModifiedLedger int64  `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange  int64  `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Deleted            bool   `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt           int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence     int64  `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
	LedgerKeyHash      string `parquet:"name=ledger_key_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	NInstructions      int64  `parquet:"name=n_instructions, type=INT64, convertedtype=UINT_64"`
	NFunctions         int64  `parquet:"name=n_functions, type=INT64, convertedtype=UINT_64"`
	NGlobals           int64  `parquet:"name=n_globals, type=INT64, convertedtype=UINT_64"`
	NTableEntries      int64  `parquet:"name=n_table_entries, type=INT64, convertedtype=UINT_64"`
	NTypes             int64  `parquet:"name=n_types, type=INT64, convertedtype=UINT_64"`
	NDataSegments      int64  `parquet:"name=n_data_segments, type=INT64, convertedtype=UINT_64"`
	NElemSegments      int64  `parquet:"name=n_elem_segments, type=INT64, convertedtype=UINT_64"`
	NImports           int64  `parquet:"name=n_imports, type=INT64, convertedtype=UINT_64"`
	NExports           int64  `parquet:"name=n_exports, type=INT64, convertedtype=UINT_64"`
	NDataSegmentBytes  int64  `parquet:"name=n_data_segment_bytes, type=INT64, convertedtype=UINT_64"`
}

// ConfigSettingOutputParquet is a representation of soroban config settings that aligns with the Bigquery table config_settings
type ConfigSettingOutputParquet struct {
	ConfigSettingId                 int32   `parquet:"name=config_setting_id, type=INT32"`
	ContractMaxSizeBytes            int64   `parquet:"name=contract_max_size_bytes, type=INT64, convertedtype=UINT_64"`
	LedgerMaxInstructions           int64   `parquet:"name=ledger_max_instructions, type=INT64"`
	TxMaxInstructions               int64   `parquet:"name=tx_max_instructions, type=INT64"`
	FeeRatePerInstructionsIncrement int64   `parquet:"name=fee_rate_per_instructions_increment, type=INT64"`
	TxMemoryLimit                   int64   `parquet:"name=tx_memory_limit, type=INT64, convertedtype=UINT_64"`
	LedgerMaxReadLedgerEntries      int64   `parquet:"name=ledger_max_read_ledger_entries, type=INT64, convertedtype=UINT_64"`
	LedgerMaxReadBytes              int64   `parquet:"name=ledger_max_read_bytes, type=INT64, convertedtype=UINT_64"`
	LedgerMaxWriteLedgerEntries     int64   `parquet:"name=ledger_max_write_ledger_entries, type=INT64, convertedtype=UINT_64"`
	LedgerMaxWriteBytes             int64   `parquet:"name=ledger_max_write_bytes, type=INT64, convertedtype=UINT_64"`
	TxMaxReadLedgerEntries          int64   `parquet:"name=tx_max_read_ledger_entries, type=INT64, convertedtype=UINT_64"`
	TxMaxReadBytes                  int64   `parquet:"name=tx_max_read_bytes, type=INT64, convertedtype=UINT_64"`
	TxMaxWriteLedgerEntries         int64   `parquet:"name=tx_max_write_ledger_entries, type=INT64, convertedtype=UINT_64"`
	TxMaxWriteBytes                 int64   `parquet:"name=tx_max_write_bytes, type=INT64, convertedtype=UINT_64"`
	FeeReadLedgerEntry              int64   `parquet:"name=fee_read_ledger_entry, type=INT64"`
	FeeWriteLedgerEntry             int64   `parquet:"name=fee_write_ledger_entry, type=INT64"`
	FeeRead1Kb                      int64   `parquet:"name=fee_read_1kb, type=INT64"`
	BucketListTargetSizeBytes       int64   `parquet:"name=bucket_list_target_size_bytes, type=INT64"`
	WriteFee1KbBucketListLow        int64   `parquet:"name=write_fee_1kb_bucket_list_low, type=INT64"`
	WriteFee1KbBucketListHigh       int64   `parquet:"name=write_fee_1kb_bucket_list_high, type=INT64"`
	BucketListWriteFeeGrowthFactor  int64   `parquet:"name=bucket_list_write_fee_growth_factor, type=INT64, convertedtype=UINT_64"`
	FeeHistorical1Kb                int64   `parquet:"name=fee_historical_1kb, type=INT64"`
	TxMaxContractEventsSizeBytes    int64   `parquet:"name=tx_max_contract_events_size_bytes, type=INT64, convertedtype=UINT_64"`
	FeeContractEvents1Kb            int64   `parquet:"name=fee_contract_events_1kb, type=INT64"`
	LedgerMaxTxsSizeBytes           int64   `parquet:"name=ledger_max_txs_size_bytes, type=INT64, convertedtype=UINT_64"`
	TxMaxSizeBytes                  int64   `parquet:"name=tx_max_size_bytes, type=INT64, convertedtype=UINT_64"`
	FeeTxSize1Kb                    int64   `parquet:"name=fee_tx_size_1kb, type=INT64"`
	ContractCostParamsCpuInsns      string  `parquet:"name=contract_cost_params_cpu_insns, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractCostParamsMemBytes      string  `parquet:"name=contract_cost_params_mem_bytes, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractDataKeySizeBytes        int64   `parquet:"name=contract_data_key_size_bytes, type=INT64, convertedtype=UINT_64"`
	ContractDataEntrySizeBytes      int64   `parquet:"name=contract_data_entry_size_bytes, type=INT64, convertedtype=UINT_64"`
	MaxEntryTtl                     int64   `parquet:"name=max_entry_ttl, type=INT64, convertedtype=UINT_64"`
	MinTemporaryTtl                 int64   `parquet:"name=min_temporary_ttl, type=INT64, convertedtype=UINT_64"`
	MinPersistentTtl                int64   `parquet:"name=min_persistent_ttl, type=INT64, convertedtype=UINT_64"`
	AutoBumpLedgers                 int64   `parquet:"name=auto_bump_ledgers, type=INT64, convertedtype=UINT_64"`
	PersistentRentRateDenominator   int64   `parquet:"name=persistent_rent_rate_denominator, type=INT64"`
	TempRentRateDenominator         int64   `parquet:"name=temp_rent_rate_denominator, type=INT64"`
	MaxEntriesToArchive             int64   `parquet:"name=max_entries_to_archive, type=INT64, convertedtype=UINT_64"`
	BucketListSizeWindowSampleSize  int64   `parquet:"name=bucket_list_size_window_sample_size, type=INT64, convertedtype=UINT_64"`
	EvictionScanSize                int64   `parquet:"name=eviction_scan_size, type=INT64"`
	StartingEvictionScanLevel       int64   `parquet:"name=starting_eviction_scan_level, type=INT64, convertedtype=UINT_64"`
	LedgerMaxTxCount                int64   `parquet:"name=ledger_max_tx_count, type=INT64, convertedtype=UINT_64"`
	BucketListSizeWindow            []int64 `parquet:"name=bucket_list_size_window, type=INT64, repetitiontype=REPEATED"`
	LastModifiedLedger              int64   `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange               int64   `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Deleted                         bool    `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt                        int64   `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence                  int64   `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
}

// TtlOutputParquet is a representation of soroban ttl that aligns with the Bigquery table ttls
type TtlOutputParquet struct {
	KeyHash            string `parquet:"name=key_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	LiveUntilLedgerSeq int64  `parquet:"name=live_until_ledger_seq, type=INT64, convertedtype=UINT_64"`
	LastModifiedLedger int64  `parquet:"name=last_modified_ledger, type=INT64, convertedtype=UINT_64"`
	LedgerEntryChange  int64  `parquet:"name=ledger_entry_change, type=INT64, convertedtype=UINT_64"`
	Deleted            bool   `parquet:"name=deleted, type=BOOLEAN"`
	ClosedAt           int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	LedgerSequence     int64  `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
}

// ContractEventOutputParquet is a representation of soroban contract events and diagnostic events
type ContractEventOutputParquet struct {
	TransactionHash          string `parquet:"name=transaction_hash, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TransactionID            int64  `parquet:"name=transaction_id, type=INT64"`
	Successful               bool   `parquet:"name=successful, type=BOOLEAN"`
	LedgerSequence           int64  `parquet:"name=ledger_sequence, type=INT64, convertedtype=UINT_64"`
	ClosedAt                 int64  `parquet:"name=closed_at, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	InSuccessfulContractCall bool   `parquet:"name=in_successful_contract_call, type=BOOLEAN"`
	ContractId               string `parquet:"name=contract_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Type                     int32  `parquet:"name=type, type=INT32"`
	TypeString               string `parquet:"name=type_string, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Topics                   string `parquet:"name=topics, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	TopicsDecoded            string `parquet:"name=topics_decoded, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Data                     string `parquet:"name=data, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DataDecoded              string `parquet:"name=data_decoded, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractEventXDR         string `parquet:"name=contract_event_xdr, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}
