package transform

import "time"

//LedgerOutput is a representation of a ledger that aligns with the BigQuery table history_ledgers
type LedgerOutput struct {
	Sequence                   int32     `json:"sequence"` //sequence number of the ledger
	LedgerHash                 string    `json:"ledger_hash"`
	PreviousLedgerHash         string    `json:"previous_ledger_hash"`
	LedgerHeader               []byte    `json:"ledger_header"` //base 64 encoding of the ledger header
	TransactionCount           int32     `json:"transaction_count"`
	OperationCount             int32     `json:"operation_count"` //counts only operations that were a part of successful transactions
	SuccessfulTransactionCount int32     `json:"successful_transaction_count"`
	FailedTransactionCount     int32     `json:"failed_transaction_count"`
	TxSetOperationCount        string    `json:"tx_set_operation_count"` //counts all operations, even those that are part of failed transactions
	ClosedAt                   time.Time `json:"closed_at"`              //UTC timestamp
	TotalCoins                 int64     `json:"total_coins"`
	FeePool                    int64     `json:"fee_pool"`
	BaseFee                    int32     `json:"base_fee"`
	BaseReserve                int32     `json:"base_reserve"`
	MaxTxSetSize               int32     `json:"max_tx_set_size"`
	ProtocolVersion            int32     `json:"protocol_version"`

	/*
		TODO implement these three fields
			CreatedAt time.Time //timestamp of table entry creation time
			UpdatedAt time.Time //timestamp of table entry update time
			ImporterVersion int32 //version of the ingestion system
	*/
}

//TransactionOutput is a representation of a transaction that aligns with the BigQuery table history_transactions
type TransactionOutput struct {
	TransactionHash  string    `json:"transaction_hash"`
	LedgerSequence   int32     `json:"ledger_sequence"`
	ApplicationOrder int32     `json:"application_order"`
	Account          string    `json:"account"`
	AccountSequence  int64     `json:"account_sequence"`
	MaxFee           int64     `json:"max_fee"`
	FeeCharged       int64     `json:"fee_charged"`
	OperationCount   int32     `json:"operation_count"`
	CreatedAt        time.Time `json:"created_at"`
	MemoType         string    `json:"memo_type"`
	Memo             string    `json:"memo"`
	TimeBounds       string    `json:"time_bounds"`
	Successful       bool      `json:"successful"`

	/*
		TODO implement
			updated_at time.Time //timestamp of table entry update time
	*/
}

//OperationOutput is a representation of an operation that aligns with the BigQuery table history_operations
type OperationOutput struct {
	SourceAccount    string  `json:"source_account"`
	Type             int32   `json:"type"`
	ApplicationOrder int32   `json:"application_order"`
	OperationDetails Details `json:"details"`
	/*
		TODO implement
			TransactionID int64 // history table mapping that connect operations to their parent transaction
	*/
}

//Details is a struct that provides additional information about operations in a way that aligns with the details struct in the BigQuery table history_operations
type Details struct {
	Account            string        `json:"account"`
	Amount             float64       `json:"amount"`
	AssetCode          string        `json:"asset_code"`
	AssetIssuer        string        `json:"asset_issuer"`
	AssetType          string        `json:"asset_type"`
	Authorize          bool          `json:"authorize"`
	BuyingAssetCode    string        `json:"buying_asset_code"`
	BuyingAssetIssuer  string        `json:"buying_asset_issuer"`
	BuyingAssetType    string        `json:"buying_asset_type"`
	From               string        `json:"from"`
	Funder             string        `json:"funder"`
	HighThreshold      uint32        `json:"high_threshold"`
	HomeDomain         string        `json:"home_domain"`
	InflationDest      string        `json:"inflation_dest"`
	Into               string        `json:"into"`
	Limit              float64       `json:"limit"`
	LowThreshold       uint32        `json:"low_threshold"`
	MasterKeyWeight    uint32        `json:"master_key_weight"`
	MedThreshold       uint32        `json:"med_threshold"`
	Name               string        `json:"name"`
	OfferID            int64         `json:"offer_id"`
	Path               []AssetOutput `json:"path"`
	Price              float64       `json:"price"`
	PriceR             Price         `json:"price_r"`
	SellingAssetCode   string        `json:"selling_asset_code"`
	SellingAssetIssuer string        `json:"selling_asset_issuer"`
	SellingAssetType   string        `json:"selling_asset_type"`
	SetFlags           []int32       `json:"set_flags"`
	SetFlagsString     []string      `json:"set_flags_s"`
	SignerKey          string        `json:"signer_key"`
	SignerWeight       uint32        `json:"signer_weight"`
	SourceAmount       float64       `json:"source_amount"`
	SourceAssetCode    string        `json:"source_asset_code"`
	SourceAssetIssuer  string        `json:"source_asset_issuer"`
	SourceAssetType    string        `json:"source_asset_type"`
	SourceMax          float64       `json:"source_max"`
	StartingBalance    float64       `json:"starting_balance"`
	To                 string        `json:"to"`
	Trustee            string        `json:"trustee"`
	Trustor            string        `json:"trustor"`
	Value              string        `json:"value"` //base64 encoding of bytes
	ClearFlags         []int32       `json:"clear_flags"`
	ClearFlagsString   []string      `json:"clear_flags_s"`
	DestinationMin     string        `json:"destination_min"`
	BumpTo             string        `json:"bump_to"`
}

//Price represents the price of an asset as a fraction
type Price struct {
	Numerator   int32 `json:"n"`
	Denominator int32 `json:"d"`
}

//AssetOutput is a representation of an asset that aligns with the BigQuery table history_assets
type AssetOutput struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}
