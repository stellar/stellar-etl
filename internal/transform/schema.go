package transform

import (
	"time"

	"github.com/guregu/null"
	"github.com/guregu/null/zero"
	"github.com/lib/pq"
	"github.com/stellar/go/xdr"
)

// LedgerOutput is a representation of a ledger that aligns with the BigQuery table history_ledgers
type LedgerOutput struct {
	Sequence                   uint32    `json:"sequence"` // sequence number of the ledger
	LedgerHash                 string    `json:"ledger_hash"`
	PreviousLedgerHash         string    `json:"previous_ledger_hash"`
	LedgerHeader               string    `json:"ledger_header"` // base 64 encoding of the ledger header
	TransactionCount           int32     `json:"transaction_count"`
	OperationCount             int32     `json:"operation_count"` // counts only operations that were a part of successful transactions
	SuccessfulTransactionCount int32     `json:"successful_transaction_count"`
	FailedTransactionCount     int32     `json:"failed_transaction_count"`
	TxSetOperationCount        string    `json:"tx_set_operation_count"` // counts all operations, even those that are part of failed transactions
	ClosedAt                   time.Time `json:"closed_at"`              // UTC timestamp
	TotalCoins                 int64     `json:"total_coins"`
	FeePool                    int64     `json:"fee_pool"`
	BaseFee                    uint32    `json:"base_fee"`
	BaseReserve                uint32    `json:"base_reserve"`
	MaxTxSetSize               uint32    `json:"max_tx_set_size"`
	ProtocolVersion            uint32    `json:"protocol_version"`
	LedgerID                   int64     `json:"id"`
}

// TransactionOutput is a representation of a transaction that aligns with the BigQuery table history_transactions
type TransactionOutput struct {
	TransactionHash             string         `json:"transaction_hash"`
	LedgerSequence              uint32         `json:"ledger_sequence"`
	Account                     string         `json:"account"`
	AccountMuxed                string         `json:"account_muxed,omitempty"`
	AccountSequence             int64          `json:"account_sequence"`
	MaxFee                      uint32         `json:"max_fee"`
	FeeCharged                  int64          `json:"fee_charged"`
	OperationCount              int32          `json:"operation_count"`
	TxEnvelope                  string         `json:"tx_envelope"`
	TxResult                    string         `json:"tx_result"`
	TxMeta                      string         `json:"tx_meta"`
	TxFeeMeta                   string         `json:"tx_fee_meta"`
	CreatedAt                   time.Time      `json:"created_at"`
	MemoType                    string         `json:"memo_type"`
	Memo                        string         `json:"memo"`
	TimeBounds                  string         `json:"time_bounds"`
	Successful                  bool           `json:"successful"`
	TransactionID               int64          `json:"id"`
	FeeAccount                  string         `json:"fee_account,omitempty"`
	FeeAccountMuxed             string         `json:"fee_account_muxed,omitempty"`
	InnerTransactionHash        string         `json:"inner_transaction_hash,omitempty"`
	NewMaxFee                   uint32         `json:"new_max_fee,omitempty"`
	LedgerBounds                string         `json:"ledger_bounds"`
	MinAccountSequence          null.Int       `json:"min_account_sequence"`
	MinAccountSequenceAge       null.Int       `json:"min_account_sequence_age"`
	MinAccountSequenceLedgerGap null.Int       `json:"min_account_sequence_ledger_gap"`
	ExtraSigners                pq.StringArray `json:"extra_signers"`
}

// AccountOutput is a representation of an account that aligns with the BigQuery table accounts
type AccountOutput struct {
	AccountID            string      `json:"account_id"` // account address
	Balance              float64     `json:"balance"`
	BuyingLiabilities    float64     `json:"buying_liabilities"`
	SellingLiabilities   float64     `json:"selling_liabilities"`
	SequenceNumber       int64       `json:"sequence_number"`
	SequenceLedger       zero.Int    `json:"sequence_ledger"`
	SequenceTime         zero.Int    `json:"sequence_time"`
	NumSubentries        uint32      `json:"num_subentries"`
	InflationDestination string      `json:"inflation_destination"`
	Flags                uint32      `json:"flags"`
	HomeDomain           string      `json:"home_domain"`
	MasterWeight         int32       `json:"master_weight"`
	ThresholdLow         int32       `json:"threshold_low"`
	ThresholdMedium      int32       `json:"threshold_medium"`
	ThresholdHigh        int32       `json:"threshold_high"`
	Sponsor              null.String `json:"sponsor"`
	NumSponsored         uint32      `json:"num_sponsored"`
	NumSponsoring        uint32      `json:"num_sponsoring"`
	LastModifiedLedger   uint32      `json:"last_modified_ledger"`
	LedgerEntryChange    uint32      `json:"ledger_entry_change"`
	Deleted              bool        `json:"deleted"`
}

// AccountSignerOutput is a representation of an account signer that aligns with the BigQuery table account_signers
type AccountSignerOutput struct {
	AccountID          string      `json:"account_id"`
	Signer             string      `json:"signer"`
	Weight             int32       `json:"weight"`
	Sponsor            null.String `json:"sponsor"`
	LastModifiedLedger uint32      `json:"last_modified_ledger"`
	LedgerEntryChange  uint32      `json:"ledger_entry_change"`
	Deleted            bool        `json:"deleted"`
}

// OperationOutput is a representation of an operation that aligns with the BigQuery table history_operations
type OperationOutput struct {
	SourceAccount      string                 `json:"source_account"`
	SourceAccountMuxed string                 `json:"source_account_muxed,omitempty"`
	Type               int32                  `json:"type"`
	TypeString         string                 `json:"type_string"`
	OperationDetails   map[string]interface{} `json:"details"` //Details is a JSON object that varies based on operation type
	TransactionID      int64                  `json:"transaction_id"`
	OperationID        int64                  `json:"id"`
}

// ClaimableBalanceOutput is a representation of a claimable balances that aligns with the BigQuery table claimable_balances
type ClaimableBalanceOutput struct {
	BalanceID          string      `json:"balance_id"`
	Claimants          []Claimant  `json:"claimants"`
	AssetCode          string      `json:"asset_code"`
	AssetIssuer        string      `json:"asset_issuer"`
	AssetType          string      `json:"asset_type"`
	AssetAmount        float64     `json:"asset_amount"`
	Sponsor            null.String `json:"sponsor"`
	Flags              uint32      `json:"flags"`
	LastModifiedLedger uint32      `json:"last_modified_ledger"`
	LedgerEntryChange  uint32      `json:"ledger_entry_change"`
	Deleted            bool        `json:"deleted"`
}

// Claimants
type Claimant struct {
	Destination string             `json:"destination"`
	Predicate   xdr.ClaimPredicate `json:"predicate"`
}

// Price represents the price of an asset as a fraction
type Price struct {
	Numerator   int32 `json:"n"`
	Denominator int32 `json:"d"`
}

// Path is a representation of an asset without an ID that forms part of a path in a path payment
type Path struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
}

// LiquidityPoolAsset represents the asset pairs in a liquidity pool
type LiquidityPoolAsset struct {
	AssetAType   string
	AssetACode   string
	AssetAIssuer string
	AssetAAmount float64
	AssetBType   string
	AssetBCode   string
	AssetBIssuer string
	AssetBAmount float64
}

// PoolOutput is a representation of a liquidity pool that aligns with the Bigquery table liquidity_pools
type PoolOutput struct {
	PoolID             string  `json:"liquidity_pool_id"`
	PoolType           string  `json:"type"`
	PoolFee            uint32  `json:"fee"`
	TrustlineCount     uint64  `json:"trustline_count"`
	PoolShareCount     float64 `json:"pool_share_count"`
	AssetAType         string  `json:"asset_a_type"`
	AssetACode         string  `json:"asset_a_code"`
	AssetAIssuer       string  `json:"asset_a_issuer"`
	AssetAReserve      float64 `json:"asset_a_amount"`
	AssetBType         string  `json:"asset_b_type"`
	AssetBCode         string  `json:"asset_b_code"`
	AssetBIssuer       string  `json:"asset_b_issuer"`
	AssetBReserve      float64 `json:"asset_b_amount"`
	LastModifiedLedger uint32  `json:"last_modified_ledger"`
	LedgerEntryChange  uint32  `json:"ledger_entry_change"`
	Deleted            bool    `json:"deleted"`
}

// AssetOutput is a representation of an asset that aligns with the BigQuery table history_assets
type AssetOutput struct {
	AssetCode   string `json:"asset_code"`
	AssetIssuer string `json:"asset_issuer"`
	AssetType   string `json:"asset_type"`
	AssetID     uint64 `json:"id"`
}

// TrustlineOutput is a representation of a trustline that aligns with the BigQuery table trust_lines
type TrustlineOutput struct {
	LedgerKey          string      `json:"ledger_key"`
	AccountID          string      `json:"account_id"`
	AssetCode          string      `json:"asset_code"`
	AssetIssuer        string      `json:"asset_issuer"`
	AssetType          int32       `json:"asset_type"`
	Balance            float64     `json:"balance"`
	TrustlineLimit     int64       `json:"trust_line_limit"`
	LiquidityPoolID    string      `json:"liquidity_pool_id"`
	BuyingLiabilities  float64     `json:"buying_liabilities"`
	SellingLiabilities float64     `json:"selling_liabilities"`
	Flags              uint32      `json:"flags"`
	LastModifiedLedger uint32      `json:"last_modified_ledger"`
	LedgerEntryChange  uint32      `json:"ledger_entry_change"`
	Sponsor            null.String `json:"sponsor"`
	Deleted            bool        `json:"deleted"`
}

// OfferOutput is a representation of an offer that aligns with the BigQuery table offers
type OfferOutput struct {
	SellerID           string      `json:"seller_id"` // Account address of the seller
	OfferID            int64       `json:"offer_id"`
	SellingAssetType   string      `json:"selling_asset_type"`
	SellingAssetCode   string      `json:"selling_asset_code"`
	SellingAssetIssuer string      `json:"selling_asset_issuer"`
	BuyingAssetType    string      `json:"buying_asset_type"`
	BuyingAssetCode    string      `json:"buying_asset_code"`
	BuyingAssetIssuer  string      `json:"buying_asset_issuer"`
	Amount             float64     `json:"amount"`
	PriceN             int32       `json:"pricen"`
	PriceD             int32       `json:"priced"`
	Price              float64     `json:"price"`
	Flags              uint32      `json:"flags"`
	LastModifiedLedger uint32      `json:"last_modified_ledger"`
	LedgerEntryChange  uint32      `json:"ledger_entry_change"`
	Deleted            bool        `json:"deleted"`
	Sponsor            null.String `json:"sponsor"`
}

// TradeOutput is a representation of a trade that aligns with the BigQuery table history_trades
type TradeOutput struct {
	Order                  int32       `json:"order"`
	LedgerClosedAt         time.Time   `json:"ledger_closed_at"`
	SellingAccountAddress  string      `json:"selling_account_address"`
	SellingAssetCode       string      `json:"selling_asset_code"`
	SellingAssetIssuer     string      `json:"selling_asset_issuer"`
	SellingAssetType       string      `json:"selling_asset_type"`
	SellingAmount          float64     `json:"selling_amount"`
	BuyingAccountAddress   string      `json:"buying_account_address"`
	BuyingAssetCode        string      `json:"buying_asset_code"`
	BuyingAssetIssuer      string      `json:"buying_asset_issuer"`
	BuyingAssetType        string      `json:"buying_asset_type"`
	BuyingAmount           float64     `json:"buying_amount"`
	PriceN                 int64       `json:"price_n"`
	PriceD                 int64       `json:"price_d"`
	SellingOfferID         null.Int    `json:"selling_offer_id"`
	BuyingOfferID          null.Int    `json:"buying_offer_id"`
	SellingLiquidityPoolID null.String `json:"selling_liquidity_pool_id"`
	LiquidityPoolFee       null.Int    `json:"liquidity_pool_fee"`
	HistoryOperationID     int64       `json:"history_operation_id"`
	TradeType              int32       `json:"trade_type"`
	RoundingSlippage       null.Int    `json:"rounding_slippage"`
	SellerIsExact          null.Bool   `json:"seller_is_exact"`
}

// DimAccount is a representation of an account that aligns with the BigQuery table dim_accounts
type DimAccount struct {
	ID      uint64 `json:"account_id"`
	Address string `json:"address"`
}

// DimOffer is a representation of an account that aligns with the BigQuery table dim_offers
type DimOffer struct {
	HorizonID     int64   `json:"horizon_offer_id"`
	DimOfferID    uint64  `json:"dim_offer_id"`
	MarketID      uint64  `json:"market_id"`
	MakerID       uint64  `json:"maker_id"`
	Action        string  `json:"action"`
	BaseAmount    float64 `json:"base_amount"`
	CounterAmount float64 `json:"counter_amount"`
	Price         float64 `json:"price"`
}

// FactOfferEvent is a representation of an offer event that aligns with the BigQuery table fact_offer_events
type FactOfferEvent struct {
	LedgerSeq       uint32 `json:"ledger_id"`
	OfferInstanceID uint64 `json:"offer_instance_id"`
}

// DimMarket is a representation of an account that aligns with the BigQuery table dim_markets
type DimMarket struct {
	ID            uint64 `json:"market_id"`
	BaseCode      string `json:"base_code"`
	BaseIssuer    string `json:"base_issuer"`
	CounterCode   string `json:"counter_code"`
	CounterIssuer string `json:"counter_issuer"`
}

// NormalizedOfferOutput ties together the information for dim_markets, dim_offers, dim_accounts, and fact_offer-events
type NormalizedOfferOutput struct {
	Market  DimMarket
	Offer   DimOffer
	Account DimAccount
	Event   FactOfferEvent
}

type SponsorshipOutput struct {
	Operation      xdr.Operation
	OperationIndex uint32
}

// EffectOutput is a representation of an operation that aligns with the BigQuery table history_effects
type EffectOutput struct {
	Address      string                 `json:"address"`
	AddressMuxed null.String            `json:"address_muxed,omitempty"`
	OperationID  int64                  `json:"operation_id"`
	Details      map[string]interface{} `json:"details"`
	Type         int32                  `json:"type"`
	TypeString   string                 `json:"type_string"`
}

// EffectType is the numeric type for an effect
type EffectType int

const (
	EffectAccountCreated                     EffectType = 0
	EffectAccountRemoved                     EffectType = 1
	EffectAccountCredited                    EffectType = 2
	EffectAccountDebited                     EffectType = 3
	EffectAccountThresholdsUpdated           EffectType = 4
	EffectAccountHomeDomainUpdated           EffectType = 5
	EffectAccountFlagsUpdated                EffectType = 6
	EffectAccountInflationDestinationUpdated EffectType = 7
	EffectSignerCreated                      EffectType = 10
	EffectSignerRemoved                      EffectType = 11
	EffectSignerUpdated                      EffectType = 12
	EffectTrustlineCreated                   EffectType = 20
	EffectTrustlineRemoved                   EffectType = 21
	EffectTrustlineUpdated                   EffectType = 22
	EffectTrustlineFlagsUpdated              EffectType = 26
	EffectOfferCreated                       EffectType = 30
	EffectOfferRemoved                       EffectType = 31
	EffectOfferUpdated                       EffectType = 32
	EffectTrade                              EffectType = 33
	EffectDataCreated                        EffectType = 40
	EffectDataRemoved                        EffectType = 41
	EffectDataUpdated                        EffectType = 42
	EffectSequenceBumped                     EffectType = 43
	EffectClaimableBalanceCreated            EffectType = 50
	EffectClaimableBalanceClaimantCreated    EffectType = 51
	EffectClaimableBalanceClaimed            EffectType = 52
	EffectAccountSponsorshipCreated          EffectType = 60
	EffectAccountSponsorshipUpdated          EffectType = 61
	EffectAccountSponsorshipRemoved          EffectType = 62
	EffectTrustlineSponsorshipCreated        EffectType = 63
	EffectTrustlineSponsorshipUpdated        EffectType = 64
	EffectTrustlineSponsorshipRemoved        EffectType = 65
	EffectDataSponsorshipCreated             EffectType = 66
	EffectDataSponsorshipUpdated             EffectType = 67
	EffectDataSponsorshipRemoved             EffectType = 68
	EffectClaimableBalanceSponsorshipCreated EffectType = 69
	EffectClaimableBalanceSponsorshipUpdated EffectType = 70
	EffectClaimableBalanceSponsorshipRemoved EffectType = 71
	EffectSignerSponsorshipCreated           EffectType = 72
	EffectSignerSponsorshipUpdated           EffectType = 73
	EffectSignerSponsorshipRemoved           EffectType = 74
	EffectClaimableBalanceClawedBack         EffectType = 80
	EffectLiquidityPoolDeposited             EffectType = 90
	EffectLiquidityPoolWithdrew              EffectType = 91
	EffectLiquidityPoolTrade                 EffectType = 92
	EffectLiquidityPoolCreated               EffectType = 93
	EffectLiquidityPoolRemoved               EffectType = 94
	EffectLiquidityPoolRevoked               EffectType = 95
)

// EffectTypeNames stores a map of effect type ID and names
var EffectTypeNames = map[EffectType]string{
	EffectAccountCreated:                     "account_created",
	EffectAccountRemoved:                     "account_removed",
	EffectAccountCredited:                    "account_credited",
	EffectAccountDebited:                     "account_debited",
	EffectAccountThresholdsUpdated:           "account_thresholds_updated",
	EffectAccountHomeDomainUpdated:           "account_home_domain_updated",
	EffectAccountFlagsUpdated:                "account_flags_updated",
	EffectAccountInflationDestinationUpdated: "account_inflation_destination_updated",
	EffectSignerCreated:                      "signer_created",
	EffectSignerRemoved:                      "signer_removed",
	EffectSignerUpdated:                      "signer_updated",
	EffectTrustlineCreated:                   "trustline_created",
	EffectTrustlineRemoved:                   "trustline_removed",
	EffectTrustlineUpdated:                   "trustline_updated",
	EffectTrustlineFlagsUpdated:              "trustline_flags_updated",
	EffectOfferCreated:                       "offer_created",
	EffectOfferRemoved:                       "offer_removed",
	EffectOfferUpdated:                       "offer_updated",
	EffectTrade:                              "trade",
	EffectDataCreated:                        "data_created",
	EffectDataRemoved:                        "data_removed",
	EffectDataUpdated:                        "data_updated",
	EffectSequenceBumped:                     "sequence_bumped",
	EffectClaimableBalanceCreated:            "claimable_balance_created",
	EffectClaimableBalanceClaimed:            "claimable_balance_claimed",
	EffectClaimableBalanceClaimantCreated:    "claimable_balance_claimant_created",
	EffectAccountSponsorshipCreated:          "account_sponsorship_created",
	EffectAccountSponsorshipUpdated:          "account_sponsorship_updated",
	EffectAccountSponsorshipRemoved:          "account_sponsorship_removed",
	EffectTrustlineSponsorshipCreated:        "trustline_sponsorship_created",
	EffectTrustlineSponsorshipUpdated:        "trustline_sponsorship_updated",
	EffectTrustlineSponsorshipRemoved:        "trustline_sponsorship_removed",
	EffectDataSponsorshipCreated:             "data_sponsorship_created",
	EffectDataSponsorshipUpdated:             "data_sponsorship_updated",
	EffectDataSponsorshipRemoved:             "data_sponsorship_removed",
	EffectClaimableBalanceSponsorshipCreated: "claimable_balance_sponsorship_created",
	EffectClaimableBalanceSponsorshipUpdated: "claimable_balance_sponsorship_updated",
	EffectClaimableBalanceSponsorshipRemoved: "claimable_balance_sponsorship_removed",
	EffectSignerSponsorshipCreated:           "signer_sponsorship_created",
	EffectSignerSponsorshipUpdated:           "signer_sponsorship_updated",
	EffectSignerSponsorshipRemoved:           "signer_sponsorship_removed",
	EffectClaimableBalanceClawedBack:         "claimable_balance_clawed_back",
	EffectLiquidityPoolDeposited:             "liquidity_pool_deposited",
	EffectLiquidityPoolWithdrew:              "liquidity_pool_withdrew",
	EffectLiquidityPoolTrade:                 "liquidity_pool_trade",
	EffectLiquidityPoolCreated:               "liquidity_pool_created",
	EffectLiquidityPoolRemoved:               "liquidity_pool_removed",
	EffectLiquidityPoolRevoked:               "liquidity_pool_revoked",
}

// TradeEffectDetails is a struct of data from `effects.DetailsString`
// when the effect type is trade
type TradeEffectDetails struct {
	Seller            string `json:"seller"`
	SellerMuxed       string `json:"seller_muxed,omitempty"`
	SellerMuxedID     uint64 `json:"seller_muxed_id,omitempty"`
	OfferID           int64  `json:"offer_id"`
	SoldAmount        string `json:"sold_amount"`
	SoldAssetType     string `json:"sold_asset_type"`
	SoldAssetCode     string `json:"sold_asset_code,omitempty"`
	SoldAssetIssuer   string `json:"sold_asset_issuer,omitempty"`
	BoughtAmount      string `json:"bought_amount"`
	BoughtAssetType   string `json:"bought_asset_type"`
	BoughtAssetCode   string `json:"bought_asset_code,omitempty"`
	BoughtAssetIssuer string `json:"bought_asset_issuer,omitempty"`
}

// TestTransaction transaction meta
type TestTransaction struct {
	Index         uint32
	EnvelopeXDR   string
	ResultXDR     string
	FeeChangesXDR string
	MetaXDR       string
	Hash          string
}
