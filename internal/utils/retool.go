package utils

// RetoolEntityDataTransformInput is a representation of the input for the TransformRetoolEntityData function
type RetoolEntityDataTransformInput struct {
	ID                     int                    `json:"id"`
	Name                   string                 `json:"name"`
	Status                 string                 `json:"status"`
	CreatedAt              int64                  `json:"created_at"`
	UpdatedAt              *int64                 `json:"updated_at"`
	Custodial              bool                   `json:"custodial"`
	NonCustodial           bool                   `json:"non_custodial"`
	HomeDomainsID          int                    `json:"home_domains_id"`
	Description            string                 `json:"description"`
	WebsiteURL             string                 `json:"website_url"`
	SdpEnabled             bool                   `json:"sdp_enabled"`
	SorobanEnabled         bool                   `json:"soroban_enabled"`
	Notes                  string                 `json:"notes"`
	Verified               bool                   `json:"verified"`
	FeeSponsor             bool                   `json:"fee_sponsor"`
	AccountSponsor         bool                   `json:"account_sponsor"`
	Live                   bool                   `json:"live"`
	HomeDomain             HomeDomain             `json:"_home_domain"`
	AppGeographiesDetails  []AppGeographyDetail   `json:"_app_geographies_details"`
	AppToRampsIntegrations []AppToRampIntegration `json:"_app_to_ramps_integrations"`
}

type HomeDomain struct {
	ID         int64  `json:"id"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
	HomeDomain string `json:"home_domain"`
}

type AppGeographyDetail struct {
	ID            int64       `json:"id"`
	AppsID        int64       `json:"apps_id"`
	CreatedAt     int64       `json:"created_at"`
	GeographiesID []Geography `json:"geographies_id"`
	Retail        bool        `json:"retail"`
	Enterprise    bool        `json:"enterprise"`
}

type Geography struct {
	ID           int64  `json:"id"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
	Name         string `json:"name"`
	OfficialName string `json:"official_name"`
}

type AppToRampIntegration struct {
	ID        int64  `json:"id"`
	CreatedAt int64  `json:"created_at"`
	AnchorsID int64  `json:"anchors_id"`
	AppsID    int64  `json:"apps_id"`
	Anchor    Anchor `json:"_anchor"`
}

type Anchor struct {
	ID        int64  `json:"id"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
	Name      string `json:"name"`
}
