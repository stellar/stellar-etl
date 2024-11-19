package input

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stellar/go/support/http/httptest"
	"github.com/stellar/go/utils/apiclient"
	"github.com/stellar/stellar-etl/internal/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetRetoolData(t *testing.T) {
	hmock := httptest.NewClient()

	mockResponses := []httptest.ResponseData{
		{
			Status: http.StatusOK,
			Body: `[
				{
					"id": 16,
					"created_at": 1706749912776,
					"updated_at": null,
					"custodial": true,
					"non_custodial": true,
					"home_domains_id": 240,
					"name": "El Dorado",
					"description": "",
					"website_url": "",
					"sdp_enabled": false,
					"soroban_enabled": false,
					"notes": "",
					"verified": false,
					"fee_sponsor": false,
					"account_sponsor": false,
					"live": true,
					"status": "live",
					"_home_domain": {
						"id": 240,
						"created_at": 1706749903897,
						"home_domain": "eldorado.io",
						"updated_at": 1706749903897
					},
					"_app_geographies_details": [
						{
							"id": 39,
							"apps_id": 16,
							"created_at": 1707887845605,
							"geographies_id": [
								{
									"id": 176,
									"created_at": 1691020699576,
									"updated_at": 1706650713745,
									"name": "Argentina",
									"official_name": "The Argentine Republic"
								},
								{
									"id": 273,
									"created_at": 1691020699834,
									"updated_at": 1706650708355,
									"name": "Brazil",
									"official_name": "The Federative Republic of Brazil"
								}
							],
							"retail": false,
							"enterprise": false
						}
					],
					"_app_to_ramps_integrations": [
						{
							"id": 18,
							"created_at": 1707617027154,
							"anchors_id": 28,
							"apps_id": 16,
							"_anchor": {
								"id": 28,
								"created_at": 1705423531705,
								"name": "MoneyGram",
								"updated_at": 1706596979487,
								"home_domains_id": 203
							}
						}
					]
				}
			]`,
			Header: nil,
		},
	}

	hmock.On("GET", fmt.Sprintf("%s/apps_details", baseUrl)).
		ReturnMultipleResults(mockResponses)

	mockClient := &apiclient.APIClient{
		BaseURL: baseUrl,
		HTTP:    hmock,
	}

	result, err := GetRetoolData(mockClient)
	if err != nil {
		t.Fatalf("Error calling GetRetoolData: %v", err)
	}

	expected := []utils.RetoolEntityDataTransformInput{
		{
			ID:             16,
			Name:           "El Dorado",
			Status:         "live",
			CreatedAt:      1706749912776,
			UpdatedAt:      nil,
			Custodial:      true,
			NonCustodial:   true,
			HomeDomainsID:  240,
			Description:    "",
			WebsiteURL:     "",
			SdpEnabled:     false,
			SorobanEnabled: false,
			Notes:          "",
			Verified:       false,
			FeeSponsor:     false,
			AccountSponsor: false,
			Live:           true,
			HomeDomain: utils.HomeDomain{
				ID:         240,
				CreatedAt:  1706749903897,
				UpdatedAt:  1706749903897,
				HomeDomain: "eldorado.io",
			},
			AppGeographiesDetails: []utils.AppGeographyDetail{
				{
					ID:        39,
					AppsID:    16,
					CreatedAt: 1707887845605,
					GeographiesID: []utils.Geography{
						{
							ID:           176,
							CreatedAt:    1691020699576,
							UpdatedAt:    1706650713745,
							Name:         "Argentina",
							OfficialName: "The Argentine Republic",
						},
						{
							ID:           273,
							CreatedAt:    1691020699834,
							UpdatedAt:    1706650708355,
							Name:         "Brazil",
							OfficialName: "The Federative Republic of Brazil",
						},
					},
					Retail:     false,
					Enterprise: false,
				},
			},
			AppToRampsIntegrations: []utils.AppToRampIntegration{
				{
					ID:        18,
					CreatedAt: 1707617027154,
					AnchorsID: 28,
					AppsID:    16,
					Anchor: utils.Anchor{
						ID:        28,
						CreatedAt: 1705423531705,
						UpdatedAt: 1706596979487,
						Name:      "MoneyGram",
					},
				},
			},
		},
	}

	assert.Equal(t, expected, result)
}
