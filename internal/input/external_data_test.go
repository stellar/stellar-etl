package input

import (
	"testing"

	"github.com/stellar/stellar-etl/internal/utils"
	"github.com/stretchr/testify/assert"
)

func getEntityDataHelper[T any](t *testing.T, provider string, expected []T) {
	mockClient := GetMockClient(provider)
	result, err := GetEntityData[T](mockClient, provider, "", "")
	if err != nil {
		t.Fatalf("Error calling GetEntityData: %v", err)
	}

	assert.Equal(t, expected, result)
}

func TestGetEntityDataForRetool(t *testing.T) {
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
	getEntityDataHelper[utils.RetoolEntityDataTransformInput](t, "retool", expected)
}
