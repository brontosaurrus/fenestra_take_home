Time TIMESTAMP,
AdvertiserId INTEGER,
OrderId BIGINT,
LineItemId BIGINT,
CreativeId BIGINT,
CreativeVersion INTEGER,
CreativeSize VARCHAR(10),
AdUnitId BIGINT,
CountryId INTEGER,
RegionId INTEGER,
MetroId INTEGER,
CityId INTEGER,
BrowserId INTEGER,
OSId INTEGER,
OSVersion VARCHAR(256),
TimeUsec2 BIGINT,
KeyPart VARCHAR(255),
Product VARCHAR(255),
RequestedAdUnitSizes VARCHAR(255),
BandwidthGroupId INTEGER,
MobileDevice BOOLEAN,
IsCompanion BOOLEAN,
DeviceCategory VARCHAR(20),
ActiveViewEligibleImpression BOOLEAN,
MobileCarrier VARCHAR(255),
EstimatedBackfillRevenue FLOAT,
GfpContentId BIGINT,
PostalCodeId INTEGER,
BandwidthId INTEGER,
MobileCapability BOOLEAN,
VideoPosition VARCHAR(20),
PodPosition INTEGER,
VideoFallbackPosition VARCHAR(20),
IsInterstitial BOOLEAN,
EventTimeUsec2 BIGINT,
EventKeyPart VARCHAR(255),
YieldGroupCompanyId BIGINT,
 RequestLanguage VARCHAR(10),
DealId VARCHAR(255),
SellerReservePrice FLOAT,
DealType VARCHAR(20),
AdxAccountId BIGINT,
Buyer VARCHAR(255),
Advertiser VARCHAR(255),
Anonymous BOOLEAN,
ImpressionId VARCHAR(255),
PRIMARY KEY (OrderId, LineItemId, KeyPart)