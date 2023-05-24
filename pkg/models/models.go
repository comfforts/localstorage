package models

type JSONMapper = map[string]interface{}

type BusinessAgent struct {
	EntityName      string
	EntityNum       string `json:"entity_num"`
	OrgName         string
	FirstName       string
	MiddleName      string
	LastName        string
	PhysicalAddress string
	AgentType       string
}

type BusinessFiling struct {
	EntityName             string
	EntityNum              string `json:"entity_num"`
	InitialFilingDate      string
	Jurisdiction           string
	EntityStatus           string
	StandingSOS            string
	EntityType             string
	FilingType             string
	ForeignName            string
	StandingFTB            string
	StandingVCFCF          string
	SuspensionDate         string
	LastSIFileNumber       string
	LastSIFileDate         string
	PrincipalAddress       string
	MailingAddress         string
	PrincipalAddressInCA   string
	LLCManagementStructure string
	TypeOfBusiness         string
}

type BusinessPrincipal struct {
	EntityName   string
	EntityNum    string `json:"entity_num"`
	OrgName      string
	FirstName    string
	MiddleName   string
	LastName     string
	Address      string
	PositionType string
}
