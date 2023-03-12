package models

import (
	"strconv"

	"github.com/comfforts/localstorage/pkg/constants"
)

type JSONMapper = map[string]interface{}

type Personnel struct {
	Postion    string
	FirstName  string
	MiddleName string
	LastName   string
	Addr       string
}

type Entity struct {
	ID                int
	Org               string
	Name              string
	OtherName         string
	Status            string
	BusinessType      string
	EntityType        string
	Jurisdiction      string
	Structure         string
	Personnel         []*Personnel
	MailingAddr       string
	PrincipalAddr     string
	CAAddr            string
	InitialFilingDate string
	FilingType        string
	StatndingFTB      string
	StandingSOS       string
	StandingAgent     string
	StandingVCFCF     string
	LastFileNumber    string
	LastFileDate      string
	SuspensionDate    string
}

type Principal struct {
	ID         int
	Name       string
	Org        string
	FirstName  string
	MiddleName string
	LastName   string
	Position   string
	Address    string
}

/*
	1 - ENTITY_NAME
	2 - ENTITY_NUM
	3 - ORG_NAME
	4 - FIRST_NAME
	5 - MIDDLE_NAME
	6 - LAST_NAME
	7 - POSITION_TYPE
	8 - ADDRESS
*/

func MapRecordToPrincipal(record []string) (*Principal, []error) {
	errs := []error{}
	id, err := strconv.Atoi(record[1])
	if err != nil {
		errs = append(errs, constants.ErrConvertingId)
	}

	name, org, fName, mName, lName, pos, addr := record[0], record[2], record[3], record[4], record[5], record[6], record[7]
	return &Principal{
		Name:       name,
		ID:         id,
		Org:        org,
		FirstName:  fName,
		MiddleName: mName,
		LastName:   lName,
		Position:   pos,
		Address:    addr,
	}, errs
}

/*
	1 - ENTITY_NAME
	2 - ENTITY_NUM
	3 - ORG_NAME
	4 - FIRST_NAME
	5 - MIDDLE_NAME
	6 - LAST_NAME
	7 - PHYSICAL_ADDRESS
	8 - AGENT_TYPE
*/

func MapRecordToAgent(record []string) (*Principal, []error) {
	errs := []error{}
	id, err := strconv.Atoi(record[1])
	if err != nil {
		errs = append(errs, constants.ErrConvertingId)
	}

	name, org, fName, mName, lName, addr, agentType := record[0], record[2], record[3], record[4], record[5], record[6], record[7]
	return &Principal{
		Name:       name,
		ID:         id,
		Org:        org,
		FirstName:  fName,
		MiddleName: mName,
		LastName:   lName,
		Position:   agentType,
		Address:    addr,
	}, errs
}

/*
 	1 - ENTITY_NAME - Name
	2 - ENTITY_NUM - ID
	3 - INITIAL_FILING_DATE - InitialFilingDate
	4 - JURISDICTION - Jurisdiction
	5 - ENTITY_STATUS - Status
	6 - STANDING_SOS - StandingSOS
	7 - ENTITY_TYPE - EntityType
	8 - FILING_TYPE - FilingType
	9 - FOREIGN_NAME - OtherName
	10 - STANDING_FTB - StatndingFTB
	11 - STANDING_VCFCF - StandingVCFCF
	12 - STANDING_AGENT - StandingAgent
	13 - SUSPENSION_DATE - SuspensionDate
	14 - LAST_SI_FILE_NUMBER - LastFileNumber
	15 - LAST_SI_FILE_DATE - LastFileDate
	16 - PRINCIPAL_ADDRESS - PrincipalAddr
	17 - MAILING_ADDRESS - MailingAddr
	18 - PRINCIPAL_ADDRESS_IN_CA - CAAddr
	19 - LLC_MANAGEMENT_STRUCTURE Structure
	20 - TYPE_OF_BUSINESS - BusinessType
*/

func MapRecordToEntity(record []string) (*Entity, []error) {
	errs := []error{}
	id, err := strconv.Atoi(record[1])
	if err != nil {
		errs = append(errs, constants.ErrConvertingId)
		id = 0
	}

	name, oName, status, eType, juris := record[0], record[8], record[4], record[6], record[3]
	mAddr, pAddr := record[16], record[15]
	filingDate, filingType, ftb, sos, agent, vcfcf, fileNum := record[2], record[7], record[9], record[5], record[11], record[10], record[13]
	siFileDate, suspDate := record[14], record[12]

	var caAddr, llcStruct, bType string
	caAddr, llcStruct = record[17], record[18]
	if len(record) == 20 {
		bType = record[19]
	}

	return &Entity{
		Name:              name,
		ID:                id,
		OtherName:         oName,
		Status:            status,
		BusinessType:      bType,
		EntityType:        eType,
		Jurisdiction:      juris,
		Structure:         llcStruct,
		MailingAddr:       mAddr,
		PrincipalAddr:     pAddr,
		CAAddr:            caAddr,
		InitialFilingDate: filingDate,
		FilingType:        filingType,
		StatndingFTB:      ftb,
		StandingSOS:       sos,
		StandingAgent:     agent,
		StandingVCFCF:     vcfcf,
		LastFileNumber:    fileNum,
		LastFileDate:      siFileDate,
		SuspensionDate:    suspDate,
	}, errs
}

func MapToEntity(res map[int]*Entity, record []string) *Entity {
	ppl, _ := MapRecordToPrincipal(record)
	rec, ok := res[ppl.ID]
	if ok {
		if ppl.FirstName != "" {
			rec.Personnel = append(rec.Personnel, &Personnel{
				Postion:    ppl.Position,
				FirstName:  ppl.FirstName,
				MiddleName: ppl.MiddleName,
				LastName:   ppl.LastName,
				Addr:       ppl.Address,
			})
		}
	} else {
		entity := Entity{
			ID:   ppl.ID,
			Org:  ppl.Org,
			Name: ppl.Name,
		}

		if ppl.FirstName != "" {
			entity.Personnel = []*Personnel{
				{
					Postion:    ppl.Position,
					FirstName:  ppl.FirstName,
					MiddleName: ppl.MiddleName,
					LastName:   ppl.LastName,
					Addr:       ppl.Address,
				},
			}
		}
		res[ppl.ID] = &entity
	}
	return res[ppl.ID]
}
