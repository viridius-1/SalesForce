from flask import Flask
import json

from simple_salesforce import Salesforce
import requests
import pandas as pd
from io import StringIO
import httplib2
import pygsheets

sf = Salesforce(
            username='yossi@bpersonal.ai', 
            password = 'bP3rs@nal!',
            security_token='DHzCRT5uNDDa4Zi7KD4naaWXX',
            )

api = Flask(__name__)

contact_results=sf.query_all("""SELECT 
                             Id,
                             Name,
                             Email,
                             AccountId,
                             AssistantName,
                             AssistantPhone,
                             Birthdate,
                             Department,
                             Description,
                             EmailBouncedDate,
                             EmailBouncedReason,
                             Fax,
                             FirstName,
                             HomePhone,
                             IndividualId,
                             Jigsaw,
                             LastActivityDate,
                             LastName,
                             LastReferencedDate,
                             LastViewedDate,
                             LeadSource,
                             MailingAddress,
                             MailingCity,
                             MailingCountry,
                             MailingGeocodeAccuracy,
                             MailingLatitude,
                             MailingLongitude,
                             MailingPostalCode,
                             MailingState,
                             MailingStreet,
                             MasterRecordId,
                             MobilePhone,
                             OtherAddress,
                             OtherCity,
                             OtherCountry,
                             OtherGeocodeAccuracy,
                             OtherLatitude,
                             OtherLongitude,
                             OtherPhone,
                             OtherPostalCode,
                             OtherState,
                             OtherStreet,
                             OwnerId,
                             Phone,
                             PhotoUrl,
                             Salutation,
                             Title
                             FROM Contact""")

campagin_all_results=sf.query_all("""SELECT 
                              Id,
                              Name, 
                              Type, 
                              Status, 
                              EndDate, 
                              BudgetedCost,
                              ActualCost,
                              ExpectedResponse,
                              Description,
                              AmountAllOpportunities,
                              AmountWonOpportunities,
                              ExpectedRevenue,
                              LastActivityDate,
                              LastReferencedDate,
                              LastViewedDate,
                              NumberOfContacts,
                              NumberOfConvertedLeads,
                              NumberOfLeads,
                              NumberOfOpportunities,
                              NumberOfResponses,
                              NumberOfWonOpportunities,
                              NumberSent,
                              StartDate                                                                             
                              FROM Campaign""")

campagin_new_results=sf.query_all("""SELECT 
                              Id,
                              Name, 
                              Type, 
                              Status, 
                              EndDate, 
                              BudgetedCost,
                              ActualCost,
                              ExpectedResponse,
                              Description,
                              AmountAllOpportunities,
                              AmountWonOpportunities,
                              ExpectedRevenue,
                              LastActivityDate,
                              LastReferencedDate,
                              LastViewedDate,
                              NumberOfContacts,
                              NumberOfConvertedLeads,
                              NumberOfLeads,
                              NumberOfOpportunities,
                              NumberOfResponses,
                              NumberOfWonOpportunities,
                              NumberSent,
                              StartDate                                                                             
                              FROM Campaign WHERE Status = 'Planned'""")

campagin_progress_results=sf.query_all("""SELECT 
                              Id,
                              Name, 
                              Type, 
                              Status, 
                              EndDate, 
                              BudgetedCost,
                              ActualCost,
                              ExpectedResponse,
                              Description,
                              AmountAllOpportunities,
                              AmountWonOpportunities,
                              ExpectedRevenue,
                              LastActivityDate,
                              LastReferencedDate,
                              LastViewedDate,
                              NumberOfContacts,
                              NumberOfConvertedLeads,
                              NumberOfLeads,
                              NumberOfOpportunities,
                              NumberOfResponses,
                              NumberOfWonOpportunities,
                              NumberSent,
                              StartDate                                                                             
                              FROM Campaign WHERE Status = 'In Progress'""")

campagin_completed_results=sf.query_all("""SELECT 
                              Id,
                              Name, 
                              Type, 
                              Status, 
                              EndDate, 
                              BudgetedCost,
                              ActualCost,
                              ExpectedResponse,
                              Description,
                              AmountAllOpportunities,
                              AmountWonOpportunities,
                              ExpectedRevenue,
                              LastActivityDate,
                              LastReferencedDate,
                              LastViewedDate,
                              NumberOfContacts,
                              NumberOfConvertedLeads,
                              NumberOfLeads,
                              NumberOfOpportunities,
                              NumberOfResponses,
                              NumberOfWonOpportunities,
                              NumberSent,
                              StartDate                                                                             
                              FROM Campaign WHERE Status = 'Completed'""")

campagin_aborted_results=sf.query_all("""SELECT 
                              Id,
                              Name, 
                              Type, 
                              Status, 
                              EndDate, 
                              BudgetedCost,
                              ActualCost,
                              ExpectedResponse,
                              Description,
                              AmountAllOpportunities,
                              AmountWonOpportunities,
                              ExpectedRevenue,
                              LastActivityDate,
                              LastReferencedDate,
                              LastViewedDate,
                              NumberOfContacts,
                              NumberOfConvertedLeads,
                              NumberOfLeads,
                              NumberOfOpportunities,
                              NumberOfResponses,
                              NumberOfWonOpportunities,
                              NumberSent,
                              StartDate                                                                             
                              FROM Campaign WHERE Status = 'Aborted'""")

lead_all_results=sf.query_all("""SELECT 
                            Address,
                            City,
                            CleanStatus,
                            Company,
                            CompanyDunsNumber,
                            ConvertedDate,
                            Country,
                            Description,
                            Email,
                            EmailBouncedDate,
                            EmailBouncedReason,
                            Fax,
                            FirstName,
                            Jigsaw,
                            LastActivityDate,
                            LastName,
                            LastReferencedDate,
                            LastViewedDate,
                            Latitude,
                            Longitude,
                            MobilePhone,
                            Name,
                            NumberOfEmployees,
                            Phone,
                            PhotoUrl,
                            PostalCode,
                            State,
                            Status,
                            Street,
                            Title,
                            Website
                            FROM Lead""")

lead_notcontacted_results=sf.query_all("""SELECT 
                            Address,
                            City,
                            CleanStatus,
                            Company,
                            CompanyDunsNumber,
                            ConvertedDate,
                            Country,
                            Description,
                            Email,
                            EmailBouncedDate,
                            EmailBouncedReason,
                            Fax,
                            FirstName,
                            Jigsaw,
                            LastActivityDate,
                            LastName,
                            LastReferencedDate,
                            LastViewedDate,
                            Latitude,
                            Longitude,
                            MobilePhone,
                            Name,
                            NumberOfEmployees,
                            Phone,
                            PhotoUrl,
                            PostalCode,
                            State,
                            Status,
                            Street,
                            Title,
                            Website
                            FROM Lead WHERE Status = 'Open - Not Contacted'""")

lead_contacted_results=sf.query_all("""SELECT 
                            Address,
                            City,
                            CleanStatus,
                            Company,
                            CompanyDunsNumber,
                            ConvertedDate,
                            Country,
                            Description,
                            Email,
                            EmailBouncedDate,
                            EmailBouncedReason,
                            Fax,
                            FirstName,
                            Jigsaw,
                            LastActivityDate,
                            LastName,
                            LastReferencedDate,
                            LastViewedDate,
                            Latitude,
                            Longitude,
                            MobilePhone,
                            Name,
                            NumberOfEmployees,
                            Phone,
                            PhotoUrl,
                            PostalCode,
                            State,
                            Status,
                            Street,
                            Title,
                            Website
                            FROM Lead WHERE Status = 'Working - Contacted'""")

lead_converted_results=sf.query_all("""SELECT 
                            Address,
                            City,
                            CleanStatus,
                            Company,
                            CompanyDunsNumber,
                            ConvertedDate,
                            Country,
                            Description,
                            Email,
                            EmailBouncedDate,
                            EmailBouncedReason,
                            Fax,
                            FirstName,
                            Jigsaw,
                            LastActivityDate,
                            LastName,
                            LastReferencedDate,
                            LastViewedDate,
                            Latitude,
                            Longitude,
                            MobilePhone,
                            Name,
                            NumberOfEmployees,
                            Phone,
                            PhotoUrl,
                            PostalCode,
                            State,
                            Status,
                            Street,
                            Title,
                            Website
                            FROM Lead WHERE Status = 'Closed - Converted'""")

lead_notconverted_results=sf.query_all("""SELECT 
                            Address,
                            City,
                            CleanStatus,
                            Company,
                            CompanyDunsNumber,
                            ConvertedDate,
                            Country,
                            Description,
                            Email,
                            EmailBouncedDate,
                            EmailBouncedReason,
                            Fax,
                            FirstName,
                            Jigsaw,
                            LastActivityDate,
                            LastName,
                            LastReferencedDate,
                            LastViewedDate,
                            Latitude,
                            Longitude,
                            MobilePhone,
                            Name,
                            NumberOfEmployees,
                            Phone,
                            PhotoUrl,
                            PostalCode,
                            State,
                            Status,
                            Street,
                            Title,
                            Website
                            FROM Lead WHERE Status = 'Closed - Not Converted'""")

account_results=sf.query_all("""SELECT 
                                AccountNumber,
                                AccountSource,
                                AnnualRevenue,
                                BillingAddress,
                                BillingCity,
                                BillingCountry,
                                BillingGeocodeAccuracy,
                                BillingLatitude,
                                BillingLongitude,
                                BillingState,
                                BillingStreet,
                                CleanStatus,
                                Description,
                                DunsNumber,
                                Fax,
                                Industry,
                                Jigsaw,
                                LastActivityDate,
                                LastReferencedDate,
                                LastViewedDate,
                                NaicsCode,
                                NaicsDesc,
                                Name,
                                NumberOfEmployees,
                                Phone,
                                PhotoUrl,
                                ShippingAddress,
                                ShippingCity,
                                ShippingCountry,
                                ShippingLatitude,
                                ShippingLongitude,
                                ShippingPostalCode,
                                ShippingState,
                                ShippingStreet,
                                Sic,
                                SicDesc,
                                Site,
                                TickerSymbol,
                                Tradestyle,
                                Website,
                                YearStarted
                                FROM Account""")

opportunity_all_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity""")

opportunity_prospecting_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Prospecting'""")

opportunity_qualification_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Qualification'""")

opportunity_needsanalysis_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Needs Analysis'""")

opportunity_valueproposition_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Value Proposition'""")

opportunity_iddecisionmakers_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Id. Decision Makers'""")

opportunity_perceptionanalysis_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Perception Analysis'""")

opportunity_proposalpricequote_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Proposal/Price Quote'""")

opportunity_negotiationreview_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Negotiation/Review'""")

opportunity_closedwon_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Closed Won'""")

opportunity_closedlost_results=sf.query_all("""SELECT 
                                    AccountId, 
                                    Amount,
                                    CloseDate,
                                    Description,
                                    ExpectedRevenue,
                                    Fiscal,
                                    FiscalQuarter,
                                    FiscalYear,
                                    LastActivityDate,
                                    LastReferencedDate,
                                    LastStageChangeDate,
                                    LastViewedDate,
                                    Name,
                                    NextStep,
                                    Probability,
                                    TotalOpportunityQuantity,
                                    Type,
                                    StageName
                                    FROM Opportunity WHERE StageName = 'Closed Lost'""")

@api.route('/contact', methods=['GET'])
def get_contact():
    return json.dumps(contact_results)
  
@api.route('/campagin/all', methods=['GET'])
def get_campagin_all():
    return json.dumps(campagin_all_results)
  
@api.route('/campagin/new', methods=['GET'])
def get_campagin_new():
    return json.dumps(campagin_new_results)
  
@api.route('/campagin/progress', methods=['GET'])
def get_campagin_progress():
    return json.dumps(campagin_progress_results)
  
@api.route('/campagin/completed', methods=['GET'])
def get_campagin_completed():
    return json.dumps(campagin_completed_results)
  
@api.route('/campagin/aborted', methods=['GET'])
def get_campagin_aborted():
    return json.dumps(campagin_aborted_results)
  
@api.route('/lead/all', methods=['GET'])
def get_lead_all():
    return json.dumps(lead_all_results)
  
@api.route('/lead/notcontacted', methods=['GET'])
def get_lead_notcontacted():
    return json.dumps(lead_notcontacted_results)
  
@api.route('/lead/contacted', methods=['GET'])
def get_lead_contacted():
    return json.dumps(lead_contacted_results)
  
@api.route('/lead/converted', methods=['GET'])
def get_lead_converted():
    return json.dumps(lead_converted_results)
  
@api.route('/lead/notconverted', methods=['GET'])
def get_lead_notconverted():
    return json.dumps(lead_notconverted_results)
  
@api.route('/account', methods=['GET'])
def get_account():
    return json.dumps(account_results)
  
@api.route('/opportunity/all', methods=['GET'])
def get_opportunity_all():
    return json.dumps(opportunity_all_results)
  
@api.route('/opportunity/prospecting', methods=['GET'])
def get_opportunity_prospecting():
    return json.dumps(opportunity_prospecting_results)
  
@api.route('/opportunity/qualification', methods=['GET'])
def get_opportunity_qualification():
    return json.dumps(opportunity_qualification_results)
  
@api.route('/opportunity/needsanalysis', methods=['GET'])
def get_opportunity_needsanalysis():
    return json.dumps(opportunity_needsanalysis_results)
  
@api.route('/opportunity/valueproposition', methods=['GET'])
def get_opportunity_valueproposition():
    return json.dumps(opportunity_valueproposition_results)
  
@api.route('/opportunity/iddecisionmakers', methods=['GET'])
def get_opportunity_iddecisionmakers():
    return json.dumps(opportunity_iddecisionmakers_results)
  
@api.route('/opportunity/perceptionanalysis', methods=['GET'])
def get_opportunity_perceptionanalysis():
    return json.dumps(opportunity_perceptionanalysis_results)
  
@api.route('/opportunity/proposalpricequote', methods=['GET'])
def get_opportunity_proposalpricequote():
    return json.dumps(opportunity_proposalpricequote_results)
  
@api.route('/opportunity/negotiationreview', methods=['GET'])
def get_opportunity_negotiationreview():
    return json.dumps(opportunity_negotiationreview_results)
  
@api.route('/opportunity/closedwon', methods=['GET'])
def get_opportunity_closedwon():
    return json.dumps(opportunity_closedwon_results)
  
@api.route('/opportunity/closedlost', methods=['GET'])
def get_opportunity_closedlost():
    return json.dumps(opportunity_closedlost_results)
  
@api.route('/campaign_fields', methods=['GET'])
def get_campaign_fields():
  desc = sf.Campaign.describe()
  fields = []
  field_labels = [field['label'] for field in desc['fields']]
  field_names =  [field['name']  for field in desc['fields']]
  field_types =  [field['type']  for field in desc['fields']]
  for label, name, dtype in zip(field_labels, field_names, field_types):
      fields.append((label, name, dtype))
  fields = pd.DataFrame(fields,
                        columns = ['label','name','type'])
  field_list = ', '.join(list(fields['name']))
  return json.dumps(field_list)

@api.route('/lead_fields', methods=['GET'])
def get_lead_fields():
  desc = sf.Lead.describe()
  fields = []
  field_labels = [field['label'] for field in desc['fields']]
  field_names =  [field['name']  for field in desc['fields']]
  field_types =  [field['type']  for field in desc['fields']]
  for label, name, dtype in zip(field_labels, field_names, field_types):
      fields.append((label, name, dtype))
  fields = pd.DataFrame(fields,
                        columns = ['label','name','type'])
  field_list = ', '.join(list(fields['name']))
  return json.dumps(field_list)

@api.route('/account_fields', methods=['GET'])
def get_account_fields():
  desc = sf.Account.describe()
  fields = []
  field_labels = [field['label'] for field in desc['fields']]
  field_names =  [field['name']  for field in desc['fields']]
  field_types =  [field['type']  for field in desc['fields']]
  for label, name, dtype in zip(field_labels, field_names, field_types):
      fields.append((label, name, dtype))
  fields = pd.DataFrame(fields,
                        columns = ['label','name','type'])
  field_list = ', '.join(list(fields['name']))
  return json.dumps(field_list)

@api.route('/contact_fields', methods=['GET'])
def get_contact_fields():
  desc = sf.Contact.describe()
  fields = []
  field_labels = [field['label'] for field in desc['fields']]
  field_names =  [field['name']  for field in desc['fields']]
  field_types =  [field['type']  for field in desc['fields']]
  for label, name, dtype in zip(field_labels, field_names, field_types):
      fields.append((label, name, dtype))
  fields = pd.DataFrame(fields,
                        columns = ['label','name','type'])
  field_list = ', '.join(list(fields['name']))
  return json.dumps(field_list)

@api.route('/opportunity_fields', methods=['GET'])
def get_opportunity_fields():
  desc = sf.Opportunity.describe()
  fields = []
  field_labels = [field['label'] for field in desc['fields']]
  field_names =  [field['name']  for field in desc['fields']]
  field_types =  [field['type']  for field in desc['fields']]
  for label, name, dtype in zip(field_labels, field_names, field_types):
      fields.append((label, name, dtype))
  fields = pd.DataFrame(fields,
                        columns = ['label','name','type'])
  field_list = ', '.join(list(fields['name']))
  return json.dumps(field_list)

@api.route('/query_by_fields', methods=['GET'])
def api_query_by_fields():
    if 'field' in request.args:
        field = request.args['field']
    else:
        return "Error: No id field provided. Please specify an id."
    if 'obj' in request.args:
        obj = request.args['obj']
    else:
        return "Error: No id field provided. Please specify an id."

    query = 'SELECT ' + field + ''' FROM ''' + obj
    contact_results=sf.query_all(query) 

    return jsonify(contact_results)

if __name__ == '__main__':
    api.run()
