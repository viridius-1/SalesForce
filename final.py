import flask
from flask import request, jsonify
from flask import Flask
import json
from datetime import datetime
import arrow
from pandas.errors import ParserError

from simple_salesforce import Salesforce
import requests
import pandas as pd
from io import StringIO
import httplib2
import pygsheets
from sqlalchemy.sql import text

sf = Salesforce(
            username='yossi@bpersonal.ai', 
            password = 'bP3rs@nal!',
            security_token='DHzCRT5uNDDa4Zi7KD4naaWXX',
            )

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Distant Reading Archive</h1>
<p>A prototype API for distant reading of science fiction novels.</p>'''

@app.route('/query_by_fields', methods=['GET', 'POST'])
def api_query_by_fields():
    data = json.loads(request.data)
    field = data.get("field", None)
    obj = data.get("obj", None)
    if field is None and obj is None:
        return jsonify({"message":"field not found"})
    else:
        query = 'SELECT ' + field + ''' FROM ''' + obj
        results=sf.query_all(query)
    return jsonify(results)

@app.route('/contact', methods=['GET', 'POST'])
def api_contact():
    obj = 'Contact'
    query = 'SELECT ' + 'Id, IsDeleted, MasterRecordId, AccountId, LastName, FirstName, Salutation, Name, OtherStreet, OtherCity, OtherState, OtherPostalCode, OtherCountry, OtherLatitude, OtherLongitude, OtherGeocodeAccuracy, OtherAddress, MailingStreet, MailingCity, MailingState, MailingPostalCode, MailingCountry, MailingLatitude, MailingLongitude, MailingGeocodeAccuracy, MailingAddress, Phone, Fax, MobilePhone, HomePhone, OtherPhone, AssistantPhone, ReportsToId, Email, Title, Department, AssistantName, LeadSource, Birthdate, Description, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastCURequestDate, LastCUUpdateDate, LastViewedDate, LastReferencedDate, EmailBouncedReason, EmailBouncedDate, IsEmailBounced, PhotoUrl, Jigsaw, JigsawContactId, CleanStatus, IndividualId, Level__c, Languages__c' + ''' FROM ''' 

    data = json.loads(request.data)
    if data is None:
        query = query + obj
    else:
        query = query + obj + ''' WHERE Id!=NULL '''
        for key in data.keys():
            if isinstance(data[key], dict)== False:
                if data[key].__class__.__name__ == "str":
                    try:
                        parsed_dt = arrow.get(data[key])
                        if data[key] != parsed_dt.format('YYYY-MM-DD'):
                            date_format_ok = False
                        else:
                            date_format_ok = True
                    except ValueError:
                        date_format_ok = False

                    if not date_format_ok:
                        query = query + ''' AND ''' + "" + key + "" + "'" + data[key] + "'"
                    else:
                        query = query + ''' AND ''' + "" + key + "" + "" + data[key] + ""

                if data[key].__class__.__name__ == "int":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

                if data[key].__class__.__name__ == "float":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

    results=sf.query_all(query) 
    
    return jsonify(results)

@app.route('/campaign', methods=['GET', 'POST'])
def api_campagin():
    obj = 'Campaign'
    query = 'SELECT ' + 'Id, IsDeleted, Name, ParentId, Type, Status, StartDate, EndDate, ExpectedRevenue, BudgetedCost, ActualCost, ExpectedResponse, NumberSent, IsActive, Description, NumberOfLeads, NumberOfConvertedLeads, NumberOfContacts, NumberOfResponses, NumberOfOpportunities, NumberOfWonOpportunities, AmountAllOpportunities, AmountWonOpportunities, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, CampaignMemberRecordTypeId' + ''' FROM '''

    data = json.loads(request.data)
    if data is None:
        query = query + obj
    else:
        query = query + obj + ''' WHERE Id!=NULL '''
        for key in data.keys():
            if isinstance(data[key], dict)== False:
                if data[key].__class__.__name__ == "str":
                    try:
                        parsed_dt = arrow.get(data[key])
                        if data[key] != parsed_dt.format('YYYY-MM-DD'):
                            date_format_ok = False
                        else:
                            date_format_ok = True
                    except ValueError:
                        date_format_ok = False

                    if not date_format_ok:
                        query = query + ''' AND ''' + "" + key + "" + "'" + data[key] + "'"
                    else:
                        query = query + ''' AND ''' + "" + key + "" + "" + data[key] + ""

                if data[key].__class__.__name__ == "int":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

                if data[key].__class__.__name__ == "float":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

    results=sf.query_all(query) 
    
    return jsonify(results)

@app.route('/lead', methods=['GET', 'POST'])
def api_lead():
    obj = 'Lead'
    query = 'SELECT ' + 'Id, IsDeleted, MasterRecordId, LastName, FirstName, Salutation, Name, Title, Company, Street, City, State, PostalCode, Country, Latitude, Longitude, GeocodeAccuracy, Address, Phone, MobilePhone, Fax, Email, Website, PhotoUrl, Description, LeadSource, Status, Industry, Rating, AnnualRevenue, NumberOfEmployees, OwnerId, IsConverted, ConvertedDate, ConvertedAccountId, ConvertedContactId, ConvertedOpportunityId, IsUnreadByOwner, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, Jigsaw, JigsawContactId, CleanStatus, CompanyDunsNumber, DandbCompanyId, EmailBouncedReason, EmailBouncedDate, IndividualId, SICCode__c, ProductInterest__c, Primary__c, CurrentGenerators__c, NumberofLocations__c' + ''' FROM '''

    data = json.loads(request.data)
    if data is None:
        query = query + obj
    else:
        query = query + obj + ''' WHERE Id!=NULL '''
        for key in data.keys():
            if isinstance(data[key], dict)== False:
                if data[key].__class__.__name__ == "str":
                    try:
                        parsed_dt = arrow.get(data[key])
                        if data[key] != parsed_dt.format('YYYY-MM-DD'):
                            date_format_ok = False
                        else:
                            date_format_ok = True
                    except ValueError:
                        date_format_ok = False

                    if not date_format_ok:
                        query = query + ''' AND ''' + "" + key + "" + "'" + data[key] + "'"
                    else:
                        query = query + ''' AND ''' + "" + key + "" + "" + data[key] + ""

                if data[key].__class__.__name__ == "int":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

                if data[key].__class__.__name__ == "float":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

    results=sf.query_all(query) 
    
    return jsonify(results)

@app.route('/opportunity', methods=['GET', 'POST'])
def api_opportunity():
    obj = 'Opportunity'
    query = 'SELECT ' + 'Id, IsDeleted, AccountId, IsPrivate, Name, Description, StageName, Amount, Probability, ExpectedRevenue, TotalOpportunityQuantity, CloseDate, Type, NextStep, LeadSource, IsClosed, IsWon, ForecastCategory, ForecastCategoryName, CampaignId, HasOpportunityLineItem, Pricebook2Id, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastStageChangeDate, FiscalQuarter, FiscalYear, Fiscal, ContactId, LastViewedDate, LastReferencedDate, HasOpenActivity, HasOverdueTask, LastAmountChangedHistoryId, LastCloseDateChangedHistoryId, DeliveryInstallationStatus__c, TrackingNumber__c, OrderNumber__c, CurrentGenerators__c, MainCompetitors__c' + ''' FROM '''
    
    data = json.loads(request.data)
    if data is None:
        query = query + obj
    else:
        query = query + obj + ''' WHERE Id!=NULL '''
        for key in data.keys():
            if isinstance(data[key], dict)== False:
                if data[key].__class__.__name__ == "str":
                    try:
                        parsed_dt = arrow.get(data[key])
                        if data[key] != parsed_dt.format('YYYY-MM-DD'):
                            date_format_ok = False
                        else:
                            date_format_ok = True
                    except ValueError:
                        date_format_ok = False

                    if not date_format_ok:
                        query = query + ''' AND ''' + "" + key + "" + "'" + data[key] + "'"
                    else:
                        query = query + ''' AND ''' + "" + key + "" + "" + data[key] + ""

                if data[key].__class__.__name__ == "int":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

                if data[key].__class__.__name__ == "float":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

    results=sf.query_all(query) 
    
    return jsonify(results)

@app.route('/account', methods=['GET', 'POST'])
def api_account():
    obj = 'Account'
    query = 'SELECT ' + 'Id, IsDeleted, MasterRecordId, Name, Type, ParentId, BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, BillingLatitude, BillingLongitude, BillingGeocodeAccuracy, BillingAddress, ShippingStreet, ShippingCity, ShippingState, ShippingPostalCode, ShippingCountry, ShippingLatitude, ShippingLongitude, ShippingGeocodeAccuracy, ShippingAddress, Phone, Fax, AccountNumber, Website, PhotoUrl, Sic, Industry, AnnualRevenue, NumberOfEmployees, Ownership, TickerSymbol, Description, Rating, Site, OwnerId, CreatedDate, CreatedById, LastModifiedDate, LastModifiedById, SystemModstamp, LastActivityDate, LastViewedDate, LastReferencedDate, Jigsaw, JigsawCompanyId, CleanStatus, AccountSource, DunsNumber, Tradestyle, NaicsCode, NaicsDesc, YearStarted, SicDesc, DandbCompanyId, OperatingHoursId, CustomerPriority__c, SLA__c, Active__c, NumberofLocations__c, UpsellOpportunity__c, SLASerialNumber__c, SLAExpirationDate__c' + ''' FROM '''

    data = json.loads(request.data)
    if data is None:
        query = query + obj
    else:
        query = query + obj + ''' WHERE Id!=NULL '''
        for key in data.keys():
            if isinstance(data[key], dict)== False:
                if data[key].__class__.__name__ == "str":
                    try:
                        parsed_dt = arrow.get(data[key])
                        if data[key] != parsed_dt.format('YYYY-MM-DD'):
                            date_format_ok = False
                        else:
                            date_format_ok = True
                    except ValueError:
                        date_format_ok = False

                    if not date_format_ok:
                        query = query + ''' AND ''' + "" + key + "" + "'" + data[key] + "'"
                    else:
                        query = query + ''' AND ''' + "" + key + "" + "" + data[key] + ""

                if data[key].__class__.__name__ == "int":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

                if data[key].__class__.__name__ == "float":
                    query = query + ''' AND ''' + "" + key + "" + "" + str(data[key]) + ""

    results=sf.query_all(query) 
    
    return jsonify(results)

@app.route('/object_fields', methods=['GET', 'POST'])
def get_campaign_fields():
    data = json.loads(request.data)
    obj = data.get("obj",None)
    if obj is None:
        return jsonify({"message":"obj not found"})
    else:
        match obj:
            case "Campaign":
                desc = sf.Campaign.describe()
            case "Lead":
                desc = sf.Lead.describe()
            case "Account":
                desc = sf.Account.describe()
            case "Contact":
                desc = sf.Contact.describe()
            case "Opportunity":
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
        return jsonify(field_list) 

@app.route('/campaign_insert', methods=['POST'])
def insert_data():
    data = json.loads(request.data)
    if data is None:
        return "message:Data not found"
    else:
        # query = 'INSERT ' + '''INTO Campaign ''' + "("
        # for key in data.keys():
        #     if isinstance(data[key], dict)== False:
        #         query = query + key
        # query = query + ")" + ''' VALUE ''' + "("
        # for key in data.keys():
        #     if isinstance(data[key], dict)== False:
        #         query = query + data[key]
        # query = query + ")"
        query = 'INSERT ' + '''INTO Campaign (CreatedById)''' + ''' VALUE (0054x000005ifDVAAY)'''

    results=sf.query_all(query) 
    
    return jsonify(results)

app.run()

