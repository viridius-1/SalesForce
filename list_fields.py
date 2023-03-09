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

desc = sf.Campaign.describe()
fields = []
field_labels = [field['label'] for field in desc['fields']]
field_names =  [field['name']  for field in desc['fields']]
field_types =  [field['type']  for field in desc['fields']]
for label, name, dtype in zip(field_labels, field_names, field_types):
    fields.append((label, name, dtype))
fields = pd.DataFrame(fields,
                      columns = ['label','name','type'])
print(str(fields.shape[0]) + ' fields')

field_list = ', '.join(list(fields['name']))
print(field_list)