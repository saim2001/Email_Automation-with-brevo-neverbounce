import os
import sib_api_v3_sdk
import time
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from pprint import pprint
import pandas as pd
from dotenv import load_dotenv
import json
import datetime

# initialize env variables
load_dotenv()

# Access configuration
configuration = sib_api_v3_sdk.Configuration()
configuration.api_key['api-key'] = os.getenv("api-key")

# contacts API instance
api_instance = sib_api_v3_sdk.ContactsApi(sib_api_v3_sdk.ApiClient(configuration))


def create_list(list_name, folder_id):
    """ function to create list inside folder """
    create_list = sib_api_v3_sdk.CreateList(name=list_name, folder_id=folder_id)
    try:
        response = api_instance.create_list(create_list)
        parsed_data = response.to_dict()
        print("✔ List succesfully created")
        return parsed_data.get('id')
    except:
        print("❌ Error while creating list")


def get_folders():
    """ function to get folder names """
    try:
        response = api_instance.get_folders(50, 0)
        parsed_data = response.to_dict()

        data_dict = {'ids':[], 'names':[]}
        for record in parsed_data.get('folders'):
            data_dict.get('ids').append(record.get('id'))
            data_dict.get('names').append(record.get('name'))

        return data_dict.get('ids'), data_dict.get('names')
    except:
        return
    
def get_folders_a():
    """ function to get folder names """
    try:
        response = api_instance.get_folders(50, 0)
        parsed_data = response.to_dict()
        data_dict = {}
        for record in parsed_data.get('folders'):
            data_dict[record.get('name')] = record.get('id')

        return data_dict
    except:
        return

def get_lists(folder_id):
    """ function to get lists names """
    try:
        response = api_instance.get_folder_lists(folder_id)
        parsed_data = response.to_dict()
        data_dict = {'ids':[], 'names':[]}
        for record in parsed_data.get('lists'):
            data_dict.get('ids').append(record.get('id'))
            data_dict.get('names').append(record.get('name'))

        return data_dict.get('ids'), data_dict.get('names')
    except:
        return
    
def get_lists_a(folder_id):
    """ function to get lists names """
    try:
        response = api_instance.get_folder_lists(folder_id)
        parsed_data = response.to_dict()
        data_dict = {}
        for record in parsed_data.get('lists'):
            data_dict[record.get('name')] = record.get('id')

        return data_dict
    except:
        return

def get_all_attr():
    try:
        api_response = api_instance.get_attributes()
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling AttributesApi->get_attributes: %s\n" % e)

def create_attribute(category,name,type):
    """ function to create a new attribute for contacts """

    create_attribute = sib_api_v3_sdk.CreateAttribute()
    create_attribute.type = type

    try:
        api_instance.create_attribute(category, name,create_attribute)
    except ApiException as e:
        print("Exception when calling ContactsApi->create_attribute: %s\n" % e)

def dump_data_into_brevo(list_id):
    """
        info: this function will filtered data, & then push into brevo
        csv file format:
           Required columns for the CSV file:
            'name': Name of the individual
            'email': Email address
            'status': Current status
            'company_country': Country of the company
            'title': Job title
            'linkedin': LinkedIn profile
            'city': City
            'location': Location
            'company_name': Name of the company
            'company_linkedin': Company's LinkedIn profile
            'company_twitter': Company's Twitter handle
            'company_facebook': Company's Facebook page
            'domain': Company's domain
            'number_of_employees': Number of employees in the company
            'yearly_revenues': Yearly revenues of the company
            'country': Country
            'linkedin_headline': Headline on LinkedIn
            'twitter': Twitter handle
            'company_website': Company's website
            'company_founded_year': Year the company was founded
            'state': State
    """
    data = pd.read_csv(f"To_insert/verified_output_emails.csv", usecols=['name','email','status','company_country','title','linkedin',
                                                    'city','location','company_name','company_linkedin',
                                                    'company_twitter','company_facebook','domain',
                                                    'number_of_employees','yearly_revenues','country','linkedin_headline',
                                                    'twitter','company_website','company_founded_year','state'])
    filtered_data = data[data['status'].isin(['catchall','valid'])].copy()

    split_name = filtered_data['name'].str.split()
    filtered_data['firstname'] = split_name.str.get(0)
    filtered_data['lastname'] = split_name.str.get(-1)


    filtered_data['attributes'] = filtered_data.apply(
        lambda series_obj: {'FIRSTNAME': series_obj['firstname'],
                            'LASTNAME':series_obj['lastname'],
                            'COUNTRY':series_obj['country'],'JOB_TITLE':series_obj['title'],
                            'LINKEDIN_PROFILE':series_obj['linkedin'],'CITY':series_obj['city'],
                            'STATE':series_obj['state'],'COMPANY':series_obj['company_name'],
                            'COMPANY_LINKEDIN':series_obj['company_linkedin'],
                            'COMPANY_TWITTER':series_obj['company_twitter'],'COMPANY_FACEBOOK':series_obj['company_facebook'],
                            'DOMAIN':series_obj['domain'],'NUMBER_OF_EMPLOYEES':series_obj['number_of_employees'],
                            'YEARLY_REVENUES':series_obj['yearly_revenues'],'WEBSITE':series_obj['company_website'],
                            'LINKEDIN_HEADLINE':series_obj['linkedin_headline'],'TWITTER':series_obj['twitter'],
                            'COMPANY_FOUND':series_obj['company_founded_year']}, axis=1)

    filtered_data.drop(['name','status','lastname','firstname','company_country','title','linkedin','city',
                        'location','company_name','company_linkedin','company_twitter','company_facebook',
                        'domain','number_of_employees','yearly_revenues','country','linkedin_headline',
                                                    'twitter','company_website','company_founded_year','state'],axis=1,inplace=True)

    parsed_data = json.loads(filtered_data.to_json(orient="records"))

    # print(parsed_data)

    # dump into brevo
    request_contact_import = sib_api_v3_sdk.RequestContactImport(json_body=parsed_data, list_ids=[list_id])

    try:
        api_response = api_instance.import_contacts(request_contact_import)
        print("✔ Records successfully added into the List, total count: ",len(parsed_data))
    except Exception as e:
        print("❌ Error while loading data into list",e)



# main driver code
if __name__ == "__main__":
  
    df = pd.read_csv(r'To_insert/input_emails.csv')
    folder_names = get_folders_a()
    print(folder_names)
    job_title_column = df.iloc[:, df.columns.str.lower().isin(['title'])]
    country_column = df.iloc[:, df.columns.str.lower().isin(['country'])]
    print(job_title_column)
    print(country_column)
    job_title = str(job_title_column.iloc[0])
    country = str(country_column.iloc[0])
    print(job_title)
    print(type(country))
    folder_name = None
    for key,value in folder_names.items():
        if (key[:-1] in job_title) or (key in job_title) or (key.lower() in job_title):
            folder_name = value
            print(key,value)
        words = job_title.split()
        first_letters = ''.join(word[0] for word in words)
        if first_letters in key or first_letters.lower() in key:
            folder_name = value
            print(key,value)
    print(folder_name)
    lists = get_lists_a(folder_name)
    list_id = None
    for key,value in lists.items():
        if 'masterlist' in key and country != "Nigeria":

            list_id = value
            print(key,value)
            break
            
        elif 'nigeria' in key and country == 'Nigeria':
            list_id = value
            print(key,value)
            break
    

    

    # dump data
    if list_id:
        dump_data_into_brevo(30)

    print("-------------")
    print("  All Done  ")
    print("-------------")

    source_path = r'To_insert/verified_output_emails.csv'

    # Specify the destination directory
    destination_dir = r'Inserted/'

    timestamp = datetime.datetime.now()
    formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M')
    str_timestamp = str(formatted_timestamp).replace(' ','_').replace(':','-')

    # Specify the new filename
    new_filename = f'inserted_emails_{str_timestamp}.csv'

    # Construct the new file path by joining the destination directory and the new filename
    new_file_path = os.path.join(destination_dir, new_filename)

    # Rename the file
    os.rename(source_path, new_file_path)

    # Move the file to the destination directory
    os.replace(new_file_path, os.path.join(destination_dir, new_filename))
