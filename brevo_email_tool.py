import os
import sib_api_v3_sdk
import time
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from pprint import pprint
import pandas as pd
from dotenv import load_dotenv
import json

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
            |-----------|-----------|----------||----------|
            |   name    |   email   |  status  ||  country |
            |-----------|-----------|----------||----------|
    """
    data = pd.read_csv(f"{file_name}.csv", usecols=['name','email','status','company_country','title','linkedin',
                                                    'city','location','company_name','company_linkedin',
                                                    'company_twitter','company_facebook','company_domain',
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
                            'DOMAIN':series_obj['company_domain'],'NUMBER_OF_EMPLOYEES':series_obj['number_of_employees'],
                            'YEARLY_REVENUES':series_obj['yearly_revenues'],'WEBSITE':series_obj['company_website'],
                            'LINKEDIN_HEADLINE':series_obj['linkedin_headline'],'TWITTER':series_obj['twitter'],
                            'COMPANY_FOUND':series_obj['company_founded_year']}, axis=1)

    filtered_data.drop(['name','status','lastname','firstname','company_country','title','linkedin','city',
                        'location','company_name','company_linkedin','company_twitter','company_facebook',
                        'company_domain','number_of_employees','yearly_revenues','country','linkedin_headline',
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
    """
        script process would be:
            - select folder
            - based on folder id, select any list in that folder
            - dump data into List
    """
    # Folder select
    ids,names = get_folders()
    folder_names = "\n".join(list(map(lambda x: f"{x[0]+1}- {x[1]}", enumerate(names))))

    print(folder_names)
    folder_idx = int(input("Please specify folder name from the above ones: "))
    folder_id = ids[folder_idx-1]

    # List select
    ids_lst, names_lst = get_lists(folder_id)
    list_names = "\n".join(list(map(lambda x: f"{x[0]}- {x[1]}", zip(ids_lst,names_lst))))

    print(list_names)

    list_id = int(input("Please specify list name from the above ones: "))
    file_name = str(input("Please enter file name: "))
    # dump data
    if list_id:
        dump_data_into_brevo(list_id)

    print("-------------")
    print("  All Done  ")
    print("-------------")

