from __future__ import print_function
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from pprint import pprint
import neverbounce_sdk
import pandas as pd

# def configure_neverbounce_client(API_key):
#     api_key = API_key
#     # Configure the NeverBounce client using the provided API key
#     client = neverbounce_sdk.client(api_key=api_key, timeout=30)
#     # Get the account information using the configured client
#     info = client.account_info()
#     pprint(info)
#
#
#
# def configure_Brevo_client(API_key):
#     configuration = sib_api_v3_sdk.Configuration()
#     configuration.api_key['api-key'] = API_key
#     # Configure the Brevo client using the provided API key
#     Email_api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
#     account_api_instance = sib_api_v3_sdk.AccountApi(sib_api_v3_sdk.ApiClient(configuration))
#     try:
#         # Get the account information using the configured client
#         api_response = account_api_instance.get_account()
#         pprint(api_response)
#     except Exception as e:
#         print("Exception when calling AccountApi->get_account: %s\n" % e)
#     return Email_api_instance

def load_data(csv_path):
    # Specify the columns to read from the CSV
    columns = ['name', 'email']
    # Read the CSV file and select the desired columns
    data = pd.read_csv(csv_path, usecols=columns)
    # Convert the DataFrame to a dictionary
    data_dict = data.to_dict(orient='list')

    result_lst = []

    for i,j in zip(data_dict['name'],data_dict['email']):
        temp_dict = {}
        if type(j) != float:
            temp_dict['name'] = i
            temp_dict['email'] = j
            result_lst.append(temp_dict)


    return result_lst

def cofigure_client(API_key,tool):
    if tool == 'neverbounce':
        api_key = API_key
        try:
            # Configure the NeverBounce client using the provided API key
            client = neverbounce_sdk.client(api_key=api_key, timeout=30)
            # Get the account information using the configured client
            info = client.account_info()
            print('\u2713 Client created successfully')
            pprint(info)

            return client
        except Exception as e:
            return ('\u2717 Error configuring client',e)
    elif tool == 'brevo':

        try:
            configuration = sib_api_v3_sdk.Configuration()
            configuration.api_key['api-key'] = API_key
            # Configure the Brevo client using the provided API key

            account_api_instance = sib_api_v3_sdk.AccountApi(sib_api_v3_sdk.ApiClient(configuration))

            # Get the account information using the configured client
            api_response = account_api_instance.get_account()
            print('\u2713 Client created successfully')
            pprint(api_response)
            return configuration
        except Exception as e:
            return ('\u2717 Error configuring client','\n',e)
    else:
        print('Wrong arguments')
        return 1




