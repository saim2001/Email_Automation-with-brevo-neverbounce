from __future__ import print_function
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from pprint import pprint
import neverbounce_sdk
import pandas as pd


def load_data(csv_path):
    # Specify the columns to read from the CSV
    columns = ['name', 'email']
    # Read the CSV file and select the desired columns
    data = pd.read_csv(csv_path, usecols=columns)
    # Convert the DataFrame to a dictionary
    data_dict = data.to_dict(orient='list')
    return data_dict

def configure_neverbounce_client(API_key):
    api_key = API_key
    # Configure the NeverBounce client using the provided API key
    client = neverbounce_sdk.client(api_key=api_key, timeout=30)
    # Get the account information using the configured client
    info = client.account_info()
    pprint(info)

def configure_Brevo_client(API_key):
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = API_key
    # Configure the Brevo client using the provided API key
    Email_api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
    account_api_instance = sib_api_v3_sdk.AccountApi(sib_api_v3_sdk.ApiClient(configuration))
    try:
        # Get the account information using the configured client
        api_response = account_api_instance.get_account()
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AccountApi->get_account: %s\n" % e)
    return Email_api_instance


'''

Subject: subject of the email (regular string)

HTML_content: HTML body of the message (string)

Sender: Object or dictionary of sender from whom email will be sent 

eg({"name":"Mary from MyShop", "email":"no-reply@myshop.com"})

to: List of email addresses and names (optional) of the recipients. For example,
[{"name":"Jimmy", "email":"jimmy98@example.com"}, {"name":"Joe", "email":"joe@example.com"}] (list of objects or dictionaries)

reply_to:Email (required), along with name (optional), on which transactional mail recipients will be able to reply back. For example,
{"email":"ann6533@example.com", "name":"Ann"} (object or dictionary)

headers: Pass the set of custom headers (not the standard headers) that shall be sent along the mail headers in the original email. 'sender.ip' header can be set (only for dedicated ip users) to mention the IP to be used for sending transactional emails. Headers are allowed in This-Case-Only (i.e. words separated by hyphen with first letter of each word in capital letter), they will be converted to such case styling if not in this format in the request payload. For example,
{"sender.ip":"1.2.3.4", "X-Mailin-custom":"some_custom_header", "idempotencyKey":"abc-123"} (object or dictionary)

params (optional): Pass the set of attributes to customize the template. For example, {"FNAME":"Joe", "LNAME":"Doe"} (object or dictionary)

api_instance: instance created from configuration client function for transactional email client 

cc (optional): List of email addresses and names (optional) of the recipients in cc (list of objects or dictionaries)

bcc (optional): List of email addresses and names (optional) of the recipients in bcc (list of objects or dictionaries)


'''


def send_emails(subject, HTML_content, sender, to, reply_to, headers, email_api_instance, cc=None, bcc=None,
                params=None):
    # Create an instance of SendSmtpEmail with the provided parameters
    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, bcc=bcc, cc=cc, reply_to=reply_to, headers=headers,
                                                   html_content=HTML_content, sender=sender, subject=subject)
    try:
        # Send the transactional email using the configured client and the SendSmtpEmail instance
        api_response = email_api_instance.send_transac_email(send_smtp_email)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)


if __name__ == "__main__":
    # Configure the Brevo client
    instance = configure_Brevo_client(
        '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')

    # Define the email parameters
    subject = "test mail"
    html_content = "<html><body><h1>This is test transactional email</h1><br><p>Task: </p><a href='https://app.clickup.com/t/866aa5v4h'>Clickup task</a></body></html>"
    sender = {"name": "Saim Rao", "email": "saim@sjcurve.com"}
    to = [{"email": "hassan@sjcurve.com", "name": "Hassan"}]
    cc = [{"email": "example2@example2.com", "name": "Janice Doe"}]
    bcc = [{"name": "John Doe", "email": "example@example.com"}]
    reply_to = {"email": "saim@sjcurve.com", "name": "Saim Rao"}
    headers = {"Some-Custom-Name": "unique-id-1234"}
    params = {"parameter": "My param value", "subject": "New Subject"}

    # Send the emails using the configured client and the email parameters
    send_emails(subject, html_content, sender, to, reply_to, headers, instance)

    # Configure the NeverBounce client
    configure_neverbounce_client('private_5f36cd0ca4d6864da9164f363fdad07e')

    # Load data from the CSV file and convert it to a dictionary
    data_dict = load_data(r'C:\Users\saim rao\Downloads\pod_mental_emails-UK.csv')