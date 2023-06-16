from __future__ import print_function
from utils import *
from dotenv import load_dotenv
import os
import pandas as pd
import pycountry
load_dotenv()


'''
API Documentations
Brevo: https://developers.brevo.com/reference/getemaileventreport-1
Neverbounce: https://developers.neverbounce.com/v4.0/reference/jobs-status
'''
def format_file(file):
    deesired_columns = ['name', 'email', 'status', 'company_country', 'title', 'linkedin',
               'city', 'location', 'company_name', 'company_linkedin',
               'company_twitter', 'company_facebook', 'company_domain',
               'number_of_employees', 'yearly_revenues', 'country', 'linkedin_headline',
               'twitter', 'company_website', 'company_founded_year', 'state']
    try:
        df = pd.read_csv(file)
        missing_cols = [col for col in deesired_columns if col not in df.columns]
        if len(missing_cols)>0:
            df = pd.concat([df,pd.DataFrame(columns=missing_cols)])
            df.to_csv(file,index=False)
        return 0
    except Exception as e:
        print(e)
        return 1

def get_country_name(code):
    try:
        country = pycountry.countries.get(alpha_2=code)
        return country.name
    except AttributeError:
        return code

def merge_csv(file_1,file_2,outputname,*columns):

    df_1 = pd.read_csv(file_1)
    df_2 = pd.read_csv(file_2)

    merged_df = pd.merge(df_1, df_2[list(columns)], on='email', how='left')
    merged_df.to_csv(outputname, index=False)
    return 0

def insert_full_country_name(file,column_name):
    try:
        df = pd.read_csv(file)
        df[column_name] = df[column_name].apply(get_country_name)
        df.to_csv(file,index=False)
        return 0
    except Exception as e:
        print(e)
        return 1


def verify_emails(file,start, end):
    # Load data from a CSV file
    emails = load_data(file)

    # Configure the NeverBounce client using the provided environment variable
    nb = cofigure_client(os.getenv('NEVERBOUNCE_KEY'), 'neverbounce')

    try:
        # Create a job to verify the emails
        job = nb.jobs_create(emails[start:end])
        resp = nb.jobs_parse(job['job_id'], auto_start=False)
        print('\u2713 job created successfully\n', job, '\n', resp)
    except Exception as e:
        print('\u2717 error creating job', e)

    try:
        # Start the job and wait for it to complete
        resp = nb.jobs_start(job_id=job['job_id'])
        progress = nb.jobs_status(job['job_id'])
        print(progress)
        while progress['job_status'] != 'complete':
            progress = nb.jobs_status(job['job_id'])
        print('\u2713 job completed successfully')
    except Exception as e:
        print('\u2717 error occurred in job completion', '\n', e)

    try:
        # Retrieve the results of the job and store them in a CSV file
        jobs = nb.jobs_results(job_id=job['job_id'])
        f = open('results.csv', mode='wb')
        nb.jobs_download(job_id=job['job_id'], fd=f)
        f.close()

        df = pd.read_csv('results.csv')
        df.columns = ['name','email','status']
        df.to_csv('results.csv',index=False)

        print('\u2713 results fetched successfully')
        return 0

    except Exception as e:
        print('\u2717 error fetching results', '\n', e)
        return 1




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


def send_emails(to, headers, template_id=None, subject=None, HTML_content=None, sender=None, reply_to=None, cc=None, bcc=None, params=None):
    # Configure the Brevo client using the provided environment variable
    configuration = cofigure_client(os.getenv('BREVO_KEY'), 'brevo')
    email_api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

    # Create an instance of SendSmtpEmail with the provided parameters
    if template_id == None:
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, bcc=bcc, cc=cc, reply_to=reply_to, headers=headers, html_content=HTML_content, sender=sender, subject=subject, params=params)
        try:
            # Send the transactional email using the configured client and the SendSmtpEmail instance
            api_response = email_api_instance.send_transac_email(send_smtp_email)
            pprint(api_response)
            return ('\u2713 emails sent successfully')
        except ApiException as e:
            print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
            return '\u2717 error in sending Emails', e
    else:
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, bcc=bcc, cc=cc, headers=headers, template_id=template_id)
        try:
            # Send the transactional email using the configured client and the SendSmtpEmail instance
            api_response = email_api_instance.send_transac_email(send_smtp_email)
            pprint(api_response)
            return ('\u2713 emails sent successfully')
        except ApiException as e:
            print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
            return '\u2717 error in sending Emails', e


def generate_report(limit, startdate, enddate, offset=0, email=None, event=None, tags=None, template_id=None):
    # Configure the Brevo client using the provided environment variable
    configuration = cofigure_client(os.getenv('BREVO_KEY'), 'brevo')
    report_api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

    if template_id == None:
        try:
            # Generate the email event report using the configured client and the provided parameters
            api_response = report_api_instance.get_email_event_report(limit=limit, offset=offset, start_date=startdate, end_date=enddate, email=email, event=event, tags=tags, template_id=template_id)
            pprint(api_response)
            return '\u2713 report generated successfully'
        except ApiException as e:
            print("Exception when calling SMTPApi->get_email_event_report: %s\n" % e)
            return '\u2717 error generating report', e
    else:
        try:
            # Generate the email event report using the configured client and the provided parameters
            api_response = report_api_instance.get_email_event_report(limit=limit, offset=offset, start_date=startdate, end_date=enddate, template_id=template_id)
            pprint(api_response)
            return '\u2713 report generated successfully'
        except ApiException as e:
            print("Exception when calling SMTPApi->get_email_event_report: %s\n" % e)
            return '\u2717 error generating report', e


if __name__ == "__main__":

    # print(verify_emails(r'C:\Users\saim rao\Downloads\OT_emails_verified (1).csv',0,227))
    # merge_csv(r'C:\Users\saim rao\Downloads\pod_mental_emails-UK (2).csv',
    #           'pod_mental_emails-UK (2)_vresults.csv','pod_mental_emails-UK (2)_vresults.csv'
    #           ,'email','status')
    format_file('pod_mental_emails-UK (2)_vresults.csv')
    # insert_full_country_name('pod_mental_emails-UK (2)_vresults.csv.csv','company_country')
    #
    # print(send_emails(to,headers,subject=subject,HTML_content=html_content,sender=sender,reply_to=reply_to,))
    # print(generate_report(300, '2023-05-24', '2023-05-24', template_id=2))



