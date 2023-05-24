from __future__ import print_function
from utils import *
from dotenv import load_dotenv
import os
load_dotenv()





def verify_emails(start,end):

    emails = load_data(r'C:\Users\saim rao\Downloads\pod_mental_emails-UK.csv')
    nb = cofigure_client(os.getenv('NEVERBOUNCE_KEY'),'neverbounce')
    try:
        job = nb.jobs_create(emails[start:end])
        resp = nb.jobs_parse(job['job_id'], auto_start=False)
        print('\u2713 job created successfully\n',job,'\n',resp)
    except Exception as e:
        print ('\u2717 error creating job', e)
    try:
        resp = nb.jobs_start(job_id=job['job_id'])
        progress = nb.jobs_status(job['job_id'])
        print(progress)
        while progress['job_status'] != 'complete':
            progress = nb.jobs_status(job['job_id'])
        print('\u2713 job completed successfully')
    except Exception as e:
        print('\u2717 error occurred in job completion','\n',e)
    try:
        jobs = nb.jobs_results(job_id=job['job_id'])
        f = open('results.csv', mode='wb')
        nb.jobs_download(job_id=job['job_id'], fd=f)
        f.close()
        verified_emails=[]
        for job in jobs:
            if job['verification']['result'] == 'valid':
                verified_emails.append(job['data'])
        print('\u2713 results fetched successfully')
        return verified_emails
    except Exception as e:
        print('\u2717 error fetching results', '\n', e)





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


def send_emails(to, headers,template_id=None,subject=None,HTML_content=None, sender=None,  reply_to=None, cc=None, bcc=None,
                params=None):
    configuration = cofigure_client(os.getenv('BREVO_KEY'),'brevo')
    email_api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
    # Create an instance of SendSmtpEmail with the provided parameters
    if template_id == None:
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, bcc=bcc, cc=cc, reply_to=reply_to, headers=headers,
                                                       html_content=HTML_content, sender=sender, subject=subject,params=params)
        try:
            # Send the transactional email using the configured client and the SendSmtpEmail instance
            api_response = email_api_instance.send_transac_email(send_smtp_email)
            pprint(api_response)
            return ('\u2713 emails sent successfully')
        except ApiException as e:
            print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
            return '\u2717 error in sending Emails', e
    else:
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, bcc=bcc, cc=cc, headers=headers,template_id=template_id)

        try:
            # Send the transactional email using the configured client and the SendSmtpEmail instance
            api_response = email_api_instance.send_transac_email(send_smtp_email)
            pprint(api_response)
            return ('\u2713 emails sent successfully')
        except ApiException as e:
            print("Exception when calling SMTPApi->send_transac_email: %s\n" % e)
            return '\u2717 error in sending Emails', e

def generate_report(limit,startdate,enddate,offset=0,email=None,event=None,tags=None,template_id=None):

        configuration = cofigure_client('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', 'brevo')
        report_api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
        if template_id == None:
            try:
                api_response = report_api_instance.get_email_event_report(limit=limit, offset=offset, start_date=startdate,
                                                                   end_date=enddate, email=email, event=event, tags=tags,
                                                                   template_id=template_id)
                pprint(api_response)
                return '\u2713 report generated successfully'
            except ApiException as e:
                print("Exception when calling SMTPApi->get_email_event_report: %s\n" % e)
                return '\u2717 error generating report', e
        else:
            try:
                api_response = report_api_instance.get_email_event_report(limit=limit, offset=offset,
                                                                          start_date=startdate,
                                                                          end_date=enddate,
                                                                          template_id=template_id)
                pprint(api_response)
                return '\u2713 report generated successfully'
            except ApiException as e:
                print("Exception when calling SMTPApi->get_email_event_report: %s\n" % e)
                return '\u2717 error generating report', e






if __name__ == "__main__":
    #
    # Define the email parameters
    subject = "test mail"
    html_content = "<html><body><h1>This is test transactional email</h1><br><p>Task: </p><a href='https://app.clickup.com/t/866aa5v4h'>Clickup task</a></body></html>"
    sender = {"name": "Saim Rao", "email": "saim@sjcurve.com"}
    to = [{"email": "saimrao49@gmail.com", "name": "saim"},{"email": "saimrao2408@gmail.com", "name": "saim"}]
    cc = [{"email": "example2@example2.com", "name": "Janice Doe"}]
    bcc = [{"name": "John Doe", "email": "example@example.com"}]
    reply_to = {"email": "saim@sjcurve.com", "name": "Saim Rao"}
    headers = {"Some-Custom-Name": "unique-id-1234"}
    params = {"parameter": "My param value", "subject": "New Subject"}

    print(verify_emails(41,44))
    print(send_emails( to,headers,2))
    print(generate_report(300,'2023-05-24','2023-05-24',template_id=2))



