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

def email_cleaner(file):

    email_patterns = [
    "{first}.{last}@{domain}",
    "{first}{last}@{domain}",
    "{first}@{domain}"
    ]

    refined_database = []
    try:
        data = pd.read_csv(f"{file}")
        filtered_df = data[~(data['domain'].isnull())]
        filtered_df_orgnl = data[~(data['email'].isnull())]
        
        # filtered_df = data[~(data['domain'].isnull())][900:]

        for idx, row in filtered_df.iterrows():
            if type(row['email'])==float:
                print(f"<-> Initiated for row {idx}")
                data_dict = row.to_dict()
                for i in range(3):
                    data_dict_copy = data_dict.copy()
                    data_dict_copy['email'] = email_patterns[i].format(first=str(row['first_name']).lower().replace("-",""), last=str(row['last_name']).lower().replace("-",""), domain=row['domain'])
                    refined_database.append(data_dict_copy)

        # print(refined_database)
        final_df = pd.DataFrame(refined_database)
        print(final_df)
        new_dataset = pd.concat([filtered_df_orgnl,final_df],axis=0)
        new_dataset.to_csv(f"{file}", index=False)
        print('✔ Emails cleaned!')
        return 0
    except Exception as e:
        print("❌ Could'nt clean emails",e)
        return 1

def is_country_code(value):
    try:
        pycountry.countries.get(alpha_2=value)
        return True
    except LookupError:
        return False

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
        if code.isupper():
            country = pycountry.countries.get(alpha_2=code)
        else:
            country = pycountry.countries.get(alpha_2=dtr(code).upper())
        return country.name
    except AttributeError:
        return code

def merge_csv(file_1,file_2,outputname,*columns):

    df_1 = pd.read_csv(file_1)
    df_2 = pd.read_csv(file_2)

    merged_df = pd.merge(df_1, df_2[list(columns)], on='email', how='left')
    merged_df.to_csv(f'To_insert/{outputname}', index=False)
    return 0

def insert_full_country_name(file,column_name):
    try:
        dt_map = {'Country':str}
        df = pd.read_csv(file,dtype=dt_map,low_memory=False)
        df[column_name] = df[column_name].apply(get_country_name)
        df.to_csv(file,index=False)
        return 0
    except Exception as e:
        print(e)
        return 1


def verify_emails(file):
    # Load data from a CSV file
    emails = load_data(file)

    # Configure the NeverBounce client using the provided environment variable
    nb = cofigure_client(os.getenv('NEVERBOUNCE_KEY'), 'neverbounce')

    try:
        # Create a job to verify the emails
        job = nb.jobs_create(emails)
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


    email_cleaner(r'To_insert\input_emails.csv')
    print(verify_emails(r'To_insert\input_emails.csv'))
    merge_csv(r'To_insert\input_emails.csv',
              'results.csv','verified_output_emails.csv'
              ,'email','status')
    format_file(r'To_insert/verified_output_emails.csv')
    contains_country_code = False
    contains_full_country_name = False
    df = pd.read_csv(r'To_insert/verified_output_emails.csv')
    country_column = df.iloc[:, df.columns.str.lower().isin(['country'])]
    print(country_column)
    for value in country_column:
        print(value)
        print(is_country_code(value))
        if is_country_code(value):
            contains_country_code = True
        else:
            contains_full_country_name = True
    print(contains_country_code)
    if contains_country_code:
        insert_full_country_name(r'To_insert/verified_output_emails.csv',country_column)
        print('done')
    
    
   




