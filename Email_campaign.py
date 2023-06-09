import os

import sib_api_v3_sdk
from main import *

def create_contact_list(name):
    configuration = cofigure_client(os.getenv('BREVO_KEY'), 'brevo')
    contact_instance  = sib_api_v3_sdk.ContactsApi(sib_api_v3_sdk.ApiClient(configuration))
    limit = 10
    offset = 0
    try:
        api_response = contact_instance.get_folders(limit, offset)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ContactsApi->get_folders: %s\n" % e)
    folder_id = int(input("In which folder (id)    do you want to crete your list in ?:"))
    create_list = sib_api_v3_sdk.CreateList(name=name, folder_id=folder_id)
    try:
        api_response = contact_instance.create_list(create_list)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ContactsApi->create_list: %s\n" % e)

def import_contacts_into_list():
    configuration = cofigure_client(os.getenv('BREVO_KEY'), 'brevo')
    contact_instance = sib_api_v3_sdk.ContactsApi(sib_api_v3_sdk.ApiClient(configuration))
    request_contact_import = sib_api_v3_sdk.RequestContactImport()
    request_contact_import.json_body = [{'email': 'saimrao49@gmail.com', 'lastname': 'Saim','firstname':'Rao'},
                                        {'email': 'hassan@sjcurve.com', 'lastname': 'Muhhammad','firstname':'Hassan'}]
    # request_contact_import.file_url = "https://docs.google.com/spreadsheets/d/1iD3OGYd-fRv7BQ1cA_gIEiVSSwaz0sSWzOhuukB6xSk/edit?usp=drive_link"
    limit = 10
    offset = 0

    try:
        api_response = contact_instance.get_lists(limit=limit, offset=offset)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ListsApi->get_lists: %s\n" % e)
    list_id = input("In what list do you want to import contacts(provide a single id or comma separated ids):")
    input_list = list_id.split(",")  # Split the string into individual values
    integer_list = [int(num) for num in input_list]
    print(type(integer_list),integer_list)
    request_contact_import.list_ids = integer_list
    request_contact_import.email_blacklist = False
    request_contact_import.sms_blacklist = False
    request_contact_import.update_existing_contacts = True
    request_contact_import.empty_contacts_attributes = False
    try:
        api_response = contact_instance.import_contacts(request_contact_import)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling ContactsApi->import_contacts: %s\n" % e)


def create_email_campaign(
        tag,sender,name,template_id,subject,reply_to,to_feild,recipients,header,
        footer,attach_url=None,inline_image=False,mirror_active=False,utm_campaign=None,scheduled_at=None):
    configuration = cofigure_client(os.getenv('BREVO_KEY'), 'brevo')
    campaign_instance = sib_api_v3_sdk.EmailCampaignsApi(sib_api_v3_sdk.ApiClient(configuration))
    email_campaigns = sib_api_v3_sdk.CreateEmailCampaign(tag=tag, sender=sender, name=name, template_id=template_id,
                                                         scheduled_at=scheduled_at, subject=subject, reply_to=reply_to,
                                                         to_field=to_feild, recipients=recipients,
                                                         attachment_url=attach_url,
                                                         inline_image_activation=inline_image,
                                                         mirror_active=mirror_active, header=header, footer=footer,
                                                         utm_campaign=utm_campaign)
    try:
        api_response = campaign_instance.create_email_campaign(email_campaigns)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling EmailCampaignsApi->create_email_campaign: %s\n" % e)

def send_email_campaign():
    configuration = cofigure_client(os.getenv('BREVO_KEY'), 'brevo')
    campaign_instance = sib_api_v3_sdk.EmailCampaignsApi(sib_api_v3_sdk.ApiClient(configuration))
    type = 'classic'
    limit = 100
    offset = 0

    try:
        api_response = campaign_instance.get_email_campaigns(type=type,limit=limit, offset=offset,exclude_html_content=True)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling EmailCampaignsApi->get_email_campaigns: %s\n" % e)
    campaign_id = int(input("Enter the id of campaign to be sent:"))
    try:
        campaign_instance.send_email_campaign_now(campaign_id)
    except ApiException as e:
        print("Exception when calling EmailCampaignsApi->send_email_campaign_now: %s\n" % e)





if __name__ == "__main__":
    # create_contact_list("test_list")
    # import_contacts_into_list()
    tag = 'myTag'
    sender = {"name": 'saim', "email": 'saim@sjcurve.com'}
    name = 'Test Campaign'
    template_id = 2
    # scheduled_at = "2021-03-25T15:10:00+05:30"
    subject = ' My Subject'
    reply_to = 'saim@sjcurve.com'
    to_field = 'examplename'
    recipients = {"listIds": [2]}
    # attachment_url = 'https://attachment.domain.com/myAttachmentFromUrl.jpg'
    inline_image_activation = False
    mirror_active = False
    header = 'If you are not able to see this mail, click {here}'
    footer = 'If you wish to unsubscribe from our newsletter, click {here}'
    utm_campaign = 'My utm campaign value'
    # create_email_campaign(
    #     tag=tag,sender=sender,name=name,template_id=template_id,subject=subject,
    #     reply_to=reply_to,to_feild=to_field,recipients=recipients,header=header,
    #     footer=footer,utm_campaign=utm_campaign
    # )
    send_email_campaign()