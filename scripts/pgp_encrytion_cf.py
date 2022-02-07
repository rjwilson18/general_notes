from google.oauth2 import id_token
from google.auth.transport.requests import Request
import requests

def trigger_dag(data, context=None):
    """Makes a POST request to the Composer DAG Trigger API

    When called via Google Cloud Functions (GCF),
    data and context are Background function parameters.

    For more info, refer to
    https://cloud.google.com/functions/docs/writing/background#functions_background_parameters-python
    
    client id can be found using this script: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/composer/rest/get_client_id.py

    To call this function from a Python script, omit the ``context`` argument
    and pass in a non-null value for the ``data`` argument.
    """
    #Define Constants
    DAG_NAME = 'pgp_encryption'
    COMPOSER_CLIENT_ID = ''
    WEBSERVER_ID = ''
    DAG_PATH =  '.appspot.com/api/experimental/dags/'
    DAG_RUN = '/dag_runs'
    SECURE_HTTP_PREFIX = 'https://'

    WEBSERVER_URL = (
        SECURE_HTTP_PREFIX
        + WEBSERVER_ID
        + DAG_PATH
        + DAG_NAME
        + DAG_RUN
    )
    # Make a POST request to IAP which then Triggers the DAG
    make_iap_request(
        WEBSERVER_URL, COMPOSER_CLIENT_ID, method='POST', json={"conf": data, "replace_microseconds": 'false'})


# This code is copied from
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/iap/make_iap_request.py
# START COPIED IAP CODE
def make_iap_request(url, client_id, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    #define constansts
    DEFAULT_TIMEOUT = 90
    
    # Set the default timeout, if missing
    if 'timeout' not in kwargs:
        kwargs['timeout'] = DEFAULT_TIMEOUT

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if resp.status_code == 403:
        raise Exception('Service account does not have permission to '
                        'access the IAP-protected application.')
    elif resp.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                resp.status_code, resp.headers, resp.text))
    else:
        return resp.text


def transunion_encryption(event, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    print('Event ID: {}'.format(context.event_id))
    print('Event type: {}'.format(context.event_type))
    print('File: {}'.format(event['name']))
    print('Created: {}'.format(event['timeCreated']))
    print('Updated: {}'.format(event['updated']))
    
    ALREADY_ENCRYPTED = event['name'].endswith('.gpg') or event['name'].endswith('.pgp')
    
    
    if ALREADY_ENCRYPTED:
        #file is already encrypted
        print("File {} is already encrypted and will be ignored".format(event['name']))
    else:
        print("encrypting file: {}".format(event['name']))
        #encypt file
        trigger_dag({"input_file":event['name']})
    
