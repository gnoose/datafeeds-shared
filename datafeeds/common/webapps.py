# Please do not edit. As soon as stasis transactions are eliminated, this module will be deprecated.

from http import client
import json
from urllib import parse

from deprecation import deprecated

from datafeeds import config

MULTIPART_BOUNDARY = 'AaB03x'


@deprecated
def post(uri, params=None):
    if params is None:
        params = {}

    uri = _build_uri(uri)
    data = parse.urlencode(params).encode('utf-8')

    try:
        conn = _create_request()
        conn.request('POST', uri, data)
        response = conn.getresponse()
    except client.HTTPException as e:
        if e.code == 500:
            errors = json.loads(e.read().decode())
            if errors:
                raise Exception(errors['errors'])
        raise

    if response.code >= 400:
        raise Exception(response.code, response.reason, response.read())

    return json.loads(response.read().decode('utf-8'))


@deprecated
def _create_request():
    if "https" in config.WEBAPPS_DOMAIN:
        conn = client.HTTPSConnection(config.WEBAPPS_DOMAIN)
    else:
        conn = client.HTTPConnection(config.WEBAPPS_DOMAIN)

    return conn


@deprecated
def _build_uri(uri, params=None):
    if params is None:
        params = {}

    params['token'] = config.WEBAPPS_TOKEN
    if uri[0] != '/':
        uri = '/' + uri

    return '%s?%s' % (
        uri,
        parse.urlencode(params)
    )
