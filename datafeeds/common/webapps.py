# Please do not edit. As soon as stasis transactions are eliminated, this module will be deprecated.

from http import client
import json
import textwrap
from urllib import parse
from typing import Dict

from datafeeds import config
from datafeeds.common.typing import BillingData, BillingDatum

MULTIPART_BOUNDARY = 'AaB03x'


def get(uri, params=None):
    return json.loads(getraw(uri, params))


def getraw(uri, params=None):
    url = _build_uri(uri, params=params)

    conn = _create_request()
    conn.request('GET', url)
    response = conn.getresponse()

    return response.read().decode('utf-8')


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


def delete(uri, params=None):
    uri = _build_uri(uri, params=params)

    conn = _create_request()
    conn.request('DELETE', uri)

    return conn.getresponse().read().decode('utf-8')


def _create_request():
    if "https" in config.WEBAPPS_DOMAIN:
        conn = client.HTTPSConnection(config.WEBAPPS_DOMAIN)
    else:
        conn = client.HTTPConnection(config.WEBAPPS_DOMAIN)

    return conn


def upload_json(uri, bodyobj):
    if uri[0] != '/':
        uri = '/' + uri
    uri += '?token=%s' % config.WEBAPPS_TOKEN
    conn = _create_request()
    conn.request('POST', uri, json.dumps(bodyobj), {
        'Content-Type': 'application/json'
    })
    response = conn.getresponse()
    if response.code != 200:
        raise RuntimeError(
            'Unable to upload json to %s%s: %s' % (config.WEBAPPS_DOMAIN, uri, response.read())
        )
    return json.loads(response.read().decode())


def upload_file(uri, formname, filebytes, mimetype):
    headers = {'Content-Type': 'multipart/form-data; boundary=' + MULTIPART_BOUNDARY}
    data = _multipart_file(formname, filebytes, mimetype)
    conn = _create_request()

    if uri[0] != '/':
        uri = '/%s' % uri

    uri = '%s?token=%s' % (uri, config.WEBAPPS_TOKEN)
    conn.request('POST', uri, data, headers)
    response = conn.getresponse()
    if response.code != 200:
        raise RuntimeError(
            'Unable to upload file to %s%s' % (config.WEBAPPS_DOMAIN, uri)
        )

    return json.loads(response.read().decode())


def list_to_post_data(data_list, prefix):
    """converts a list of dictionaries to post data for wtforms.fields.FieldList"""
    post_data = {}
    for i, list_item in enumerate(data_list):
        item_prefix = '%s-%d' % (prefix, i)
        for key, item in list_item.items():
            post_data['%s-%s' % (item_prefix, key)] = item
    return post_data


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


def _multipart_file(name, content, content_type):
    # yes, filename is required. http://www.w3.org/TR/html401/interact/forms.html#h-17.13.4.2
    rval = """\
    --{}
    Content-Disposition: form-data; name="{}"; filename="{}"
    Content-Type: {}

    {}
    --{}--"""
    rval = textwrap.dedent(rval)
    rval = rval.format(
        MULTIPART_BOUNDARY,
        name,
        name,
        content_type,
        content,
        MULTIPART_BOUNDARY
    )

    rval = rval.replace('\n', '\r\n')
    return rval


def billing_datum_to_webapps_params(bill: BillingDatum) -> Dict:
    """Convert a BillingDatum object into a set of webapps request parameters

    This handles things like ensuring attachments and line items are json-encoded strings. This is principally
    used by `post_billing_data` function in this module, to convert individual BillingDatum objects into param
    dicts as part of submitting a list of bills to webapps.

    Args:
        bill: A BillingDatum object representing information about an individual billing period

    Returns:
        A parameter dictionary mapping string keys to values
    """
    return {
        'start': bill.start.strftime('%Y-%m-%d'),
        'end': bill.end.strftime('%Y-%m-%d'),
        'cost': bill.cost,
        'used': bill.used,
        'peak': bill.peak,
        'line_items': json.dumps(
            [
                {
                    'description': item.description,
                    'quantity': item.quantity,
                    'rate': item.rate,
                    'total': item.total,
                    'kind': item.kind,
                    'unit': item.unit
                }
                for item in (bill.items or [])
            ]
        ),
        'attachments': json.dumps(
            [
                {
                    "key": attachment.key,
                    "kind": attachment.kind,
                    "format": attachment.format
                }
                for attachment in (bill.attachments or [])
            ]
        )
    }


def post_billing_data(account_id: str, meter_id: str, bd: BillingData) -> dict:
    non_nones = lambda b: {k: v for k, v in b.items() if v is not None}
    bill_params = [non_nones(billing_datum_to_webapps_params(b)) for b in bd]
    return post(
        '/accounts/%s/meters/%s/bills' % (account_id, meter_id),
        list_to_post_data(bill_params, 'bills')
    )
