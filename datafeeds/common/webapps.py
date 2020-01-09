# Please do not edit. As soon as stasis transactions are eliminated, this module will be deprecated.

from http import client
import json
from urllib import parse
import logging

from deprecation import deprecated

from datafeeds import config

log = logging.getLogger(__name__)

MULTIPART_BOUNDARY = "AaB03x"


@deprecated(details="To be removed when we deprecate stasis transactions.")
def post(uri, params=None):
    if params is None:
        params = {}

    uri = _build_uri(uri)
    data = parse.urlencode(params).encode("utf-8")

    log.debug("Webapps Request: %s : %s", uri, data)

    try:
        conn = _create_request()
        conn.request("POST", uri, data)
        response = conn.getresponse()
    except client.HTTPException as e:
        if e.code == 500:
            errors = json.loads(e.read().decode())
            if errors:
                raise Exception(errors["errors"])
        raise

    if response.code >= 400:
        raise Exception(response.code, response.reason, response.read())

    data = response.read()
    log.debug("Webapps Response: %s : %s", response.code, data)
    return json.loads(data.decode("utf-8"))


@deprecated(details="To be removed when we deprecate stasis transactions.")
def _create_request():
    return client.HTTPSConnection(config.WEBAPPS_DOMAIN)


@deprecated(details="To be removed when we deprecate stasis transactions.")
def _build_uri(uri, params=None):
    if params is None:
        params = {}

    params["token"] = config.WEBAPPS_TOKEN
    if uri[0] != "/":
        uri = "/" + uri

    return "%s?%s" % (uri, parse.urlencode(params))
