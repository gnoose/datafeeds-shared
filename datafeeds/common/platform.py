import http
import json
import urllib

from datafeeds import config


class PlatformException(Exception):
    pass


def post(path, data, params=None, test_params=None):
    return _request('POST', path, data, params)


def _request(method, path, data, params):
    conn = _connection()
    url = _build_url(path, params)
    if data is None:
        headers = {}
    else:
        data = json.dumps(data)
        headers = {
            "Content-type": "application/json",
            "Accept": "*"
        }
    conn.request(method, url, data, headers=headers)
    response = conn.getresponse()
    if response.code >= 400:
        raise PlatformException(method, url, response.code, response.reason)
    return response


def _connection():
    return http.client.HTTPConnection(
        '%s:%s' % (config.PLATFORM_HOST, config.PLATFORM_PORT)
    )


def _build_url(url, params):
    query = {'format': 'json'}
    if params is not None:
        query.update(params)

    url = urllib.parse.quote(url.lstrip('/'))
    url = '/rest/%s?%s' % (url, urllib.parse.urlencode(query, doseq=True))
    return url
