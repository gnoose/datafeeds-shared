import json
import logging

from datafeeds.common import webapps

log = logging.getLogger(__name__)

UPLOAD_DATA_BATCH_SIZE = 20


def upload_data(data, account_id, meter_id, dst_strategy='none'):
    """Upload formatted interval data to platform. This handles batching
    the upload to maximize efficiency and general error handling around it.
    The interval format is:

    {
        '%Y-%m-%d': [96]
    }

    IE:
    {
        '2017-04-02' : [59.1, 30.2,...]
    }
    """

    data_to_upload = {}
    batch_number = 0
    response = webapps.post('/transactions/create', {'target': meter_id})
    transaction_oid = response['oid']

    for key in data.keys():
        data_to_upload[key] = data[key]
        if len(data_to_upload) == UPLOAD_DATA_BATCH_SIZE:
            log.debug(
                "Uploading %d-%d of %d" % (
                    batch_number * UPLOAD_DATA_BATCH_SIZE,
                    (batch_number * UPLOAD_DATA_BATCH_SIZE) + UPLOAD_DATA_BATCH_SIZE,
                    len(data)
                )
            )

            webapps.post(
                '/accounts/%s/meters/%s/readings' % (account_id, meter_id),
                {'transaction': transaction_oid, 'readings': json.dumps(data_to_upload), 'dstStrategy': dst_strategy}
            )

            data_to_upload = {}
            batch_number += 1

    if data_to_upload:
        log.debug("Uploading last data batch")
        webapps.post(
            '/accounts/%s/meters/%s/readings' % (account_id, meter_id),
            {'transaction': transaction_oid, 'readings': json.dumps(data_to_upload), 'dstStrategy': dst_strategy}
        )

    webapps.post('/transactions/commit', {'oid': transaction_oid})
