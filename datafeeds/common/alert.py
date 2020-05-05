import logging

import slack
from sqlalchemy.orm import joinedload

from datafeeds import config, db
from datafeeds.models import (
    SnapmeterAccountDataSource,
    SnapmeterAccountMeter,
    SnapmeterMeterDataSource,
)


log = logging.getLogger("datafeeds")


def post_slack_message(message, channel, icon_emoji=None, username=None):
    """Send a message to a Slack channel."""
    if username is None:
        username = "Datafeeds"

    if not config.SLACK_TOKEN:
        log.info("post to %s: %s" % (channel, message))
        return
    client = slack.WebClient(token=config.SLACK_TOKEN)
    try:
        client.chat_postMessage(
            channel=channel, text=message, username=username, icon_emoji=icon_emoji
        )
    except Exception as e:
        log.error("Failed to post message to Slack: %s", e)


def disable_logins(acct_ds: SnapmeterAccountDataSource):
    """Send a Slack message listing meters that were disabled."""
    # get meter names and accounts for alert
    query = (
        db.session.query(SnapmeterAccountMeter)
        .filter(SnapmeterAccountMeter.meter == SnapmeterMeterDataSource._meter)
        .filter(SnapmeterMeterDataSource.account_data_source == acct_ds)
        .options(joinedload(SnapmeterAccountMeter.account_obj))
    )
    meter_list = []
    for sam in query:
        meter_list.append(
            "\t%s %s (%s)\n" % (sam.account_obj.name, sam.meter_obj.name, sam.meter)
        )
    if not meter_list:
        return
    msg = (
        'Login failed for <a href="https://snapmeter.com/admin/accounts/%s/utility-logins">%s'
        "</a>; disabled scrapers for meters:\n%s"
        % (acct_ds.account.hex_id, acct_ds.name, "\n".join(meter_list),)
    )
    post_slack_message(
        msg, "#scraper-logins", ":exclamation:", username="Scraper monitor"
    )
