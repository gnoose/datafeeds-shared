from collections import defaultdict
from datetime import datetime, timedelta
import re
from typing import Dict, Optional
import xml.etree.ElementTree as ET

from dateutil import tz


class IntervalTypeException(Exception):
    pass


class IncompleteIntervalException(Exception):
    pass


class MixedDurationException(Exception):
    pass


class MonthlyDataException(IntervalTypeException):
    def __init__(self):
        msg = "Unable to parse file that contains monthly data"
        super(MonthlyDataException, self).__init__(msg)


class UnsupportedDurationException(Exception):
    pass


class UnsupportedServiceException(Exception):
    pass


class UnsupportedUnitException(Exception):
    pass


class Units:
    """
    GB can report values in various units of measurement (uoms), so we need to whitelist
    and explicitly handle them to help avoid making assumptions about calculations to perform.

    Mimic an enum without using stdlib enum, which is only Python 3.4+. Intended usage:
     - Check inclusion, eg: `if Units.has(uom)`
     - Compare, eg: `if uom == Units.WATTS`
    """

    THERMS = 169
    WATTS = 38
    WATT_HOURS = 72

    ALL = [THERMS, WATTS, WATT_HOURS]

    @classmethod
    def has(cls, uom):
        """Check if uom code (eg, 38) is a whitelisted unit"""
        return uom in cls.ALL

    @classmethod
    def is_demand(cls, uom):
        return uom in [cls.WATTS]

    @classmethod
    def is_usage(cls, uom):
        return uom in [cls.WATT_HOURS, cls.THERMS]


class ServiceCategories:
    """
    Whitelist the acceptable service categories for GB files
    """

    ELECTRIC = 0
    GAS = 1

    ALL = [ELECTRIC, GAS]

    @classmethod
    def has(cls, service_category):
        return service_category in cls.ALL


class GBCTree:
    def __init__(self, filepath):
        self.tree = ET.parse(filepath)

    def root(self):
        return GBCNode(self.tree)


class GBCNode:
    NAMESPACES = {
        "gb": "http://www.w3.org/2005/Atom",
        "espi": "http://naesb.org/espi",
        "cust": "http://naesb.org/espi/customer",
    }

    def __init__(self, node):
        self.node = node

    def is_empty(self):
        return self.node is None

    def find(self, selector):
        return GBCNode(self.node.find(selector, namespaces=self.NAMESPACES))

    def findall(self, selector):
        return GBCNodeList(self.node.findall(selector, namespaces=self.NAMESPACES))

    def find_content(self, selector):
        return GBCNode(
            self.node.find(
                "./gb:entry/gb:content/%s" % selector, namespaces=self.NAMESPACES
            )
        )

    def findall_content(self, selector):
        return GBCNodeList(
            self.node.findall(
                "./gb:entry/gb:content/%s" % selector, namespaces=self.NAMESPACES
            )
        )

    def get(self, field, default):
        if self.node is None:
            return default

        return getattr(self, field)

    def __getattr__(self, attr):
        return getattr(self.node, attr)


class GBCNodeList:
    def __init__(self, node_list):
        self.node_list = [GBCNode(node) for node in node_list]

    def __iter__(self):
        for node in self.node_list:
            yield node


def customer_model(filepath):  # noqa: C901
    tree = GBCTree(filepath)
    root = tree.root()

    entries = root.findall("gb:entry")

    models = {}
    p = re.compile(
        ".*/RetailCustomer/([a-zA-Z0-9]*)/Customer/(.*)/CustomerAccount/"
        + "([a-zA-Z0-9]*)/CustomerAgreement/([a-zA-Z0-9]*)"
    )

    usage_search = lambda x: x["usage_point"] == point
    account_search = lambda x: x["gb_customer_account"] == acct

    for entry in entries:
        links = entry.findall("gb:link")
        for link in links:
            href = link.attrib["href"]
            m = p.match(href)
            if m:
                cust = m.group(1)
                acct = m.group(3)
                point = m.group(4)

                if cust not in models:
                    models[cust] = {
                        "gb_retail_customer": cust,
                        "gb_customer_accounts": [],
                    }

                gb_customer_accounts = models[cust]["gb_customer_accounts"]
                gb_customer_account = next(
                    filter(account_search, gb_customer_accounts), None
                )
                if not gb_customer_account:
                    gb_customer_account = {
                        "gb_customer_account": acct,
                        "gb_customer_agreements": [],
                    }
                    gb_customer_accounts.append(gb_customer_account)
                gb_customer_agreements = gb_customer_account["gb_customer_agreements"]

                gb_customer_agreement = next(
                    filter(usage_search, gb_customer_agreements), None
                )
                if not gb_customer_agreement:
                    gb_customer_agreements.append({"usage_point": point})

    acct_pattern = re.compile(
        ".*/RetailCustomer/([a-zA-Z0-9]*)/Customer/(.*)/CustomerAccount/([a-zA-Z0-9]*)"
    )
    cust_pattern = re.compile(".*/RetailCustomer/([a-zA-Z0-9]*)/.*")

    for entry in entries:
        customer_node = entry.find("gb:content/cust:Customer")
        if customer_node.node:
            customer = customer_node.find("cust:name").text
            links = entry.findall("gb:link")
            for link in links:
                href = link.attrib["href"]
                match = cust_pattern.match(href)
                if match:
                    cust = match.group(1)
                    model = models[cust]
                    model["name"] = customer

        customer_account = entry.find("gb:content/cust:CustomerAccount")
        if customer_account.node:
            name = customer_account.find("cust:name").text
            links = entry.findall("gb:link")
            for link in links:
                href = link.attrib["href"]
                match = acct_pattern.match(href)
                if match:
                    cust = match.group(1)
                    acct = match.group(3)
                    model = models[cust]
                    accounts = model["gb_customer_accounts"]
                    account = next(filter(account_search, accounts), None)
                    account.update({"name": name})

    for entry in entries:
        links = entry.findall("gb:link")
        for link in links:
            href = link.attrib["href"]
            m = p.match(href)
            if m:
                cust = m.group(1)
                acct = m.group(3)
                point = m.group(4)

                model = models[cust]
                accounts = model["gb_customer_accounts"]
                account = next(filter(account_search, accounts))
                gb_agreement = next(
                    filter(usage_search, account["gb_customer_agreements"])
                )

                cust_agreement = entry.find("gb:content/cust:CustomerAgreement")
                if cust_agreement.node:
                    node = cust_agreement.find("cust:name")
                    if node.node is not None:
                        said = node.text
                        gb_agreement.update({"sa_id": said})

                service_location = entry.find("gb:content/cust:ServiceLocation")
                if service_location.node:
                    gb_agreement.update({"address": _address(service_location)})
                else:
                    gb_agreement.update(
                        {
                            "address": {
                                "street1": "",
                                "street2": "",
                                "city": "",
                                "state": "",
                                "zip": "",
                            }
                        }
                    )

    return list(models.values())


def _address(service_location):
    address2 = ""

    address_node = service_location.find(
        "cust:mainAddress/cust:streetDetail/cust:addressGeneral"
    )

    if address_node is not None and address_node.node is not None:
        address1 = address_node.text
        address_2_node = service_location.find(
            "cust:mainAddress/cust:streetDetail/cust:addressGeneral2"
        )
        if address_2_node is not None and address_2_node.node is not None:
            address2 = address_2_node.text
    else:
        number_node = service_location.find(
            "cust:mainAddress/cust:streetDetail/cust:number"
        )
        name_node = service_location.find(
            "cust:mainAddress/cust:streetDetail/cust:name"
        )
        address_parts = []
        for addr_node in [number_node, name_node]:
            if addr_node is not None and addr_node.node is not None:
                text = None
                try:
                    text = addr_node.text or ""
                except Exception:
                    pass
                if text:
                    address_parts.append(text)
        address1 = " ".join([str(x) for x in address_parts]).strip()

    city = service_location.find("cust:mainAddress/cust:townDetail/cust:name").text
    state = service_location.find(
        "cust:mainAddress/cust:townDetail/cust:stateOrProvince"
    ).text

    code = ""
    code_node = service_location.find("cust:mainAddress/cust:townDetail/cust:code")
    if code_node is not None and code_node.node is not None:
        code = code_node.text
    else:
        code_node = service_location.find("cust:mainAddress/cust:postalCode")
        if code_node is not None and code_node.node is not None:
            code = code_node.text

    return {
        "street1": address1,
        "street2": address2,
        "city": city,
        "state": state,
        "zip": code.strip(),
    }


def parse_summary(filepath):
    """
    Parse bills from a Usage Summary file
    """
    tree = GBCTree(filepath)
    root = tree.root()
    return _bills(root)


def parse_customer(filepath):  # noqa: C901
    """
    Parse services/locations from a file with a single customer
    """
    tree = GBCTree(filepath)
    root = tree.root()

    entries = root.findall("gb:entry")
    account = ""
    customer = ""
    account_map = {}
    for entry in entries:
        customer_account = entry.find("gb:content/cust:CustomerAccount")
        if customer_account.node:
            account = customer_account.find("cust:name").text
            links = entry.findall("gb:link")
            for link in links:
                if link.attrib["rel"] == "self":
                    href = link.attrib["href"]
                    index = href.rfind("/")
                    start = index + 1
                    gb_account = href[start:]
                    account_map[gb_account] = account
        customer_node = entry.find("gb:content/cust:Customer")
        if customer_node.node:
            customer = customer_node.find("cust:name").text

    points = defaultdict(lambda: {"customer": customer})
    for entry in entries:  # pylint: disable=too-many-nested-blocks
        usage_point = _usage_point(entry)
        if usage_point is not None:
            cust_agreement = entry.find("gb:content/cust:CustomerAgreement")
            if cust_agreement.node:
                name = cust_agreement.find("cust:name")
                if name.node is None:
                    continue
                said = name.text
                links = entry.findall("gb:link")
                for link in links:
                    if link.attrib["rel"] == "up":
                        href = link.attrib["href"]
                        paths = href.split("/")
                        gb_account = paths[-2]
                        account = account_map.get(gb_account, "unknown")
                usage_point = _usage_point(entry)
                points[usage_point].update(
                    {"usage_point": usage_point, "said": said, "account": account}
                )

            service_location = entry.find("gb:content/cust:ServiceLocation")
            if service_location.node:
                address2 = ""

                address_node = service_location.find(
                    "cust:mainAddress/cust:streetDetail/cust:addressGeneral"
                )

                if address_node is not None and address_node.node is not None:
                    address = address_node.text
                    address_2_node = service_location.find(
                        "cust:mainAddress/cust:streetDetail/cust:addressGeneral2"
                    )
                    if address_2_node is not None and address_2_node.node is not None:
                        address2 = address_2_node.text
                else:
                    number_node = service_location.find(
                        "cust:mainAddress/cust:streetDetail/cust:number"
                    )
                    name_node = service_location.find(
                        "cust:mainAddress/cust:streetDetail/cust:name"
                    )
                    address_parts = []
                    for addr_node in [number_node, name_node]:
                        if addr_node is not None and addr_node.node is not None:
                            text = None
                            try:
                                text = addr_node.text or ""
                            except Exception:
                                pass
                            if text:
                                address_parts.append(text)
                    address = " ".join([str(x) for x in address_parts]).strip()

                town_detail = service_location.find("cust:mainAddress/cust:townDetail")

                if town_detail.node:
                    city = town_detail.find("cust:name").text
                    state = town_detail.find("cust:stateOrProvince").text

                    code = ""
                    code_node = town_detail.find("cust:code")
                    if code_node is not None and code_node.node is not None:
                        code = code_node.text
                    else:
                        code_node = town_detail.find("cust:postalCode")
                        if code_node is not None and code_node.node is not None:
                            code = code_node.text

                    points[usage_point].update(
                        {
                            "street1": address,
                            "street2": address2,
                            "city": city,
                            "state": state,
                            "zip": code.strip(),
                        }
                    )

    saids = dict()
    for value in points.values():
        if "said" in value:
            saids[value["said"]] = value
    return saids


def _usage_point(entry):
    p = re.compile(".*CustomerAccount/([a-zA-Z0-9]*)/CustomerAgreement/([a-zA-Z0-9]*)")
    links = entry.findall("gb:link")
    for link in links:
        href = link.attrib["href"]
        m = p.match(href)
        if m:
            return m.group(2)
    return None


def _parse_gas(root):
    readings = {}

    reading_type = root.find_content("espi:ReadingType")
    power_of_ten = int(reading_type.find("espi:powerOfTenMultiplier").text)
    uom = int(reading_type.find("espi:uom").text)

    interval_blocks = root.findall_content("espi:IntervalBlock")

    for i, interval_block in enumerate(interval_blocks):
        for j, interval in enumerate(interval_block.findall("espi:IntervalReading")):
            duration = int(interval.find("espi:timePeriod/espi:duration").text)
            start_ts = int(interval.find("espi:timePeriod/espi:start").text)
            start = datetime.fromtimestamp(start_ts)
            day = start.strftime("%Y-%m-%d")
            reading = int(interval.find("espi:value").text)

            if duration == 86400:
                size = 1
                index = 0
            elif duration == 3600:
                size = 24
                index = int(start.hour)
            else:
                raise IntervalTypeException(
                    "NOT IMPLEMENTED: Duration must daily or hourly gas data."
                )
            if day not in readings:
                readings[day] = [None] * size

            readings[day][index] = _convert_reading(power_of_ten, uom, reading)

    return readings


def _parse_interval_blocks(root, blocks, logger=None):
    readings: Dict[str, Optional[dict]] = {}

    reading_type = root.find_content("espi:ReadingType")
    power_of_ten = int(reading_type.find("espi:powerOfTenMultiplier").text)
    uom = int(reading_type.find("espi:uom").text)

    for interval_block in blocks:
        for interval in interval_block.findall("espi:IntervalReading"):
            duration = int(interval.find("espi:timePeriod/espi:duration").text)
            minutes = int(duration / 60)

            if minutes != 15:
                if minutes / (60 * 24) > 28:
                    raise MonthlyDataException()
                elif minutes == 5:
                    # Should this log somewhere that it gets downsampled?
                    pass
                else:
                    raise IntervalTypeException(
                        "NOT IMPLEMENTED: Duration must be a 15 minute interval. Is %d minutes long"
                        % minutes
                    )

            start_ts = int(interval.find("espi:timePeriod/espi:start").text)
            start = datetime.fromtimestamp(start_ts)
            day = start.strftime("%Y-%m-%d")

            if day not in readings:
                readings[day] = [None] * 96

            reading = int(interval.find("espi:value").text)
            reading = _convert_reading(power_of_ten, uom, reading)

            if reading is None:
                continue

            # Convert the timestamp to 15-min index
            # NOTE: 5-min intervals will get assigned to a 15-min interval block,
            # eg: 1:30, 1:35, and 1:40 will all get assigned to 1:30
            index = int((start.hour * 4) + (start.minute / 15))

            # Multiple readings can be assigned to same interval, so once all readings have
            # been parsed various calcs will be performed (summed or averaged based on
            # reading type) and some might be filtered out (eg. if 15-min and 5-min both show
            # up in the same interval)
            if not readings[day][index]:
                readings[day][index] = []

            readings[day][index].append({"reading": reading, "interval": minutes})

    _aggregate_readings(readings, uom, logger=logger)

    return readings


def _aggregate_readings(readings, uom, logger=None):
    # Aggregate readings for all intervals
    for day in readings:
        for index, interval_readings in enumerate(readings[day]):
            # No guarantees that the readings returned included every interval,
            # in which case there will be None stored for a certain time of day
            if not interval_readings:
                continue

            readings[day][index] = _aggregate_reading(
                interval_readings, uom, day, index, logger=logger
            )


def _aggregate_reading(interval_readings, uom, day, index, logger=None):
    interval_lengths = set(r["interval"] for r in interval_readings)

    # Only use 15-min interval if both intervals are present
    if interval_lengths == {5, 15}:
        interval_readings = [r for r in interval_readings if r["interval"] == 15]

    # Other mixed intervals are not yet supported
    elif len(interval_lengths) > 1:
        raise MixedDurationException(
            "Interval block {} for {} has mixed reading intervals: {}".format(
                index, day, interval_lengths
            )
        )

    # 5-min data SHOULD always have 3 readings within a 15-min interval,
    # but sometimes this isn't the case (there might be a single one dropped
    # somewhere) so rather than fail out completely, so warn and drop others
    elif interval_lengths == {5} and len(interval_readings) < 3:
        msg = "5-min intervals do not fully cover interval {} on {}".format(index, day)

        if logger:
            logger.warning(msg)
        else:
            print("WARNING: {}".format(msg))

        return None

    # Usage can simply be summed...
    reading = sum(r["reading"] for r in interval_readings)

    # ... but demand needs to be averaged
    if Units.is_demand(uom):
        reading = len(interval_readings)

    return reading


def parse_multi(filepath, parse_bills=True):  # noqa: C901
    """
    Parse GB file that includes multiple customers and usage points, eg. SCE
    """
    tree = GBCTree(filepath)
    root = tree.root()

    entries = root.findall("gb:entry")

    reading_types = {}
    customers = defaultdict(set)
    usage_entries = {}
    interval_block_entries = defaultdict(list)
    usage_summaries = defaultdict(list)
    p = re.compile(".*/RetailCustomer/([a-zA-Z0-9]*)/UsagePoint/([a-zA-Z0-9]*)/.*")

    usage_pattern = re.compile(
        ".*/RetailCustomer/([a-zA-Z0-9]*)/UsagePoint/([a-zA-Z0-9]*)/UsageSummary/([a-zA-Z0-9]*)"
    )

    for entry in entries:
        reading_type_entry = entry.find("gb:content/espi:ReadingType")
        if reading_type_entry.node:
            links = entry.findall("gb:link")
            for link in links:
                if link.attrib["rel"] == "self":
                    href = link.attrib["href"]
                    reading_type = href.rsplit("/", 1)[-1]
                    reading_types[reading_type] = entry

    for entry in entries:
        usage_content = entry.find("gb:content/espi:UsagePoint")
        if usage_content.node:
            links = entry.findall("gb:link")
            for link in links:
                if link.attrib["rel"] == "self":
                    href = link.attrib["href"]
                    point = href.rsplit("/", 1)[-1]
                    kind = usage_content.find("espi:ServiceCategory/espi:kind")
                    if int(kind.node.text) == 0:
                        usage_entries[point] = "kw"
                    elif int(kind.node.text) == 1:
                        usage_entries[point] = "gas"
                    else:
                        usage_entries[point] = "unknown"

    for entry in entries:
        content = entry.find("gb:content/espi:IntervalBlock")
        if content.node:
            links = entry.findall("gb:link")
            for link in links:
                if link.attrib["rel"] == "self":
                    href = link.attrib["href"]
                    m = p.match(href)
                    if m:
                        customer = m.group(1)
                        point = m.group(2)
                        customers[customer].add(point)
                        interval_block_entries[point].append(content)

    for entry in entries:  # pylint: disable=too-many-nested-blocks
        content = entry.find("gb:content/espi:UsageSummary")
        if content.node:
            links = entry.findall("gb:link")
            for link in links:
                if link.attrib["rel"] == "self":
                    href = link.attrib["href"]
                    m = usage_pattern.match(href)
                    if m:
                        customer = m.group(1)
                        point = m.group(2)
                        if point not in customers[customer]:
                            customers[customer].add(point)
                        usage_summaries[point].append(content)

    rval = {}
    for customer in customers:
        rval[customer] = defaultdict(dict)
        for point in customers[customer]:
            try:
                readings = _parse_interval_blocks(root, interval_block_entries[point])

                if parse_bills:
                    bills = _bills(root, usage_summaries[point])
                else:
                    bills = []

                rval[customer][point]["readings"] = readings
                rval[customer][point]["bills"] = bills

            except IntervalTypeException:
                # print("Point {0} doesn't have 15 minute intervals".format(point))
                pass

    return rval


def parse(filepath, parse_bills=True, parse_readings=True):
    """
    Parse bills and/or interval readings from a file for a single customer/usage point
    """
    tree = GBCTree(filepath)
    root = tree.root()

    rval = {}

    if parse_readings:
        usage_point = root.find_content("espi:UsagePoint")
        if usage_point.node:

            category = int(
                root.find_content("espi:UsagePoint/espi:ServiceCategory/espi:kind").text
            )

            if not ServiceCategories.has(category):
                raise UnsupportedServiceException(
                    'Service category "{}" is unsupported'.format(category)
                )

            if category == ServiceCategories.ELECTRIC:
                interval_blocks = root.findall_content("espi:IntervalBlock")
                rval["readings"] = _parse_interval_blocks(root, interval_blocks)

            elif category == ServiceCategories.GAS:
                rval["readings"] = _parse_gas(root)

            else:
                raise UnsupportedServiceException(
                    "Unhandled service category: {}".format(category)
                )

    if parse_bills:
        rval["bills"] = _bills(root)

    return rval


def unify_bills(bills):
    """De-duplicate bills based on their date range.

    SCE Green Button has multiple Usage Summaries for the same bill,
    so we need to aggregate the non-null fields into one record giving
    a complete picture of the bill.
    """
    date_groups = defaultdict(dict)
    for b in bills:
        start = b["start"]
        end = b["end"]
        group = date_groups[(start, end)]

        # Update with data that is not null-like (where null-like
        # means None, empty string, or 0).
        for k, v in b.items():
            if k not in group or (v is not None and v != "" and v != 0):
                group[k] = v

    return list(date_groups.values())


def _bills(root, usage_summaries=None):
    bills = []

    if not usage_summaries:
        usage_summaries = root.findall_content("espi:UsageSummary")

    for summary in usage_summaries:
        # billingPeriod
        duration_node = summary.find("espi:billingPeriod/espi:duration")
        start_ts_node = summary.find("espi:billingPeriod/espi:start")

        if duration_node.is_empty() or start_ts_node.is_empty():
            # This usage summary is for part of the month, it isn't
            # relevant to our analysis.
            continue

        duration = int(duration_node.text)
        start_ts = int(start_ts_node.text)

        start = datetime.fromtimestamp(start_ts, tz.tzutc())
        end = start + timedelta(seconds=duration)

        # adjust for daylight savings
        if end.hour == 23:
            end = end + timedelta(hours=1)
        if end.hour == 1:
            end = end - timedelta(hours=1)

        # standardize to our concept of a bill cycle
        start = start + timedelta(days=1)

        start_day = start.strftime("%Y-%m-%d %H:%M:%S")
        end_day = end.strftime("%Y-%m-%d %H:%M:%S")

        try:
            cost = (
                float(summary.find("espi:billLastPeriod").get("text", None)) / 10 ** 5
            )
        except TypeError:
            cost = None

        consumption_value = int(
            summary.find("espi:overallConsumptionLastPeriod/espi:value").get("text", 0)
        )
        consumption_power = int(
            summary.find(
                "espi:overallConsumptionLastPeriod/espi:powerOfTenMultiplier"
            ).get("text", 0)
        )
        uom = int(
            summary.find("espi:overallConsumptionLastPeriod/espi:uom").get("text", 38)
        )  # defualt to watts

        factor = 1000
        if uom == 169:  # therm
            factor = 1
        use = (consumption_value / factor) * (10 ** consumption_power)

        peak = 0
        costs = summary.findall("espi:costAdditionalDetailLastPeriod")
        for c in costs:
            note = c.find("espi:note").text
            if "Demand" in note:
                pot = int(c.find("espi:measurement/espi:powerOfTenMultiplier").text)
                unit = int(c.find("espi:measurement/espi:uom").text)
                value = int(c.find("espi:measurement/espi:value").text)
                peak = max(peak, _convert_reading(pot, unit, value))

        tariff = summary.find("espi:tariffProfile").get("text", "")

        bill = {
            "start": start_day,
            "end": end_day,
            "used": use,
            "cost": cost,
            "peak": peak,
            "tariff": tariff,
        }

        bills.append(bill)

    return unify_bills(bills)


def _convert_reading(power_of_ten, uom, reading, interval=15):
    # There was a "bug" in the past where the peak was getting divided by 4
    # (which was making up for a bug where PG&E was returning usage marked
    # as demand but then turned into a bug when PG&E fixed theirs), so this
    # functions needs to be the single source of truth for conversions.
    #
    # This function therefore needs to handle all cases explicitly, so raise errors
    # on any uom that isn't whitelisted and then add when necessary, so we can avoid
    # making assumptions elsewhere like the one removed here:
    #   https://github.com/Gridium/tasks/commit/d278d255215c098c9ffd6f5c5ada048b94797604?diff=unified#diff-878ab52af69fae92b75084ec235917c9L592

    # Sometimes SCE green button files will set uom to 0, which
    # appears to be an error / meaningless unit. When this happens and
    # the reading is 0 as well, we can still interpret the result
    # (zero times change of units is still 0).
    if reading == 0:
        return 0.0

    if not Units.has(uom):
        raise UnsupportedUnitException('Unit code "{}" is unsupported'.format(uom))

    # Convert anything Watt-related to kiloWatts
    if uom in [Units.WATTS, Units.WATT_HOURS]:
        reading = reading / 1000

    if uom == Units.WATT_HOURS:
        reading = _usage_to_demand(reading, interval)

    return reading * (10 ** power_of_ten)


def _usage_to_demand(reading, interval):
    # Convert electric use to demand. For a 15-min interval, a use of "5Wh"
    # is really "5Wh/15min" so multiply by 4 to get demand for interval, eg...
    #
    #    5 Wh         60 min             60 min
    #  ---------  x  --------  =  5 W * --------  = 5 W * 4
    #   15 min         1 h               15 min
    #
    # ... but the actual coefficient depends on interval
    return reading * (60 / interval)
