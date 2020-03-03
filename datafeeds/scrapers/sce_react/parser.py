import csv
from datetime import datetime
from typing import NewType, Tuple, List, Optional

from dateutil.parser import parse as parse_date

from gridium_tasks.lib.scrapers.sce.react.errors import IntervalDataParseException

# Note:the reading can be either a usage or a demand value, depending on context
IntervalReading = NewType("IntervalReading", Tuple[datetime, Optional[float]])


def _to_float(text):
    text = text.strip().replace(",", "")
    if text:
        return float(text)
    return None


def parse_sce_csv_file(path: str, service_id: str) -> List[IntervalReading]:
    """Extract interval data readings from a CSV file downloaded from the SCE website

    This file is little unusual; there is an extended header portion, several line containing various
    metadata about the interval data that follows. We skip over that header and extract the interval
    readings. They are returned as a list of tuples of the form (datetime.datetime, float). We expect
    these readings to be 15 minute readings. The readings can either be demand or usage values, depending
    on manner in which the CSV file is downloaded. This function makes no assumption about the units
    on these readings.

    A given file can contain reading for multiple service ids. This is implemented by having one column per
    service ID. This function takes an argument specifying which service id column to fetch data for. If the
    service ID cannot be found, an IntervalDataParseException is thrown. Similarly, any errors that occur
    while parsing will be raised as IntervalDataParseException instances.

    Arguments:
        path: The path to the CSV file on the file system
        service_id: The service id of interest

    Returns:
        A list of interval data readings, formatted as 2-tuples, of the for (datetime.datetime, float). The first
        tuple element stores the time when the reading occurred, the second the interval data reading, as a float.

    Raises:
        IntervalDataParseException: If the desired service ID can't be found, or an error occurs while parsing.
    """

    # Read lines until we find the interval data header line (starts with the string "Date",
    # then store the raw data into data_lines
    data_started = False
    data_lines = []
    with open(path) as f:
        for line in f:
            stripped_line = line.strip()
            if not data_started:
                if stripped_line.startswith("Date"):
                    data_started = True
                    data_lines.append(stripped_line)
            else:
                data_lines.append(stripped_line)

    # Parse each reading row
    csv_reader = csv.reader(data_lines)
    first = True
    data_column = None
    results = []
    for row in csv_reader:
        if first:
            first = False
            headers = [th.strip() for th in row]
            for idx, th in enumerate(headers):
                if th == service_id:
                    data_column = idx
            if not data_column:
                raise IntervalDataParseException("Could not find data for SAID={}".format(service_id))
        else:
            try:
                reading_date = parse_date(row[0].strip()).date()
                reading_time = parse_date(row[1].strip()).time()
                reading_datetime = datetime.combine(reading_date, reading_time)
                reading_value = _to_float(row[data_column].strip())
                results.append(IntervalReading((reading_datetime, reading_value)))
            except Exception as e:
                msg = "An error occured while trying to parse interval data from the SCE website."
                raise IntervalDataParseException(msg) from e
    return results
