# Daylight savings time

Utilities are inconsistent in how they deal with daylight savings time. This doc describes DST-related
issues we've seen and how we fixed them, so that we can identify and apply common patterns.

## Fall (25 hour day)

The week after the DST change, ops reviewed meters looking for unusual spikes. Some utilities returned
100 data points. If we receive two readings for the same interval, we should
average them. Other scrapers seemed to return doubled readings for only some meters; we fixed the data
manually.

We added a [daylight_savings.py](https://github.com/Gridium/datafeeds/blob/master/datafeeds/common/daylight_savings.py)
helper to get the DST start and end dates with rrules.

### extra readings

In 2019, [LADWP MVweb](https://gridium.atlassian.net/browse/GRID-3274)
returned 96 intervals for fall DST; in 2020, they returned 100 intervals, with duplicate
timestamps for 1:00-1:45. Updated the scraper to save readings by timestamp using the
[Timeline class](https://github.com/Gridium/datafeeds/blob/master/datafeeds/common/timeline.py).
If we get multiple readings for same interval, average them.

[Smart Meter Texas](https://gridium.atlassian.net/browse/GRID-3273) returns 100 readings for fall DST.
If the date is a DST day (`if day in daylight_savings.DST_ENDS`), average the readings.

[SCL](https://gridium.atlassian.net/browse/GRID-3275) returns 100 readings for fall DST.
Average the readings for the fall DST time range.

### only some meters doubled

Some [pge-energyexpert](https://gridium.atlassian.net/browse/GRID-3272)
meters had doubled data for 2:45 interval, and missing data at 1:45; others did not.
Manually fixed data for affected meters, since it did not seem to affect all meters.

A few [PSE](https://gridium.atlassian.net/browse/GRID-3276) meters showed spikes during the DST hour,
but others didn't. Manually fixed data for affected meters, since it did not seem to affect all meters.


## Spring (23 hour day)
