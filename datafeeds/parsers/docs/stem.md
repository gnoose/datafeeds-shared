# STEM API

## Introduction

Stem aggregates some of our customers interval data and makes it
available to us via REST API. This module allows tasks to integrate
with this API. This document provides a brief overview of the design
for this module and some discussion of how the Stem API works.

## Stem API Endpoints

The API has three main datapoints, each of which has its own
datatype. The API documentation doesn't provide very clear
definitions, so some of this information is based on polling endpoints
and making inferences.

The first main datatype is a "client", which appears to be a
business/building owner. Each client has a name and UUID. Unlike a
utility, there doesn't appear to be billing/SAID information
associated with a client.

The second main datatype is a "site". A client may have one or more
sites, which appear to be in one to one correspondence with
meters. (The API doesn't supply us with address or utility SAID
information.) Sites may have names, but from what I've seen, they may
simply match the client name. Sites also have UUIDs, which again may
match the client.

Finally, a site may have one or more "streams" that capture interval
data. We are interested in the "monitor" streams. These capture
average kw values. Other stream types can appear (and may be ignored).

## The `stem` Module

This module wraps the REST API with the following steps:

0. A `Session` object consumes our API key, and maps python function
   calls to HTTP requests using the `requests` library. The `Session`
   object is responsible for handling pagination, so that other
   modules can just think of clients/sites/interval data as a list.
1. For each datatype, we have a parser. The parser's job is to decode
   text responses from the REST API, parse them as JSON, and validate
   that JSON against a schema. Schema validation allows later code to
   make assumptions about how the response JSON is structured. In this
   way, we avoid some "defensive programming" (and will be alerted if
   the REST API changes for some reason outside our control).
2. Parsers decode datatypes to named tuples.
3. A STEM API "scraper" coordinates a Session object (used to obtain
   interval data) and a Timeline object (that formats interval data
   and handles missing intervals). Once the Timeline object is
   finished, it pushes the data to platform.

## Other Remarks

For the purposes of making an automated data ingestion task, we only
need the site and stream endpoints. This is because we can record the
UUID associated with the stream we want to ingest and add that to the
configuration for the task.

However, it is still useful to know about the client endpoint (and
have some automation for it) because this endpoint allows us to look
up *all* of the data sources we have permission to view. If a customer
doesn't have their UUID but has given us access, we can look up the
information we need with this endpoint, based just on the client's
name.
