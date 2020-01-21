# Grovestreams API

## Introduction

Grovestreams API is a separate REST API served by STEM. It should be considered the "next iteration" of the STEM API, which may be eventually be deprecated.

Like STEM API, this API sends and receives JSON. In theory there is support for API Keys, so that we could theoretically have one Gridium API key for all streams that we need to access. 
In practice, this has not worked. This appears to happen because the customer with a grovestreams login does not have the permissions 
necessary to grant us access with an API key.

In the meantime, we can use the customer's username/login combination (as we do in older scrapers) to get access to the one endpoint we need to gather interval data.  

## Relevant API Endpoints

Full Grovestreams API documentation can be found [here](https://www.grovestreams.com/developers/api.html).

A copy of the PDF instructions STEM sent us can be found in docs.

The scraper depends on only two endpoints:

- `/api/login` (POST): This is necessary to obtain login cookies for later endpoints.

- `/api/feed` (GET): This endpoint lets us recover interval data between two points in time.

## Other Thoughts

Provisioning meters with this scraper seems to require emailing STEM at the moment, which is a little slow/manual.
Looking at their API, it seems there may be sufficient endpoints to make this process self-serve.