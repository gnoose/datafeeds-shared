# Urjanet Library

This directory contains a library for building Urjanet scrapers.

## Urjanet Overview

Urjanet data is available in a MySQL database, which we maintain in an
AWS RDS instance. Urjanet regularly sends us updates for this database,
which we apply. This database has four tables of interest:

1) The Account table includes high-level information about an account
on a bill. This is where you will find important payment data such as
the amount due. This table also houses address data, such as billing
and payment addresses.

2) The Meter table organizes usage information into specific “points” of
service. Urjanet’s definition of a meter is not limited to physical
meters. The meter table captures data such as service types, tariffs,
and usage periods.

3) The Usage table stores commodity-specific consumption data. Here,
you will find information such as consumption amounts, read types,
present and previous meter readings, and units of measure.

4) The Charge table includes information related to the line-item charges
on your utility bill. Here, you can find details such as charges names,
rates, and currencies. If a charge is associated with more than one meter,
Urjanet will prorate the charge based on the usage amount.

These descriptions are taken directly from the
[Urjanet help site](https://urjanet.zendesk.com/hc/en-us/articles/360011538891-3-The-Urjanet-Data-Model),
which you may need a login for (ask the operations team for help with that).

## Library Overview

This framework has two core concepts: 1) **datasources** and 2) **transformers**.

Datasources query data from an Urjanet database, filter in some fashion,
and load them into an intermediate model. This model is defined in
[the model folder](../datafeeds/urjanet/model), and consists of a Python class for each
major Urjanet entity (Account, Meter, Usage, Charge), and a few
additional entities. Model objects typically are specified using
the `jsonobject` python module. This module was chosen because it makes it very easy to
specify model objects, and moreover allows one to easily serialize models to json.
format. The datasources themselves can be found in the
[datasource folder](../datafeeds/urjanet/datasource).

Transformers take in model objects and apply some transformation to them.
Most often, this transformation will synthesize billing periods from
the Urjanet objects, for a given service. Transformers can be found in
the [transformer folder](../datafeeds/urjanet/transformer).

The [scripts folder](../datafeeds/urjanet/scripts) contains some high-level scripts for loading
data and performing transformations.

## Model

The main model classes are defined in [core.py](../datafeeds/urjanet/model/core.py).

### Urjanet Model Classes
- `UrjanetData`: Top-most model object. Represents some projection of data from the
Urjanet database into the local model. Contains a list of `Account` instances.
- `Account`: Represents an Urjanet Account entity, as described above. Has
various useful Account attributes like statement date, total bill cost,
utility provider name, and so on. Also contains a list of `Meter` instances
associated with the Account, as well as a list of `Charge` instances. These
charges are referred to as "floating charges", since they are are not
associated with a specific meter. Sometimes this represents an issue with the
source Urjanet data (e.g. their process failed to attach a charge to the
right meter).
- `Meter`: Represents an Urjanet Meter entity, as described above. Has
useful Meter attributes like service ID, meter number, tariff, and so on.
Also contains a list of `Charge` and `Usage` instances associated with
the Meter.
- `Charge`: Represents an Urjanet Charge entity, as described above. In brief,
a Charge is an individual bill line item. Contains data like line item name, cost,
date ranges, and so on. Usually associated with a meter, but not also (see
discussion of floating charges above, under `Account`)
- `Usage`: Represents an Urjanet Usage entity, as described above. In brief,
a Usage entity represents an individual usage measurement on a utility bill.
Always associated with a `Meter` object.

### Other Model Classes
- `GridiumBillingPeriodCollection`: A collection of `GridiumBillingPeriod`
instances. This class is generally the output of scraper transformer classes.

- `GridiumBillingPeriod`: Represents a basic billing period as understood by
Gridium's backend.

## Datasources

A datasource loads data from an Urjanet datasource into the internal data model,
filtered depending on the particular scraping task at hand. Generally speaking,
a given datasource should load "just enough" data to support a given scraping
task, though further filtering can be done in the transformation component
if necessary. To support this, datasources will often take parameters such
as account numbers or service IDs that are used to filter the queried data.

### MySQL Datasources

MySQL datasources can extend from the `UrjanetPyMySqlDataSource` base class.
This class has two abstract functions that must be overloaded by inheritors:
- `load_accounts`: Loads `Account` entities from the database, usually filtered by
account number and/or utility name (may vary from utility to utility).
- `load_meters`: Loads `Meter` entities from the database for a given `Account`,
usually filtered by some kind of service ID or service type.

There are several existing examples of datasources in the
[datasource directory](../datafeeds/urjanet/datasource).

## Transformer

A transformer transforms Urjanet data loaded by a datasource into Gridium billing
periods. Transformers should inherit from the base class `UrjanetGridiumTransformer`.
The `urja_to_gridium` function accepts an `UrjanetData`
object, and returns a `GridiumBillingPeriodCollection`. The implementation of this
function will likely vary slightly for different utilities. Transformers may also need to
override `GenericBillingPeriod` to adjust for differences in data layout. To use a custom
billing period implementation, override `def billing_period(account: Account)` in the
transformer to return your custom billing period type. There are several existing
examples of transformers in the [transformer directory](../datafeeds/urjanet/transformer).

## Scripts
The [scripts directory](../datafeeds/urjanet/scripts) defines a simple, common command line interface for
Urjanet scrapers. There are two basic commands supported.

### [dump_urja_json.py](../datafeeds/urjanet/scripts/dump_urja_json.py)
This command reads Urjanet data from a MySQL database, and dumps it into a local
json file. It is effectively a wrapper around a datasource, as described above.

Important: This command requires a database connection. In order to run it locally, you
must have access to an Urjanet database. See [urjanet_setup.md](urjanet_setup.md) to set up
a local database.

### [transform_urja_json.py](../datafeeds/urjanet/scripts/transform_urja_json.py)
This command transforms Urjanet data in a json format (e.g. produced by `dump_urja_json.py`)
into Gridium billing periods. The billing periods are written out in a separate json file.
Run the script with the path to the input json (generated by `dump_urja_json.py`) and the
transformer (ie utility) to use:

    python transform_urja_json.py keller_isd_input.json southlake

### Supporting a new utilty
A new Utility can be added to the CLI by adding an entry to the
[cli_hooks.py](../datafeeds/urjanet/scripts/cli_hooks.py)
file, e.g.:

```
class MyNewUtility(DatasourceCli):
    __cli_key__ = "unique_key"

    def add_datasource_args(self, parser):
        # Add custom arguments for this datasource
        parser.add_argument("account_number")

    def make_datasource(self, conn, args):
        # Make the datasource object associated with this utility
        return urja_datasource.MyUtilityDatasource(conn, args.account_number)

    def make_transformer(self):
        # Make the transformer object associated with this utility
        return urja_transformer.MyUtilityTransfomer()

```

## Testing

Tests can be found in [datafeeds/urjanet/tests](../datafeeds/urjanet/tests/).
There are several examples for existing utilities.
One approach is to generate some fixtures from the production database using `dump_urja_json.py`, then
writing tests to ensure that your transformer works correctly.
