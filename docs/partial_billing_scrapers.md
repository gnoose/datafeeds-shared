# Adding Partial Billing Scrapers

Partial Billing Scrapers are a billing scraper variant where we assume we're only scraping a subset of the customer's
overall charges. If a customer has a third-party handling the generation of their power, we might connect two `partial-billing` scrapers to
the meter, one for scraping `tnd-only` charges, and the other for scraping `generation-only` charges.

The primary difference between a `billing` scraper run, and a `partial-billing` scraper run is that `partial-billing`
scrapers create *partial_bills* and `billing_scrapers` create *bills*. Partial-bills are written directly to the 
`partial-bill` table, instead of going through platform.  A daily webapps process matches `tnd-only` 
partial bills with their corresponding `generation-only` partial bill components and creates totalized `bills`.


## Scraper Configuration

### Creating a partial billing scraper

This should be designated at the scraper level.   Add a record to the `scraper` table, and set
`source_types` to `{partial-billing}`.

| Column        | Type                  |
| --------------| ----------------------|
| name          | character varying     |
| label         | character varying     |
| active        | boolean               |
| source_types  | character varying[]   |
| meta          | jsonb                 |


### scrape_partial_bills

Importantly, the scraper Configuration needs to have `scrape_partial_bills` set to True.  If `scrape_partial_bills`
is True, the scraper results will be sent through the partial bills workflow.

```python
class SampleGenerationPartialBillingConfiguration(Configuration):
    def __init__(self, utility: str, account_number: str, gen_service_id: str):
        super().__init__(scrape_partial_bills=True)
        self.account_number = account_number
        self.utility = utility
        self.gen_service_id = gen_service_id
```

Sometimes, billing scraper code will be able to perform double-duty and also be used for a partial-billing scraper.
For example, the `sce-react-energymanager-billing` scraper was originally written to be a typical billing scraper for SCE,
but we also use this underlying code in a partial-billing scraper `sce-react-energymanager-partial-billing`,
for scraping `tnd-only` charges, for SCE customers that are using a CCA for generation.

Partial billing scrapers in the db should have scraper.source_types set to [`partial-billing`], so this might be 
helpful in determining if scraper results should go through the billing workflow or partial billing workflow.

```python
configuration = SampleBillingConfiguration(
        utility=meter.utility_service.utility,
        utility_account_id=meter.utility_account_id,
        service_id=meter.service_id,
        scrape_bills="billing" in datasource.source_types,
        scrape_partial_bills="partial-billing" in datasource.source_types,
    )
```

### T&D-only bills or Generation-only partial bills.

It is very important that your scraper produces the correct "type" of partial bill.  In the stitching process, `tnd-only`
partials will be matched to `generation-only` partial bills.

For a meter where there are multiple SA's involved, `UtilityService.service_id` should be used to store the T&D SAID. The
corresponding generation SAID should be stored in `UtilityService.gen_service_id` (if necessary).  If you are building a T&D-only
partial billing scraper, you should generally pass in the `service_id` or `utility_account_id` to the Configuration.
If you are building a generation-only partial-billing scraper, you might need to pass
in the `gen_service_id` or `gen_utility_account_id` to the Configuration. For some utilities, these service ids are 
the same.  You might also have your scraper search the `UtilityServiceSnapshot` table to get historical service ids
and historical generation service ids.

After collecting results, the scraper should return bills using the appropriate attribute in the `Results` class:

  - full bills go in `bills`
  - T&D partials go in `tnd_bills`
  - generation partials go in `generation_bills`

For example, to create T&D partial bills: `return Results(tnd_bills=self.billing_history)`

### Urjanet

Urjanet Partial Billing Scrapers should pass in the type of partial bill that they scrape via the `partial_type` parameter
to `run_urjanet_datafeed`.

Example: PG&E Urjanet Generation Scraper (generation-only)
```python
def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    utility_service = meter.utility_service
    return run_urjanet_datafeed(
        account,
        meter,
        datasource,
        params,
        PacificGasElectricXMLDatasource(
            utility_service.utility,
            utility_service.utility_account_id,
            utility_service.service_id,
            utility_service.gen_utility,
            utility_service.gen_utility_account_id,
            utility_service,
        ),
        PacificGasElectricUrjaXMLTransformer(),
        task_id=task_id,
        partial_type=PartialBillProviderType.GENERATION_ONLY,
    )
```

The `BaseUrjanetScraper` uses this to determine how to return results:

```
    if self._configuration.partial_type == PartialBillProviderType.GENERATION_ONLY:
        return Results(generation_bills=billing_data_final)
    if self._configuration.partial_type == PartialBillProviderType.TND_ONLY:
        return Results(tnd_bills=billing_data_final)
    return Results(bills=billing_data_final)
```

## Scraping Utility Codes
We want to start extracting service configuration and storing on partial bills. When adding a partial billing scraper, 
attempt to scrape the `utility_code` (the utility's version of the tariff), and pass into BillingDatum.  This will 
be persisted to the PartialBill record in the `PartialBillProcessor`.

```python
if key not in raw_billing_data:
    rate = None
    if service_row:
        rate = service_row.rate

    bill_data = BillingDatum(
        start=current_bill_row.bill_start_date,
        end=current_bill_row.bill_end_date - timedelta(days=1),
        statement=current_bill_row.statement_date,
        cost=current_bill_row.bill_amount,
        used=current_bill_row.kwh,
        peak=current_bill_row.kw,
        items=None,
        attachments=None,
        utility_code=rate,
    )
```

## Scraping "third_party_expected"
When building a T&D Partial Billing Scraper, if you can tell that the service is on a third-party 
for the given billing period, set `third_party_expected` to True.  Alternatively, if you can verify
that the service is bundled for that billing period, set `third_party_expected` to False.  This
will help the `PartialBillStitcher` in webapps determine if we're missing generation charges, 
or if no generation charges were expected that month.  We dictate this to the scraper level 
to keep the Stitcher agnostic.

```python
BillingDatum(
    start=date(2017, 1, 2),
    end=date(2017, 1, 30),
    cost=3411.2,
    used=5,
    peak=None,
    items=[],
    statement=date(2017, 1, 30),
    attachments=None,
    utility_code=None,
    utility_account_id=None,
    utility="utility:pge",
    service_id="1234566",
    third_party_expected=True,
)

```


## Running partial billing scrapers locally

Running a generation-only partial-billing scraper.  Pass in the `gen_service_id` optional config if needed.
```bash
python launch.py by-name 'sce-clean-power-alliance-urjanet' '2-41-422-5144' '3-049-1578-16' '2019-01-01' '2020-05-01' --gen_service_id '3-050-6585-98'

```

Running a generation-only partial-billing scraper where the service id is the same for the T&D/Gen provider:
```bash
python launch.py by-name pge-urjanet-generation 9166678644-6 9166678022 2018-1-22 2019-1-22
```

Running a tnd-only partial billing scraper:
```bash
python launch.py by-name smd-tnd-partial-billing 9166678644-6 9166678022 2015-11-20 2021-1-15
```

## Adding New Fields to the Partial Bill Model

If you add a new field to the PartialBill model, consider if changes to this field should cause existing partial bills 
to be overridden.  Add this field to `PartialBill.differs` if necessary.



