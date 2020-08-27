# Adding Partial Billing Scrapers

Partial Billing Scrapers are a billing scraper variant where we assume we're only scraping a subset of the customer's
overall charges. If a customer has a third-party handling the generation of their power, we might connect two `partial-billing` scrapers to
the meter, one for scraping `tnd-only` charges, and the other for scraping `generation-only` charges.

The primary difference between a `billing` scraper run, and a `partial-billing` scraper run is that
partial-bills will be written directly into the `partial-bill` table, instead of going through platform and then into 
the `bill` table.  A daily webapps process will attempt to stitch `tnd-only` partial bills with their corresponding 
`generation-only` partial bill components and created totalized `bills`.


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

Partial billing scrapers in the db should have scraper.source_types set to [`partial-billing`], so this can 
be a way to automatically determine if the scraper results should use the billing workflow or the partial billing workflow.

```python
configuration = SceReactEnergyManagerBillingConfiguration(
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
corresponding generation SAID should be stored in `UtilityService.gen_service_id`.  If you are building a T&D-only
partial billing scraper, you should generally pass in the `service_id` or `utility_account_id` to the Configuration. 
If you are building a generation-only partial-billing scraper, you should use the generation service fields, and pass 
in the `gen_service_id` or `gen_utility_account_id` to the Configuration. The scraper configuration should pass in 
`partial_type` which will either be `tnd-only` or `generation-only`. This will be set on `PartialBill.provider_type` for 
use in the stitching process later.

### Urjanet 

Urjanet Partial Billing Scrapers should pass in `urja_partial_billing=True` to `run_urjanet_datafeed`.  Urjanet scrapers
define the scraper configuration in a central place, so this flag determines if the `BasePartialBillUrjanetConfiguration` is
used or the `BaseUrjanetConfiguration` is used.  The `BasePartialBillUrjanetConfiguration` has `scrape_partial_bills` 
set to True, which will send the urjanet scraper results into the `partial_bill` table.

SCE CCA Scraper - Clean Power Alliance (generation-only)
```python
def datafeed(
    account: SnapmeterAccount,
    meter: Meter,
    datasource: MeterDataSource,
    params: dict,
    task_id: Optional[str] = None,
) -> Status:
    return run_urjanet_datafeed(
        account=account,
        meter=meter,
        datasource=datasource,
        params=params,
        urja_datasource=SCECleanPowerAllianceDatasource(
            utility=meter.utility_service.utility,
            account_number=meter.utility_account_id,
            gen_utility=meter.utility_service.utility,
            gen_account_number=meter.utility_service.gen_utility_account_id,
            gen_said=meter.utility_service.gen_service_id,
        ),
        transformer=UrjanetGridiumTransformer(),
        task_id=task_id,
        urja_partial_billing=True,
    )
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


## Running partial billing scrapers locally

Running a generation-only partial-billing scraper.  Pass in the `gen_service_id` optional config.
```bash
python launch.py by-name 'sce-clean-power-alliance-urjanet' '2-41-422-5144' '3-049-1578-16' '2019-01-01' '2020-05-01' --gen_service_id '3-050-6585-98'

```

Running a tnd-only partial billing scraper.  Passing in `--source_type='partial-billing` config, because 
the Configuration for this scraper looks at `source_types` to determine if this is a `billing` scraper or a 
`partial-billing` scraper.
```bash
python launch.py by-name 'sce-react-energymanager-partial-billing' '2-41-422-5144' '3-049-1578-16' '2020-01-01' '2020-03-01' --source_type='partial-billing' --username **** --password ****

```
