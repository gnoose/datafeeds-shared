# SCE Scrapers

This directory contains scraper implementations for the utility Southern California Edison (SCE).

## React Scrapers
The "react" scrapers—so-called because they interact with the [ReactJs](https://reactjs.org/)
UI on SCE's website—form the bulk of the current SCE scrapers. They were written
in response to SCE's shift to the React-based UI in early July, 2019. This directory contains the implementation of these scrapers.

#### Energy Manager Interval Scraper
See [this document](https://docs.google.com/document/d/19rMdfb_hazGkLL4h7-kfn0lBG_qutaEHRVceDZ2kLec/edit?usp=sharing)
for a basic overview of the Energy Manager interval scraper. The main implementation
of this scraper is [energymanager_interval.py](energymanager_interval.py).

#### Energy Manager Billing Scraper
See [this document](https://docs.google.com/document/d/1NTYRP_wL6HX93sdtOHN8SL0zKegY5sjlOGEqUu97loA/edit?usp=sharing)
for a basic overview of the Energy Manager billing scraper. The main implementation
of this scraper is [energymanager_billing.py](energymanager_billing.py).

#### Basic Billing Scraper
See [this document](https://docs.google.com/document/d/1dWzLfOvykmRw7Xf5LyWesk67e1p5XIFN3_OUCLbaxkU/edit?usp=sharing)
for a basic overview of the Energy Manager billing scraper. The main implementation
of this scraper is [basic_billing.py](basic_billing.py).

