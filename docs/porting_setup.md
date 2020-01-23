# Urjanet

In tasks, update the scraper to have a data source and transformer.

Generate test data for several accounts (if needed); see https://github.com/Gridium/tasks/tree/master/gridium_tasks/lib/urjanet#dump_urja_jsonpy

Generate datafeeds files with [setup_urja_port.py utilityId UtilityName](../scripts/setup_urja_port.py). This will create datafeeds files from [templates](../scripts/templates):

  - datafeeds/urjanet/datasource/*utilityId*.py
  - datafeeds/urjanet/transformer/*utlityId*.py
  - copy test fixtures from tasks/gridium_tasks/lib/tests/urjanet/data/*utilityId*/*.json
  - create datafeeds/urjanet/tests/test_urjanet_*utilityId*_transformer.py to run transform (*_input.json -> *_expected.json)

Copy content for datafeeds/urjanet/datasource/*utilityId*.py from tasks/gridium_tasks/lib/urjanet/datasource/*utilityId*.py

Copy contents for datafeeds/urjanet/transformer/*utilityId*.py frm tasks/gridium_tasks/lib/urjanet/transformer/*utilityId*.py

Check in new files to a branch (ok if it doesn't pass; porting is complete when it does pass).

After reviewing PR,
  - add migration for new scraper
  - remove scraper from tasks
