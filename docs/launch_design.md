# Batch Launch Script Design Document

Launch Script Responsibilities

- Creates a working directory for scraper artifacts (like screenshots and downloads).
- Writes instrumentation to elasticsearch.
- Triggers a scraper.
- Logs final scraper run state.
- Impose quality checks on data (e.g. Interval data readings shall not be 1000x the average for the past 3 weeks.).
- Pushes a run archive to S3.
- Push data to platform.

Issues with current design:

- Requires writing many similar-looking functions.
- No tests / hard to test each step in isolation.
- Not clear what is guaranteed during the run.
- Can't enforce a policy like: Don't retry a scraper if the login fails.
- Duplicate code for testing a scraper. 

Old System:
- Every data source has a task.
- Meter and account are passed in as mongo records.
- Task may or may not utilize Kevin's scraper objects.
- Sometimes data is read/written from a file to interface with a JS scraper.


