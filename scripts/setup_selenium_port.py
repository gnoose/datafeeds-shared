import argparse
from glob import glob
import re
import shutil


def main():
    parser = argparse.ArgumentParser(
        description="set up files for Selenium port to datafeeds"
    )
    parser.add_argument("utility_id", type=str, help="utility code, ie heco")
    parser.add_argument(
        "utility_camel", type=str, help="camel cased utility name, ie HECO"
    )
    args = parser.parse_args()

    # scraper
    with open("templates/selenium_scraper.py") as inf:
        data = inf.read()
    filename = "../datafeeds/scrapers/%s.py" % args.utility_id
    with open(filename, "w") as outf:
        data = re.sub("_UtilityName_", args.utility_camel, data)
        data = re.sub("_UtilityId_", args.utility_id, data)
        outf.write(data)
        print(
            "copy content for %s from tasks/gridium_tasks/lib/scrapers"
            % (args.utility_id)
        )

    # directions
    with open("templates/selenium_directions.md") as inf:
        data = inf.read()
    filename = "../%s_directions.md" % args.utility_id
    with open(filename, "w") as outf:
        data = re.sub("_UtilityName_", args.utility_camel, data)
        data = re.sub("_UtilityId_", args.utility_id, data)
        outf.write(data)
        print(
            "copy ../%s_directions.md to pull request"
            % (args.utility_id)
        )



if __name__ == "__main__":
    main()
