import argparse
from glob import glob
import re
import shutil


def main():
    parser = argparse.ArgumentParser(
        description="set up files for Urjanet port to datafeeds"
    )
    parser.add_argument("utility_id", type=str, help="utility code, ie fostercity")
    parser.add_argument(
        "utility_camel", type=str, help="camel cased utility name, ie FosterCity"
    )
    args = parser.parse_args()

    # datasource
    with open("templates/urjanet_datasource.py") as inf:
        data = inf.read()
    filename = "../datafeeds/urjanet/datasource/%s.py" % args.utility_id
    with open(filename, "w") as outf:
        outf.write(re.sub("_UtilityName_", args.utility_camel, data))
        print(
            "copy content for %s from tasks/gridium_tasks/lib/urjanet/datasource/%s.py"
            % (filename, args.utility_id)
        )

    # transformer
    with open("templates/urjanet_transformer.py") as inf:
        data = inf.read()
    filename = "../datafeeds/urjanet/transformer/%s.py" % args.utility_id
    with open(filename, "w") as outf:
        outf.write(re.sub("_UtilityName_", args.utility_camel, data))
        print(
            "copy content for %s from tasks/gridium_tasks/lib/urjanet/transformer/%s.py"
            % (filename, args.utility_id)
        )
    for filename in glob(r"../../tasks/gridium_tasks/lib/tests/urjanet/data/%s/*.json"):
        shutil.copy(filename, "../datafeeds/urjanet/tests/data/%s" % args.utility_id)
        print("copied %s" % filename)

    # test
    test_files = set()
    for filename in glob(
        r"../../tasks/gridium_tasks/lib/tests/urjanet/data/%s/*.json" % args.utility_id
    ):
        shutil.copy(filename, "../datafeeds/urjanet/tests/data/%s" % args.utility_id)
        match = re.match(
            r".+?%s/(.*?)_((input)|(expected))\.json" % args.utility_id, filename
        )
        key = match.group(1)
        file_type = match.group(2)
        test_files.add(key)
    with open("templates/test_urjanet_transformer.py") as inf:
        data = inf.read()
    filename = (
        "../datafeeds/urjanet/tests/test_urjanet_%s_transformer.py" % args.utility_id
    )
    with open(filename, "w") as outf:
        data = re.sub("_UtilityName_", args.utility_camel, data)
        data = re.sub("_utilityId_", args.utility_id, data)
        tests = []
        for key in test_files:
            tests.append(
                '        self.%s_test("%s_input.json", "%s_expected.json")'
                % (args.utility_id, key, key)
            )
        data = re.sub(
            "        self.%s_test()" % args.utility_id, "\n".join(tests), data
        )
        outf.write(data)
        print("wrote %s tests to %s" % (len(tests), filename))


if __name__ == "__main__":
    main()
