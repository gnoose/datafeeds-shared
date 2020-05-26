# usage: urja_test_fixture.sh meter utility utility_account said
cd datafeeds/urjanet/scripts
meter=$1
utility=$2
utility_account=$3
said=$4
data="../../urjanet/tests/data/$utility"
python dump_urja_json.py $utility $utility_account $said > $data"/"$meter"_input.json"
echo "** wrote " $data"/"$meter"_input.json"
python transform_urja_json.py $data"/"$meter"_input.json" $utility > $data"/"$meter"_expected.json"
echo "** wrote " $data"/"$meter"_expected.json"
git add "../../urjanet/tests/data/"$utility"/"$meter"_*.json"
echo "** adding ../../urjanet/tests/data/"$utility"/"$meter"_*.json"
echo ""
echo "self."$utility"_electricity_test(\""$meter"_input.json\", \""$meter"_expected.json\")"
cd ../../../
