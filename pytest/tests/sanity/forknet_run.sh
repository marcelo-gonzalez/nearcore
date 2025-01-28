if [ $# -ne 2 -a $# -ne 3 ]; then
    echo 'bad args'
    exit 1
fi

neard=$1
near_dir=$2

if [ $# -eq 3 ]; then
    test_dir=$3
else
    test_dir=$(mktemp -d)
fi
echo "copying $near_dir to $test_dir"
rsync -a "$near_dir/" "$test_dir/"

echo "running fork-network init"
$neard --home $test_dir fork-network init 2> "$test_dir/init-stderr"

echo "running amend-access-keys with stderr in $test_dir/fork-network-stderr"
$neard --home $test_dir fork-network amend-access-keys 2> "$test_dir/fork-network-stderr"

jq '[{account_id, public_key, amount: "10000000000000"}]' "$test_dir/validator_key.json" > "$test_dir/validators.json"

echo "running fork-network set-validators and finalize"
$neard --home $test_dir fork-network set-validators --validators "$test_dir/validators.json" --chain-id-suffix "bug" '--epoch-length' 1000 2> "$test_dir/set-validators-stderr"
$neard --home $test_dir fork-network finalize 2> "$test_dir/finalize-stderr"

echo "running neard on forked DB"

tmp=$(mktemp /tmp/tmpXXX.json)
jq '.consensus.min_num_peers=0' "$test_dir/config.json" > "$tmp"
mv "$tmp" "$test_dir/config.json"

$neard --home $test_dir run


