#!/usr/bin/env python3

import sys, time, base58, random
import pathlib
import json

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from transaction import sign_staking_tx

TIMEOUT = 240

NUM_VALIDATORS = 6
NUM_OBSERVERS = 2

MASTER_PATH='/home/marcelo/nearcore/kickout-test/before'
CHANGE_PATH='/home/marcelo/nearcore/kickout-test/after'

config_changes = {}
for i in range(NUM_VALIDATORS + NUM_OBSERVERS):
    config_changes[i] = {"tracked_shards": [0, 1, 2, 3]}
config = load_config()
config['near_root'] = MASTER_PATH

near_root, node_dirs = init_cluster(
    num_nodes=6,
    num_observers=2,
    num_shards=4,
    config=config,
    genesis_config_changes=[],
    client_config_changes=config_changes)

validator_nodes = []
for i in range(NUM_VALIDATORS):
    if i % 2 == 0:
        config['near_root'] = MASTER_PATH
    else:
        config['near_root'] = CHANGE_PATH
    if i == 0:
        boot_node = None
    else:
        boot_node = validator_nodes[0]
    validator_nodes.append(spin_up_node(config, config['near_root'], node_dirs[i], i, boot_node=boot_node))

observer_nodes = []
for i in range(NUM_VALIDATORS, NUM_VALIDATORS+NUM_OBSERVERS):
    if i % 2 == 0:
        config['near_root'] = MASTER_PATH
    else:
        config['near_root'] = CHANGE_PATH
    observer_nodes.append(spin_up_node(config, config['near_root'], node_dirs[i], i, boot_node=validator_nodes[0]))


validator_nodes[0].kill()

time.sleep(5)

block_hash = observer_nodes[0].get_latest_block().hash_bytes
tx = sign_staking_tx(validator_nodes[1].signer_key, validator_nodes[1].validator_key,
                     0, 1, block_hash)
validator_nodes[1].send_tx(tx)

epoch_ids = set()

while True:
    b = observer_nodes[0].get_final_block()['result']
    logger.info(f'height {b["header"]["height"]}')
    epoch_ids.add(b['header']['epoch_id'])
    if len(epoch_ids) >= 3:
        break
    time.sleep(5)

for epoch_id in epoch_ids:
    print(epoch_id)
    v0 = observer_nodes[0].get_validators(epoch_id=epoch_id)['result']
    v1 = observer_nodes[1].get_validators(epoch_id=epoch_id)['result']

    keys0 = list(v0.keys())
    keys1 = list(v1.keys())

    assert keys0 == keys1, (keys0, keys1)

    for k in keys0:
        if k == 'current_validators':
            cv0 = json.dumps(v0[k], indent=2)
            cv1 = json.dumps(v1[k], indent=2)
            print(f'current_validators:\nbefore:\n{cv0}\nafter:{cv1}')
        else:
            assert v0[k] == v1[k], (v0[k], v1[k])