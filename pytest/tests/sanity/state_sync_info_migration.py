#!/usr/bin/env python3

import sys, time, base58, random
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, load_config
from configured_logger import logger
from utils import MetricsTracker
import utils

TIMEOUT = 240

# dir with file called "neard" in it
OLD_BINARY_ROOT = '/tmp/neard-old/'
NEW_BINARY_ROOT = '/Users/marcelo/nearcore/target/debug/'

config = load_config()
config['near_root'] = OLD_BINARY_ROOT

nodes = start_cluster(
    num_nodes=4,
    num_observers=1,
    num_shards=4,
    config=None,
    extra_state_dumper=True,
    genesis_config_changes=[["shuffle_shard_assignment_for_chunk_producers", True], ["protocol_version", 71],
                            ["epoch_length", 20],
                            ["block_producer_kickout_threshold", 70]],
    client_config_changes={
        0: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        1: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        2: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        3: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            }
        },
        4: {
            "consensus": {
                "state_sync_timeout": {
                    "secs": 2,
                    "nanos": 0
                }
            },
            "tracked_shards": [0, 1, 2, 3]
        }
    })

metrics = MetricsTracker(nodes[0])
restarted = False
for height, hash in utils.poll_blocks(nodes[4], timeout=TIMEOUT):
    if height >= 100:
        break

    if not restarted:
        status = metrics.get_metric_all_values("near_state_sync_stage")
        if status != []:
            logger.info(f'restarting node')
            nodes[0].kill()
            nodes[0].near_root = NEW_BINARY_ROOT
            nodes[0].start()
            restarted = True
