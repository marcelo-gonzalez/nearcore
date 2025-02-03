import sys, time, base58, random
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
from transaction import sign_payment_tx
import utils

nodes = start_cluster(
    num_nodes=1,
    num_observers=0,
    num_shards=4,
    config=None,
    extra_state_dumper=True,
    genesis_config_changes=[["epoch_length", 20]],
    client_config_changes={}
)

for height, hash in utils.poll_blocks(nodes[0], timeout=9999999999):
    print(height)
