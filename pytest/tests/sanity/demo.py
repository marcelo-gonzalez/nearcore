#!/usr/bin/env python3

import base58
import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
import utils
import transaction

from configured_logger import logger


def main():
    config = cluster.load_config()

    nodes = cluster.start_cluster(2, 0, 4, config, [["epoch_length", 100]],
                                  {0: {
                                      "tracked_shards": [0, 1, 2, 3],
                                  }})

    amount = 1
    nonce = 1
    for height, block_hash in utils.poll_blocks(nodes[0], timeout=10**9):
        block_hash = base58.b58decode(block_hash.encode('utf8'))

        tx = transaction.sign_payment_tx(nodes[0].signer_key, 'test1', amount,
                                         nonce, block_hash)
        res = nodes[0].send_tx(tx)
        print(f'sent tx: {res}')
        nonce += 1
        amount += 1


if __name__ == '__main__':
    main()
