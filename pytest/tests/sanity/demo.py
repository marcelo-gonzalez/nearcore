#!/usr/bin/env python3

import sys
import time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
import utils
from configured_logger import logger


def main():
    config = cluster.load_config()

    nodes = cluster.start_cluster(1, 0, 4, config, [["epoch_length", 100]],
                                  {0: {
                                      "tracked_shards": [0, 1, 2, 3],
                                  }})

    for height, hash_ in utils.poll_blocks(nodes[0], timeout=10**9):
        print(height)


if __name__ == '__main__':
    main()
