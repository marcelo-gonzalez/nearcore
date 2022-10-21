#!/usr/bin/env python3

import sys, time, base58, random
import atexit
import base58
import json
import os
import pathlib
import shutil
import signal
import subprocess

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from mocknet import create_genesis_file
import transaction
import utils
import key

# This sets up an environment to test the tools/mirror process. It starts a localnet with a few validators
# and waits for some blocks to be produced. Then we fork the state and start a new chain from that, and
# start the mirror process that should mirror transactions from the source chain to the target chain.
# Halfway through we restart it to make sure that it still works properly when restarted

TIMEOUT = 240
NUM_VALIDATORS = 4
SHARDNET_VALIDATORS = ['foo0', 'foo1', 'foo2']

SOURCE_DIR = 'test0_finished'


def mkdir_clean(dirname):
    try:
        dirname.mkdir()
    except FileExistsError:
        shutil.rmtree(dirname)
        dirname.mkdir()


def dot_near():
    return pathlib.Path.home() / '.near'


def ordinal_to_port(port, ordinal):
    return f'0.0.0.0:{port + 10 + ordinal}'


def init_shardnet_dir(neard, home, ordinal, validator_account=None):
    mkdir_clean(home)

    try:
        subprocess.check_output([neard, '--home', home, 'init'],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"neard init" command failed: output: {e.stdout}')
    shutil.copy(dot_near() / SOURCE_DIR / 'config.json', home / 'config.json')
    shutil.copy(dot_near() / SOURCE_DIR / 'output/forked/genesis.json',
                home / 'genesis.json')
    shutil.copy(dot_near() / SOURCE_DIR / 'output/forked/records.json',
                home / 'records.json')

    with open(home / 'config.json', 'r') as f:
        config = json.load(f)
        config['genesis_records_file'] = 'records.json'
        config['network']['addr'] = ordinal_to_port(24567, ordinal)
        config['rpc']['addr'] = ordinal_to_port(3030, ordinal)
    with open(home / 'config.json', 'w') as f:
        json.dump(config, f)

    if validator_account is None:
        os.remove(home / 'validator_key.json')
    else:
        # this key and the suffix -load-test.near are hardcoded in create_genesis_file()
        with open(home / 'validator_key.json', 'w') as f:
            json.dump(
                {
                    'account_id':
                        f'{validator_account + "-load-test.near"}',
                    'public_key':
                        'ed25519:76NVkDErhbP1LGrSAf5Db6BsFJ6LBw6YVA4BsfTBohmN',
                    'secret_key':
                        'ed25519:3cCk8KUWBySGCxBcn1syMoY5u73wx5eaPLRbQcMi23LwBA3aLsqEbA33Ww1bsJaFrchmDciGe9otdn45SrDSkow2'
                }, f)


def init_shardnet_dirs(neard):
    ordinal = 0
    dirs = []

    for account_id in SHARDNET_VALIDATORS:
        home = dot_near() / f'shardnet_{account_id}'
        dirs.append(str(home))
        init_shardnet_dir(neard, home, ordinal, validator_account=account_id)
        ordinal += 1

    observer = dot_near() / 'mirror/target-offline'
    init_shardnet_dir(neard, observer, ordinal, validator_account=None)
    shutil.copy(dot_near() / SOURCE_DIR / 'output/mirror-secret.json',
                observer / 'mirror-secret.json')
    return dirs, observer


def create_shardnet(config, near_root):
    binary_name = config.get('binary_name', 'neard')
    neard = os.path.join(near_root, binary_name)
    try:
        shutil.rmtree(dot_near() / SOURCE_DIR / 'output')
    except FileNotFoundError:
        pass
    try:
        subprocess.check_output([
            neard, "--home",
            dot_near() / SOURCE_DIR, "view-state", "dump-state", "--stream",
            "--height", "25"
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"dump-state" command failed: output: {e.stdout}')
    try:
        subprocess.check_output([
            os.path.join(near_root, 'mirror'),
            'prepare',
            '--records-file-in',
            dot_near() / SOURCE_DIR / 'output/records.json',
            '--records-file-out',
            dot_near() / SOURCE_DIR / 'output/mirror-records.json',
            '--secret-file-out',
            dot_near() / SOURCE_DIR / 'output/mirror-secret.json',
        ],
                                stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.exit(f'"mirror prepare" command failed: output: {e.stdout}')

    os.mkdir(dot_near() / SOURCE_DIR / 'output/forked')
    genesis_filename_in = dot_near() / SOURCE_DIR / 'output/genesis.json'
    genesis_filename_out = dot_near(
    ) / SOURCE_DIR / 'output/forked/genesis.json'
    records_filename_in = dot_near() / SOURCE_DIR / 'output/mirror-records.json'
    records_filename_out = dot_near(
    ) / SOURCE_DIR / 'output/forked/records.json'
    create_genesis_file(SHARDNET_VALIDATORS,
                        genesis_filename_in=genesis_filename_in,
                        genesis_filename_out=genesis_filename_out,
                        records_filename_in=records_filename_in,
                        records_filename_out=records_filename_out,
                        rpc_node_names=[],
                        chain_id='shardnet',
                        append=True,
                        epoch_length=20,
                        node_pks=None,
                        increasing_stakes=0.0,
                        num_seats=len(SHARDNET_VALIDATORS))
    return init_shardnet_dirs(neard)


def mirror_cleanup(process):
    process.send_signal(signal.SIGINT)
    try:
        process.wait(5)
    except:
        process.kill()
        logger.error('can\'t kill mirror process')


def start_mirror(near_root, target_home, boot_node):
    with open(target_home / 'config.json', 'r') as f:
        config = json.load(f)
        config['network']['boot_nodes'] = boot_node.addr_with_pk()
    with open(target_home / 'config.json', 'w') as f:
        json.dump(config, f)
    env = os.environ.copy()
    env["RUST_LOG"] = "actix_web=warn,mio=warn,tokio_util=warn,actix_server=warn,actix_http=warn," + env.get(
        "RUST_LOG", "debug")
    with open(dot_near() / 'mirror/offline-stdout', 'ab') as stdout, \
        open(dot_near() / 'mirror/offline-stderr', 'ab') as stderr:
        process = subprocess.Popen([
            os.path.join(near_root, 'mirror'),
            'run',
            "--source-home",
            dot_near() / SOURCE_DIR,
            "--target-home",
            target_home,
            '--secret-file',
            target_home / 'mirror-secret.json',
        ],
                                   stdin=subprocess.DEVNULL,
                                   stdout=stdout,
                                   stderr=stderr,
                                   env=env)
    logger.info(f'Started mirror process {process.poll()}')
    atexit.register(mirror_cleanup, process)
    return process


def count_total_txs(node, min_height=0):
    total = 0
    h = node.get_latest_block().hash
    while True:
        block = node.get_block(h)['result']
        height = int(block['header']['height'])
        if height < min_height:
            return total

        for c in block['chunks']:
            if int(c['height_included']) == height:
                chunk = node.get_chunk(c['chunk_hash'])['result']
                total += len(chunk['transactions'])

        h = block['header']['prev_hash']
        if h == '11111111111111111111111111111111':
            return total


def main():
    config_changes = {}
    for i in range(NUM_VALIDATORS + 1):
        config_changes[i] = {"tracked_shards": [0, 1, 2, 3], "archive": True}

    config = load_config()
    near_root = config['near_root']

    shardnet_node_dirs, shardnet_observer_dir = create_shardnet(
        config, near_root)

    shardnet_nodes = [spin_up_node(config, near_root, shardnet_node_dirs[0], 0)]
    ordinal = 0
    for i in range(1, len(shardnet_node_dirs)):
        ordinal += 1
        shardnet_nodes.append(
            spin_up_node(config,
                         near_root,
                         shardnet_node_dirs[i],
                         ordinal,
                         boot_node=shardnet_nodes[0]))

    try:
        os.remove(dot_near() / 'mirror/offline-stdout')
        os.remove(dot_near() / 'mirror/offline-stderr')
    except FileNotFoundError:
        pass

    p = start_mirror(near_root, shardnet_observer_dir, shardnet_nodes[0])

    restarted = False

    for height, block_hash in utils.poll_blocks(shardnet_nodes[0],
                                                timeout=int(1e20)):
        code = p.poll()
        if code is not None:
            assert code == 0
            break

        if not restarted and height >= 90:
            logger.info('stopping mirror process')
            p.terminate()
            p.wait()
            with open(dot_near() / 'mirror/offline-stderr', 'ab') as stderr:
                stderr.write(
                    b'<><><><><><><><><><><><> restarting <><><><><><><><><><><><><><><><><><><><>\n'
                )
                stderr.write(
                    b'<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>\n'
                )
                stderr.write(
                    b'<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>\n'
                )
            p = start_mirror(near_root, shardnet_observer_dir,
                             shardnet_nodes[0])
            restarted = True

    total_txs = count_total_txs(shardnet_nodes[0])
    print(f'got {total_txs} txs')


if __name__ == '__main__':
    main()
