import base58
import time
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger
import transaction
import key
import utils


def send_add_access_key(node, key, target_key, nonce, block_hash):
    action = transaction.create_full_access_key_action(target_key.decoded_pk())
    tx = transaction.sign_and_serialize_transaction(target_key.account_id,
                                                    nonce, [action], block_hash,
                                                    key.account_id,
                                                    key.decoded_pk(),
                                                    key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'sent add key tx for {target_key.account_id} {target_key.pk}: {res}')


def send_delete_access_key(node, key, target_key, nonce, block_hash):
    action = transaction.create_delete_access_key_action(
        target_key.decoded_pk())
    tx = transaction.sign_and_serialize_transaction(target_key.account_id,
                                                    nonce, [action], block_hash,
                                                    target_key.account_id,
                                                    key.decoded_pk(),
                                                    key.decoded_sk())
    res = node.send_tx(tx)
    logger.info(
        f'sent delete key tx for {target_key.account_id} {target_key.pk}: {res}'
    )


class AddedKey:

    def __init__(self, key):
        self.nonce = None
        self.key = key

    def send_if_inited(self, node, transfers, block_hash):
        if self.nonce is None:
            self.nonce = node.get_nonce_for_pk(self.key.account_id,
                                               self.key.pk,
                                               finality='final')
            if self.nonce is not None:
                logger.info(
                    f'added key {self.key.account_id} {self.key.pk} inited @ {self.nonce}'
                )

        if self.nonce is not None:
            for (receiver_id, amount) in transfers:
                self.nonce += 1
                tx = transaction.sign_payment_tx(self.key, receiver_id, amount,
                                                 self.nonce, block_hash)
                res = node.send_tx(tx)
                logger.info(
                    f'send tx {self.key.account_id} to {receiver_id}: {res}'
                )

    def account_id(self):
        return self.key.account_id

    def inited(self):
        return self.nonce is not None

    def check_inited(self, node):
        if self.nonce is not None:
            return True
        self.nonce = node.get_nonce_for_pk(self.key.account_id,
                                           self.key.pk,
                                           finality='final')
        if self.nonce is not None:
            logger.info(
                f'added key {self.key.account_id} {self.key.pk} inited @ {self.nonce}'
            )
        return self.nonce is not None


class ImplicitAccount:

    def __init__(self):
        self.key = AddedKey(key.Key.implicit_account())

    def account_id(self):
        return self.key.account_id()

    def transfer(self, node, sender_key, amount, block_hash, nonce):
        tx = transaction.sign_payment_tx(sender_key, self.account_id(), amount,
                                         nonce, block_hash)
        res = node.send_tx(tx)
        logger.info(
            f'sent {amount} to initialize implicit account {self.account_id()}: {res}'
        )

    def send_if_inited(self, node, transfers, block_hash):
        self.key.send_if_inited(node, transfers, block_hash)

    def inited(self):
        return self.key.inited()

class TestAccount:
    def __init__(self):
        self.account = ImplicitAccount()
        new_key = key.Key.from_random(self.account.account_id())

        logger.info(f'account ID: {self.account.account_id()} extra key: {new_key.pk}')

        self.new_key = AddedKey(new_key)
        self.key_added = False
        self.key_found = False


def init_implicit_accounts(node, implicit_accounts):
    height, block_hash = node.get_latest_block()
    block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

    nonce = 10
    for t in implicit_accounts:
        t.account.transfer(node, node.signer_key, 10**24,
                            block_hash_bytes, nonce)
        nonce += 1

def send_add_keys(node, implicit_accounts):
    for height, block_hash in utils.poll_blocks(node, timeout=200):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        for t in implicit_accounts:
            if t.key_found:
                continue

            if not t.key_added:
                if t.account.key.check_inited(node):
                    logger.info(f'implicit account {t.account.account_id()} inited. Adding extra key')

                    t.account.key.nonce += 1
                    send_add_access_key(node, t.account.key.key,
                                        t.new_key.key, t.account.key.nonce,
                                        block_hash_bytes)
                    t.account.key.nonce += 1
                    t.key_added = True
                else:
                    logger.info(f'implicit account {t.account.account_id()} not inited.')
            else:
                if t.new_key.check_inited(node):
                    logger.info(f'implicit account {t.account.account_id()} extra key inited')
                    t.key_found = True

        if all([t.key_found for t in implicit_accounts]):
            break

def send_txs(node, implicit_accounts):
    transfers = [(a.account.account_id(), 100) for a in implicit_accounts]

    n = 0
    for height, block_hash in utils.poll_blocks(node, timeout=200):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        print(f'send txs at {height}')
        for t in implicit_accounts:
            t.account.send_if_inited(node, transfers, block_hash_bytes)
        
        n += 1
        if n > 10:
            break

def send_node_txs(node, node_keys):
    transfers = [(node_keys[0].account_id(), 100)] * 100
    print(transfers[:2])

    n = 0
    for height, block_hash in utils.poll_blocks(node, timeout=200):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        print(f'send node txs at {height}')
        for k in node_keys:
            k.send_if_inited(node, transfers, block_hash_bytes)

        n += 1
        if n > 10:
            break

def main():
    nodes = start_cluster(
        num_nodes=2,
        num_observers=0,
        num_shards=6,
        config=None,
        genesis_config_changes=[["use_production_config", True]],
        client_config_changes={
            0: {
                "tracked_shards": [0, 1, 2, 3, 4, 5]
            },
            1: {
                "tracked_shards": [0, 1, 2, 3, 4, 5]
            },
        })

    implicit_accounts = []
    for i in range(30):
        implicit_accounts.append(TestAccount())

    init_implicit_accounts(nodes[0], implicit_accounts)

    send_add_keys(nodes[0], implicit_accounts)

    send_txs(nodes[0], implicit_accounts)

if __name__ == '__main__':
    main()
