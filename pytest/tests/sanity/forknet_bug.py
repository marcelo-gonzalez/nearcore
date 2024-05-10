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
                node.send_tx(tx)

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

    implicit_account = ImplicitAccount()
    logger.info(f'account ID: {implicit_account.account_id()}')
    key_added = False
    key_deleted = False
    transfered = False
    new_key = key.Key.from_random(implicit_account.account_id())
    new_key = AddedKey(new_key)

    for height, block_hash in utils.poll_blocks(nodes[0], timeout=200):
        block_hash_bytes = base58.b58decode(block_hash.encode('utf8'))

        if not transfered:
            implicit_account.transfer(nodes[0], nodes[0].signer_key, 10**24,
                                      block_hash_bytes, 10)
            transfered = True
            continue

        if implicit_account.key.check_inited(nodes[0]):
            logger.info('implicit account inited')
            if not key_added:
                implicit_account.key.nonce += 1
                send_add_access_key(nodes[0], implicit_account.key.key,
                                    new_key.key, implicit_account.key.nonce,
                                    block_hash_bytes)
                implicit_account.key.nonce += 1
                key_added = True
            elif not key_deleted:
                send_delete_access_key(nodes[0], implicit_account.key.key,
                                       implicit_account.key.key,
                                       implicit_account.key.nonce,
                                       block_hash_bytes)
                implicit_account.key.nonce += 1
                key_deleted = True
                logger.info('key deleted. sleeping')
                time.sleep(10)
                return


if __name__ == '__main__':
    main()
