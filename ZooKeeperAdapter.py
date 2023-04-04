from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
import json


class ZooKeeperAdapter:
    def __init__(self, zk_hosts):
        # Initialize the Kazoo client and connect to ZooKeeper
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        # Ensure that the base zNodes exist
        self.zk.ensure_path("/discovery/publishers")
        self.zk.ensure_path("/discovery/subscribers")

    def register_publisher(self, pub_id, pub_info):
        try:
            # Create an ephemeral zNode for the publisher
            self.zk.create(
                f"/discovery/publishers/{pub_id}", value=pub_info.encode(), ephemeral=True)
        except NodeExistsError:
            # Update the information if the publisher already exists
            self.zk.set(
                f"/discovery/publishers/{pub_id}", value=pub_info.encode())

    def deregister_publisher(self, pub_id):
        try:
            # Delete the zNode for the publisher
            self.zk.delete(f"/discovery/publishers/{pub_id}")
        except NoNodeError:
            # Ignore if the publisher does not exist
            pass

    def register_subscriber(self, sub_id, sub_info):
        try:
            # Create an ephemeral zNode for the subscriber
            self.zk.create(
                f"/discovery/subscribers/{sub_id}", value=sub_info.encode(), ephemeral=True)
        except NodeExistsError:
            # Update the information if the subscriber already exists
            self.zk.set(
                f"/discovery/subscribers/{sub_id}", value=sub_info.encode())

    def deregister_subscriber(self, sub_id):
        try:
            # Delete the zNode for the subscriber
            self.zk.delete(f"/discovery/subscribers/{sub_id}")
        except NoNodeError:
            # Ignore if the subscriber does not exist
            pass

    def stop(self):
        # Stop the Kazoo client and disconnect from ZooKeeper
        self.zk.stop()
