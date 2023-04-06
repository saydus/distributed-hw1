###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers.

from enum import Enum
import zmq

import sys
import time
import argparse
import configparser
import logging

from CS6381_MW import discovery_pb2
from CS6381_MW.BrokerMW import BrokerMW

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import json


class BrokerAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        LOOKUP = 4,
        BROKE = 5,
        COMPLETED = 6

    def __init__(self, logger, args):
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.timeout = None

        self.lookup = None
        self.dissemination = None
        self.name = None
        self.mw_obj = None

        self.zk_client = None
        self.discovery_addr = None
        self.discovery_port = None
        self.discovery_sync_port = None
        self.addr = None
        self.port = None
        self.is_leader = False

        config = configparser.ConfigParser()
        config.read(args.config)

        self.group = None
        self.topics_assigned = config["GroupToTopicMapping"][self.group]

    def configure(self, args):
        self.logger.info("BrokerAppln: configure")
        self.state = self.State.CONFIGURE
        self.timeout = args.timeout * 1000
        self.name = args.name

        self.addr = args.addr
        self.port = args.port

        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]

        self.mw_obj = BrokerMW(self.logger)
        self.mw_obj.configure(args)

        self.zookeeper_addr = args.zookeeper
        self.zk_client = KazooClient(hosts=self.zookeeper_addr)
        self.zk_client.start()

        self.zk_am_broker_leader = self.register_broker()
        # Start a children watch on /discovery

        @self.zk_client.ChildrenWatch('/brokers')
        def watch_discovery_children(children):
            if (len(children) == 0):
                self.zk_am_broker_leader = self.register_broker()

        while not self.zk_am_broker_leader:
            time.sleep(1)

        # Get discovery Service
        self.connect_to_leader()

        # Set up a watch for the primary discovery
        self.watch_discovery()

        self.logger.info("BrokerAppln: configure: completed")

    def watch_discovery(self):
        @self.zk_client.ChildrenWatch('/discovery')
        def watch_discovery_children(children):
            if (len(children) == 0):
                if (self.discovery_addr != None):
                    self.mw_obj.disconnect_from_discovery(
                        self.discovery_addr, self.discovery_port, self.discovery_sync_port)

                self.discovery_addr = None
                self.discovery_port = None
                self.discovery_sync_port = None

                return

            else:
                leader_data, _ = self.zk_client.get('/discovery/leader')
                info = json.loads(leader_data.decode('utf-8'))

                if (info['addr'] != self.discovery_addr):
                    if (self.discovery_addr != None):
                        self.mw_obj.disconnect_from_discovery(
                            self.discovery_addr, self.discovery_port, self.discovery_sync_port)

                    self.mw_obj.connect_to_discovery(
                        info['addr'], info['port'], info['sub_port'])

                    self.discovery_addr = info['addr']
                    self.discovery_port = info['port']
                    self.discovery_sync_port = info['sub_port']
            return

    def connect_to_leader(self):
        while True:
            if not self.zk_client.exists("/discovery/leader"):
                time.sleep(1)
            else:
                leader, _ = self.zk_client.get('/discovery/leader')
                info = json.loads(leader.decode('utf-8'))

                self.discovery_addr = info['addr']
                self.discovery_port = info['port']
                self.discovery_sync_port = info['sub_port']
                # Connect to the new discovery and subscribe for updates from it
                self.mw_obj.connect_to_discovery(
                    info['addr'], info['port'], info['sub_port'])
                break

    def register_broker(self):
        try:
            data_bytes = json.dumps({
                "addr": self.addr,
                "port": self.port,
                "name": self.name
            }).encode("utf-8")

            self.zk_client.create(
                "/brokers/leader", ephemeral=True, makepath=True, value=data_bytes)
            self.logger.info(
                "Emphemeral /discovery/leader node already exists, we are not a leader")
            return True
        except NodeExistsError:
            self.logger.info(
                "Cannot create ephemeral /discovery/leader node, we are NOT a leader")
            return False

    def dump(self):
        ''' Pretty print '''
        try:
            self.logger.info("**********************************")
            self.logger.info("BrokerAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info(
                "     Dissemination: {}".format(self.dissemination))
            self.logger.info("**********************************")

        except Exception as e:
            raise e

    def driver(self):
        try:
            self.logger.info("BrokerAppln: driver")
            self.dump()
            self.mw_obj.set_upcall_handle(self)
            self.state = self.State.REGISTER

            self.mw_obj.event_loop(timeout=0)

            self.logger.info("BrokerAppln: driver: completed")
        except Exception as e:
            raise e

    def handle_register(self, resp):
        if resp.status == discovery_pb2.STATUS_SUCCESS:
            self.state = self.State.ISREADY
            self.logger.info("BrokerAppln: handle_register: SUCCESS")
            return 0
        else:
            raise Exception(
                "BrokerAppln: handle_register: received failure response on registration")

    def handle_isready(self, resp):
        if resp.status:
            self.state = self.State.LOOKUP
        else:
            time.sleep(30)
        return 0

    def handle_allpub_lookup(self, resp):
        self.mw_obj.connect_to_pubs(resp.publist)
        self.state = self.State.BROKE
        return None

    def invoke_operation(self):
        try:
            self.logger.info("BrokerAppln: invoke_operation")
            if self.state == self.State.REGISTER:
                self.mw_obj.register(self.name)
                return None
            elif self.state == self.State.ISREADY:
                self.mw_obj.is_ready()
                return None
            elif self.state == self.State.LOOKUP:
                self.mw_obj.allpub_lookup()
                return None

            elif self.state == self.State.BROKE:
                self.state = self.state.COMPLETED
                return 0
            elif self.state == self.State.COMPLETED:
                self.mw_obj.disable_event_loop()
                return None

        except Exception as e:
            raise e


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Broker Application")

    parser.add_argument("-n", "--name", default="broker",
                        help="Name assigned to us. Keep it unique per broker")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this broker to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=5599,
                        help="Port number on which our underlying ZMQ service runs, default=5599")

    parser.add_argument("-d", "--discovery", default="localhost:5555",
                        help="IP Addr:Port combo for the discovery service, default localhost:5555")

    parser.add_argument("-c", "--config", default="config.ini",
                        help="configuration file (default: config.ini)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-t", "--timeout", type=int, default=30,
                        help="Timeout for broking data.")

    parser.add_argument("-z", "--zookeeper", default='localhost:2181',
                        help="Zookeeper server address. default=localhost:2181")

    return parser.parse_args()


def main():
    try:
        logging.info("BrokerAppln: main")
        logger = logging.getLogger("BrokerAppln")

        args = parseCmdLineArgs()
        logger.setLevel(args.loglevel)
        app = BrokerAppln(logger)
        app.configure(args)
        app.driver()
    except Exception as e:
        logging.error("BrokerAppln: main: Exception: %s", str(e))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
