###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.


# import
from enum import Enum
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging

# for logging. Use it in place of print statements.
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2


from ZooKeeperAdapter import ZooKeeperAdapter
from kazoo.recipe.election import Election

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import json


###################################
#
# Parse command line arguments
#
###################################


class DiscoveryAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        WAIT = 2,
        READY = 3,
        COMPLETED = 4

    def __init__(self, logger):
        self.logger = logger
        self.state = self.State.INITIALIZE

        self.lookup = None
        self.dissemination = None

        self.mw_obj = None

        self.publishers = set()
        self.subscribers = set()
        self.brokers = set()

        self.publisher_to_ip_port = {}
        self.topic_to_publishers = {}
        self.broker_to_ip_port = {}

        # Zookeeper related stuff
        zk_hosts = "localhost:2181"
        self.zookeeper_adapter = ZooKeeperAdapter(zk_hosts)
        # Unique identifier for this Discovery service instance. Will be assigned in configure()
        self.node_id = None

        # election object
        self.election = None

        self.zk_client = None
        self.zk_am_leader = False
        self.addr = None
        self.sub_port = None
        self.port = None

        self.broker_leader = {
            'group1': None,
            'group2': None,
            'group3': None
        }

    def become_leader(self):
        print(
            f"Instance {self.node_id} is now the leader of the Discovery service.")

    def run(self):
        self.election.run(self.become_leader)

    def configure(self, args):
        try:
            self.logger.info("DiscoveryAppln: configure")

            self.node_id = args.name

            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config['Discovery']['Strategy']
            self.dissemination = config['Dissemination']['Strategy']

            self.group_to_topics_mapping = {
                'group1': config['GroupToTopicMapping']['group1'].split(','),
                'group2': config['GroupToTopicMapping']['group2'].split(','),
                'group3': config['GroupToTopicMapping']['group3'].split(',')
            }

            self.mw_obj = DiscoveryMW(self.logger)
            self.mw_obj.configure()

            self.addr = args.addr
            self.sub_port = args.sub_port
            self.port = args.port
            self.zookeeper_addr = args.zookeeper

            self.zk_client = KazooClient(hosts=args.zookeeper)
            self.zk_client.start()

            # Conduct the election of a leader
            self.zk_am_leader = self.zookeeper_discovery()

            @self.zk_client.ChildrenWatch('/discovery')
            def watch_children(children):
                if (len(children) == 0):
                    self.zk_am_leader = self.zookeeper_discovery()
                return

            self.zk_client.ensure_path('/pubs')

            @self.zk_client.ChildrenWatch('/pubs')
            def watch_publishers(children):
                self.zookeeper_pubs_children(children)

            # If using Broker dissemination, set up a watch for broker leader too
            if (self.dissemination == 'Broker'):
                brokers_path = '/brokers'
                self.zk_client.ensure_path(brokers_path)

                @self.zk_client.ChildrenWatch(brokers_path)
                def watch_brokers_children(children):
                    self.zookeeper_broker_children(children)

            self.logger.info("DiscoveryAppln: configure: completed")
        except Exception as e:
            raise e

    def zookeeper_discovery(self):
        # Create an ephemeral node for Discovery
        try:
            node_dict = {
                'port': self.port,
                'sub_port': self.sub_port,
                'addr': self.addr,
                'name': self.name
            }
            data_bytes = json.dumps(node_dict).encode('utf-8')

            self.zk_client.create(
                '/discovery/leader', ephemeral=True, makepath=True, value=data_bytes)
            self.logger.info(
                "Ephemeral node in /discovery/leader created, we are a leader")
            return True

        except NodeExistsError:
            self.logger.info(
                "Ephemeral node /discovery/leader already exists, we are NOT a leader")

            data_bytes, _ = self.zk_client.get('/discovery/leader')
            node_dict = json.loads(data_bytes.decode('utf-8'))
            self.logger.info(
                f"Connecting to leader {self.name} at {node_dict['addr']}:{node_dict['sub_port']}")
            self.mw_obj.subscribe_to_leader(
                node_dict['addr'], node_dict['sub_port'])

            return False

    def zookeeper_pubs_children(self, children):
        self.logger.info('Pubs watch triggered')
        remaining_publishers = [
            publisher for publisher in self.publishers if publisher not in children]

        # registered_publishers now contains the publishers that died
        # notify subscribers and brokers of the nodes they need to unsubscribe from
        self.logger.info(f'Died publishers: {remaining_publishers}')

        for died_publisher_name in remaining_publishers:
            ipport = self.publisher_to_ip_port[died_publisher_name]
            ip = ipport[:(ipport.find(':')-1)]
            port = ipport[ipport.find(':') + 1:]

            self.mw_obj.send_unsubscribe_update({
                'addr': ip,
                'port': port
            })

            # remove the publisher from the state
            self.publishers.remove(died_publisher_name)
            del self.publisher_to_ip_port[died_publisher_name]

            for topic in self.topic_to_publishers:
                if died_publisher_name in self.topic_to_publishers[topic]:
                    self.topic_to_publishers[topic].remove(
                        died_publisher_name)

        self.logger.info(
            f'New registered publishers: reg_pubs:{self.publishers}, pub_ipport:{self.publisher_to_ip_port}, topic_pubid:{self.topic_to_publishers}')

        self.mw_obj.publish_discovery_update({
            "publishers": self.publishers,
            "publisher_to_ip_port": self.publisher_to_ip_port,
            "topic_to_publishers": self.topic_to_publishers,
            "subscribers": self.subscribers,
            "brokers": self.brokers,
            "broker_to_ip_port": self.broker_to_ip_port})

    def zookeeper_broker_children(self, children):
        new_leaders = {
            'group1': None,
            'group2': None,
            'group3': None,
        }

        for child in children:
            data_bytes, _ = self.zk_client.get('/brokers/' + child)
            node_dict = json.loads(data_bytes.decode('utf-8'))
            new_leaders[node_dict['group']] = node_dict['name']

            new_leaders[child] = node_dict

        self.check_group_leaders(new_leaders)
        self.broker_leader = new_leaders

    def check_group_leaders(self, new_leaders):
        for group_name in ["group1", "group2", "group3"]:
            if (self.broker_leader[group_name] == None):
                if (new_leaders[group_name] != None):
                    self.mw_obj.send_subscribe_update({
                        'update_type': 'broker',
                        'addr': new_leaders[group_name]['addr'],
                        'port': new_leaders[group_name]['port'],
                        'topics': self.group_to_topics_mapping[group_name]
                    })
                else:
                    if (new_leaders[group_name] == None):
                        self.mw_obj.send_unsubscribe_update({
                            'addr': self.broker_leader[group_name]['addr'],
                            'port': self.broker_leader[group_name]['port']
                        })
                        self.brokers.remove(
                            self.broker_leader[group_name]['name'])
                        del self.broker_to_ip_port[self.broker_leader[group_name]['name']]

                    elif (self.broker_leader[group_name]['name'] != new_leaders[group_name]['name']):
                        self.mw_obj.send_unsubscribe_update({
                            'addr': self.broker_leader[group_name]['addr'],
                            'port': self.broker_leader[group_name]['port']
                        })
                        self.brokers.remove(
                            self.broker_leader[group_name]['name'])
                        del self.broker_to_ip_port[self.broker_leader[group_name]['name']]

                        self.mw_obj.send_subscribe_update({
                            'update_type': 'broker',
                            'addr': new_leaders[group_name]['addr'],
                            'port': new_leaders[group_name]['port'],
                            'topics': self.group_to_topics_mapping[group_name]
                        })

    def handle_register(self, register_req):
        try:
            self.logger.info("DiscoveryAppln: handle_register")
            ip_addr = register_req.info.addr
            port = register_req.info.port
            id = register_req.info.id
            addr = ip_addr + ":" + str(port)

            if register_req.role == discovery_pb2.ROLE_PUBLISHER:
                self.logger.info("DiscoveryAppln: handle_register: publisher")
                if (id in self.publishers):
                    # got a duplicate registration
                    self.logger.info(
                        "DiscoveryAppln: handle_register: publisher: duplicate registration")
                    self.mw_obj.send_register_resp(
                        False, "Duplicate registration")
                else:
                    self.publishers.add(id)
                    topiclist = register_req.topiclist
                    self.publisher_to_ip_port[id] = addr

                    for topic in topiclist:
                        self.topic_to_publishers.setdefault(
                            topic, set()).add(id)

                    self.logger.info("DiscoveryAppln: handle_register: publisher: topic_to_publishers: {}".format(
                        self.topic_to_publishers))
                    self.mw_obj.send_register_resp(True)

                    # Zookeeper register
                    self.mw_obj.send_subscribe_update({
                        'update_type': 'pub',
                        'addr': ip_addr,
                        'port': port,
                        'topics': list(topiclist)
                    })

            elif register_req.role == discovery_pb2.ROLE_SUBSCRIBER:
                self.logger.info("DiscoveryAppln: handle_register: subscriber")
                if (id in self.subscribers):
                    # got a duplicate registration
                    self.logger.info(
                        "DiscoveryAppln: handle_register: subscriber: duplicate registration")
                    self.mw_obj.send_register_resp(
                        False, "Duplicate registration")
                else:
                    self.subscribers.add(id)
                    self.mw_obj.send_register_resp(True)
                    # self.zookeeper_adapter.register_subscriber(id, addr)

            elif register_req.role == discovery_pb2.ROLE_BOTH:
                self.logger.info("DiscoveryAppln: handle_register: broker")
                if (id in self.brokers):
                    # got a duplicate registration
                    self.logger.info(
                        "DiscoveryAppln: handle_register: broker: duplicate registration")
                    self.mw_obj.send_register_resp(
                        False, "Duplicate registration")
                else:
                    self.brokers.add(id)
                    self.broker_to_ip_port[id] = addr
                    self.mw_obj.send_register_resp(True)

            self.mw_obj.publish_discovery_update(
                {
                    "publishers": self.publishers,
                    "publisher_to_ip_port": self.publisher_to_ip_port,
                    "topic_to_publishers": self.topic_to_publishers,
                    "subscribers": self.subscribers,
                    "brokers": self.brokers,
                    "broker_to_ip_port": self.broker_to_ip_port}
            )
            return None

        except Exception as e:
            raise e

    def handle_lookup_topic(self, lookup_req):
        self.logger.info("DiscoveryAppln: handle_lookup_topic")
        try:
            sockets = []

            if (self.dissemination == 'Broker'):
                # send a broker
                broker = next(iter(self.brokers), None)
                ip = self.broker_to_ip_port[broker]
                sockets.append(ip)
            else:
                # return ips of publishers
                for topic in lookup_req.topiclist:
                    if not topic in self.topic_to_publishers:
                        continue
                    for publisher in self.topic_to_publishers[topic]:
                        if (self.publisher_to_ip_port[publisher] not in sockets):
                            sockets.append(
                                self.publisher_to_ip_port[publisher])
                self.logger.info(
                    "DiscoveryAppln: handle_lookup_topic: sockets: {}".format(sockets))

            self.mw_obj.send_lookup_resp(sockets)
            return None
        except Exception as e:
            raise e

    def update_zookeeper_state(self, state):
        self.publishers = set(state['publishers'])
        self.publisher_to_ip_port = state['publisher_to_ip_port']
        self.topic_to_publishers = state['topic_to_publishers']
        self.subscribers = set(state['subscribers'])
        self.brokers = set(state['brokers'])
        self.broker_to_ip_port = state['broker_to_ip_port']

    def handle_lookup_all_publishers(self, lookup_req):
        sockets = []
        for publisher in self.publishers:
            sockets.append(self.publisher_to_ip_port[publisher])

        self.mw_obj.send_lookup_all_resp(sockets)
        return None

    def driver(self):
        try:
            self.logger.info("DiscoverAppln::driver")
            self.dump()

            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.WAIT
            self.mw_obj.event_loop(timeout=None)

            self.logger.info("DiscoverAppln::driver: completed")

        except Exception as e:
            raise e

    ########################################
    # dump the contents of the object
    ########################################

    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info("**********************************")
            self.logger.info("DiscoverAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info(
                "     Dissemination: {}".format(self.dissemination))
            self.logger.info(
                "     Num Publishers: {}".format(self.num_publishers))
            self.logger.info(
                "     Num Subscribers: {}".format(self.num_subscribers))
            self.logger.info("**********************************")

        except Exception as e:
            raise e


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Discovery Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-n", "--name", default="discovery",
                        help="Some name assigned to us. Keep it unique per discovery process")

    parser.add_argument("-c", "--config", default="config.ini",
                        help="configuration file (default: config.ini)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-z", "--zookeeper",
                        default='localhost:2181', help="Zookeeper instance")

    parser.add_argument("-a", "--addr", default="localhost")

    parser.add_argument("-p", "--port", type=int, default=8888)

    parser.add_argument("-s", "--sub_port", type=int, default=8888,
                        help="Port used by the discovery node to other subs")
    return parser.parse_args()


###################################
#
# Main program
#
###################################
def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info(
            "Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln.py")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(
            logger.getEffectiveLevel()))

        # Obtain a publisher application
        logger.debug("Main: obtain the discovery appln object")
        pub_app = DiscoveryAppln(logger)

        # configure the object
        logger.debug("Main: configure the publisher appln object")
        pub_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the publisher appln driver")
        pub_app.driver()

    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        type, value, traceback = sys.exc_info()
        logger.error("Exception type: {}".format(type))
        logger.error("Exception value: {}".format(value))
        return


###################################
#
# Main entry point
#
###################################
if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
