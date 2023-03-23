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
        self.num_publishers = 0
        self.num_subscribers = 0

        self.publishers = set()
        self.subscribers = set()
        self.brokers = set()

        self.publisher_to_ip_port = {}
        self.topic_to_publishers = {}
        self.broker_to_ip_port = {}

        self.name = None

    def configure(self, args):
        try:
            self.logger.info("DiscoveryAppln: configure")
            self.num_publishers = args.publishers
            self.num_subscribers = args.subscribers
            self.name = args.name

            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config['Discovery']['Strategy']
            self.dissemination = config['Dissemination']['Strategy']

            self.mw_obj = DiscoveryMW(self.logger, self.lookup)
            self.mw_obj.set_upcall_handle(self)
            self.mw_obj.configure(args)

            self.logger.info("DiscoveryAppln: configure: completed")
        except Exception as e:
            raise e

    def handle_isready_dht(self, isready_data, frames, sent_timestamp):
        dht_data = isready_data.dht_is_ready
        visited_nodes_collection = set(dht_data.visited_nodes)
        registered_pubs_collection = set(dht_data.pubs)
        registered_subs_collection = set(dht_data.subs)
        registered_brokers_collection = set(dht_data.brokers)

        if self.name in visited_nodes_collection:
            # Full circle completed
            subscribersReady = (self.num_subscribers ==
                                len(registered_subs_collection))
            publishersReady = (self.num_publishers ==
                               len(registered_pubs_collection))

            brokersReady = (self.dissemination != 'Broker' or (
                self.dissemination == 'Broker' and len(registered_brokers_collection) != 0))

            self.logger.debug("brokersReady = %s", str(brokersReady))

            systemReady = subscribersReady and publishersReady and brokersReady

            self.mw_obj.send_isready(systemReady, frames, sent_timestamp)
            self.logger.debug(
                f"IS_READY, visited nodes: {str(visited_nodes_collection)} ")

            return None

        else:
            visited_nodes_collection.add(self.name)
            registered_pubs_collection.update(self.publishers)
            registered_subs_collection.update(self.subscribers)
            registered_brokers_collection.update(self.brokers)
            # Forward the is_ready request to the subsequent node in dht ring
            self.mw_obj.send_isready_request_to_next_node(
                visited_nodes_collection, registered_pubs_collection, registered_subs_collection, registered_brokers_collection, frames, sent_timestamp)

            return None

    def handle_isready(self, isready_body, frames, timestamp):
        if self.lookup == 'DHT':
            return self.handle_isready_dht(isready_body, frames, timestamp)

        isSubscribersReady = len(self.subscribers) == self.num_subscribers
        isPublishersReady = len(self.publishers) == self.num_publishers
        isBrokersReady = (len(self.brokers) != 0 if self.dissemination ==
                          'Broker' else True)

        self.logger.info("DiscoveryAppln: handle_isready: isSubscribersReady: {}, isPublishersReady: {}, isBrokersReady: {}".format(
            isSubscribersReady, isPublishersReady, isBrokersReady))

        self.mw_obj.send_isready(
            isSubscribersReady and isPublishersReady and isBrokersReady, frames, timestamp)
        return None

    def handle_register(self, register_req, frames, timestamp):
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
                        False, "Duplicate registration", frames, timestamp)
                else:
                    self.publishers.add(id)
                    topiclist = register_req.topiclist
                    self.publisher_to_ip_port[id] = addr

                    for topic in topiclist:
                        self.topic_to_publishers.setdefault(
                            topic, set()).add(id)

                    self.logger.info("DiscoveryAppln: handle_register: publisher: topic_to_publishers: {}".format(
                        self.topic_to_publishers))
                    self.mw_obj.send_register_resp(True, "", frames, timestamp)

            elif register_req.role == discovery_pb2.ROLE_SUBSCRIBER:
                self.logger.info("DiscoveryAppln: handle_register: subscriber")
                if (id in self.subscribers):
                    # got a duplicate registration
                    self.logger.info(
                        "DiscoveryAppln: handle_register: subscriber: duplicate registration")
                    self.mw_obj.send_register_resp(
                        False, "Duplicate registration", frames, timestamp)
                else:
                    self.subscribers.add(id)
                    self.mw_obj.send_register_resp(True, "", frames, timestamp)

            elif register_req.role == discovery_pb2.ROLE_BOTH:
                self.logger.info("DiscoveryAppln: handle_register: broker")
                if (id in self.brokers):
                    # got a duplicate registration
                    self.logger.info(
                        "DiscoveryAppln: handle_register: broker: duplicate registration")
                    self.mw_obj.send_register_resp(
                        False, "Duplicate registration", frames, timestamp)
                else:
                    self.brokers.add(id)
                    self.broker_to_ip_port[id] = addr
                    self.mw_obj.send_register_resp(True, "", frames, timestamp)

            return None

        except Exception as e:
            raise e

    def handle_lookup_topic(self, lookup_req, frames, timestamp):
        self.logger.info("DiscoveryAppln: handle_lookup_topic")
        try:
            sockets = []

            if (self.dissemination == 'Broker'):
                if len(self.brokers) != 0:
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
                    "DiscoveryAppln: handle_lookup_topic: sockets: {}, lookupreq: {}".format(sockets, lookup_req))

            if (self.lookup != "DHT"):
                self.mw_obj.send_lookup(sockets, frames, timestamp)
                return None

            self.logger.info(
                "DiscoveryAppln: handle_lookup_topic: now send the request to other node")

            visited_nodes = set(lookup_req.visited_nodes)
            added_sockets = set(lookup_req.sockets_for_connection)
            added_sockets.update(sockets)

            self.logger.info(
                "DiscoveryAppln: handle_lookup_topic: updated sockets")

            if self.name in visited_nodes:
                self.logger.info(
                    "DiscoveryAppln: handle_lookup_topic: send response")
                self.mw_obj.send_lookup_resp(added_sockets, frames, timestamp)
                return None

            else:
                self.logger.info(
                    "DiscoveryAppln: handle_lookup_topic: send to next node")
                self.logger.info(
                    "DiscoveryAppln: handle_lookup_topic: send to next node, topiclist: {}".format(lookup_req.topiclist))

                visited_nodes.add(self.name)
                self.mw_obj.send_lookup_request_to_next_node(lookup_req.topiclist, False,
                                                             visited_nodes, added_sockets, frames, timestamp)
                return None

        except Exception as e:
            raise e

    def handle_lookup_all_publishers(self, lookup_req, frames, timestamp):
        sockets = []
        for publisher in self.publishers:
            sockets.append(self.publisher_to_ip_port[publisher])

        if self.lookup != "DHT":
            self.mw_obj.send_lookup_all_resp(sockets, frames, timestamp)
            return None

        visited_nodes = set(lookup_req.visited_nodes)
        added_sockets = set(lookup_req.sockets_for_connection)
        added_sockets.update(sockets)

        if self.name in visited_nodes:
            self.mw_obj.send_lookup_resp(added_sockets, frames, timestamp)
            return None

        else:
            visited_nodes.add(self.name)
            self.mw_obj.send_lookup_request_to_next_node(lookup_req.topiclist, True,
                                                         visited_nodes, added_sockets, frames, timestamp)
            return None

    def driver(self):
        try:
            self.logger.info("DiscoverAppln::driver")
            self.dump()

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

    parser.add_argument("-P", "--publishers", type=int,
                        default=1, help="Number of publishers")

    parser.add_argument("-S", "--subscribers", type=int,
                        default=1, help="Number of subscribers")

    parser.add_argument("-c", "--config", default="config.ini",
                        help="configuration file (default: config.ini)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-j", "--json_path_dht", type=str,
                        default='dht.json', help="DHT nodes in the ring.")

    parser.add_argument("-n", "--name", type=str,
                        default='discovery', help="Discovery node")

    parser.add_argument("-p", "--port", type=int, default=8888,
                        help="Port used by this discovery node")

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
