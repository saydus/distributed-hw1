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


class BrokerAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        LOOKUP = 4,
        BROKE = 5,
        COMPLETED = 6

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.logger = logger
        self.timeout = None

        self.lookup = None
        self.dissemination = None
        self.name = None
        self.mw_obj = None

    def configure(self, args):
        self.logger.info("BrokerAppln: configure")
        self.state = self.State.CONFIGURE
        self.timeout = args.timeout * 1000
        self.name = args.name

        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]

        self.mw_obj = BrokerMW(self.logger, self.lookup, self)
        self.mw_obj.configure(args)

        self.logger.info("BrokerAppln: configure: completed")

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
        return self.timeout

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

    # json_path_dht
    parser.add_argument("-j", "--json_path_dht", default="dht.json",
                        help="json_path_dht (default: dht.json)")

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
