###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.

from topic_selector import TopicSelector

from CS6381_MW.SubscriberMW import SubscriberMW
from enum import Enum  # for an enumeration we are using to describe what state we are in

from CS6381_MW import discovery_pb2
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import json


MAX_MESSAGES_RECEIVED = 1000

##################################
#       SubscriberAppln class
##################################


class SubscriberAppln():

    # these are the states through which our subscriber appln object goes thru.
    # We maintain the state so we know where we are in the lifecycle and then
    # take decisions accordingly
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        LOOKUP = 4,
        LISTEN = 5,
        COMPLETED = 6

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None  # different topics to which we subscribe
        self.num_topics = None  # number of topics to which we subscribe
        self.lookup = None  # lookup method to use
        self.dissemination = None  # direct or via broker
        self.mw_obj = None  # middleware object
        self.logger = logger
        self.meesages_received = 0
        self.count_msgs = 0

        self.zk_client = None
        self.discovery_addr = None
        self.discovery_port = None
        self.discovery_sync_port = None
        self.zookeeper_addr = None

    ########################################
    # configure/initialize
    ########################################

    def configure(self, args):
        ''' Initialize the object '''

        try:
            # Here we initialize any internal variables
            self.logger.info("SubscriberAppln::configure")

            # set our current state to CONFIGURE state
            self.state = self.State.CONFIGURE

            # initialize our variables
            self.name = args.name  # our name

            self.num_topics = args.num_topics  # total num of topics we listen to

            # Now, get the configuration object
            self.logger.debug(
                "SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # Now get our topic list of interest
            self.logger.debug(
                "SubscriberAppln::configure - selecting our topic list")
            ts = TopicSelector()
            # let topic selector give us the desired num of topics
            self.topiclist = ts.interest(self.num_topics)

            # Now setup up our underlying middleware object to which we delegate
            # everything
            self.logger.debug(
                "SubscriberAppln::configure - initialize the middleware object")
            self.mw_obj = SubscriberMW(self.logger, self.topiclist)
            # pass remainder of the args to the m/w object
            self.mw_obj.configure(args)

            self.logger.info(
                "SubscriberAppln::configure - configuration complete")

            self.zookeeper_addr = args.zookeeper
            self.zk_client = KazooClient(hosts=self.zookeeper_addr)
            self.zk_client.start()

            looper = True
            while looper:
                if (self.zk_client.exists('/discovery/leader')):

                    leader_data, _ = self.zk_client.get('/discovery/leader')
                    info = json.loads(leader_data.decode('utf-8'))

                    self.discovery_sync_port = info['sub_port']
                    self.discovery_addr = info['addr']
                    self.discovery_port = info['port']

                    self.mw_obj.connect_to_discovery(
                        info['addr'], info['port'], info['sub_port'])

                    looper = False
                else:
                    time.sleep(1)

        except Exception as e:
            raise e

    def dump(self):
        try:
            self.logger.info("**********************************")
            self.logger.info("SubscriberAppln::dump")
            self.logger.info("------------------------------")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info(
                "     Dissemination: {}".format(self.dissemination))
            self.logger.info("     Num Topics: {}".format(self.num_topics))
            self.logger.info("     TopicList: {}".format(self.topiclist))
            self.logger.info("**********************************")

        except Exception as e:
            raise e

     ########################################
    # handle isready response method called as part of upcall
    #
    # Also a part of upcall handled by application logic
    ########################################

    def isready_response(self, isready_resp):
        ''' handle isready response '''

        try:
            self.logger.info("SubscriberAppln::isready_response")

            # Notice how we get that loop effect with the sleep (10)
            # by an interaction between the event loop and these
            # upcall methods.
            if not isready_resp.status:
                # discovery service is not ready yet
                self.logger.debug(
                    "SubscriberAppln::driver - Not ready yet; check again")
                # sleep between calls so that we don't make excessive calls
                time.sleep(10)

            else:
                # we got the go ahead
                # set the state to LOOKUP
                self.state = self.State.LOOKUP

            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return 0

        except Exception as e:
            raise e

    def lookup_response(self):
        try:
            self.logger.info("SubscriberAppln::islookup_response")
            # set the state to listen
            self.state = self.State.LISTEN

            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            return None

        except Exception as e:
            raise e

    def handle_data(self, strRcvd):
        ''' handle data '''

        try:
            self.logger.info("SubscriberAppln::handle_data")

            self.logger.info(
                "SubscriberAppln: Received a message {}".format(strRcvd))

            self.meesages_received += 1

            # return timeout of 0 so event loop calls us back in the invoke_operation
            # method, where we take action based on what state we are in.
            if self.meesages_received == MAX_MESSAGES_RECEIVED:
                self.state = self.State.COMPLETED
                return 0

            return None

        except Exception as e:
            raise e

    def invoke_operation(self):
        try:
            self.logger.info("SubscriberAppln::invoke_operation")

            # check what state are we in. If we are in REGISTER state,
            # we send register request to discovery service. If we are in
            # ISREADY state, then we keep checking with the discovery
            # service.
            if (self.state == self.State.REGISTER):
                # send a register msg to discovery service
                self.logger.debug(
                    "SubscriberAppln::invoke_operation - register with the discovery service {}".format(self.topiclist))
                self.mw_obj.register(self.name, self.topiclist)

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a register request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None

            elif (self.state == self.State.ISREADY):
                # Now keep checking with the discovery service if we are ready to go
                #
                # Note that in the previous version of the code, we had a loop. But now instead
                # of an explicit loop we are going to go back and forth between the event loop
                # and the upcall until we receive the go ahead from the discovery service.

                self.logger.debug(
                    "SubscriberAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready()  # send the is_ready? request

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return None

            elif (self.state == self.State.LOOKUP):
                self.logger.debug(
                    "SubscriberAppln::invoke_operation - start looking up")
                self.mw_obj.lookup()
                return None

            elif (self.state == self.State.COMPLETED):

                # we are done. Time to break the event loop. So we created this special method on the
                # middleware object to kill its event loop
                self.mw_obj.disable_event_loop()
                return None

            else:
                raise ValueError("Undefined state of the appln object")

        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        ''' handle register response '''
        try:
            self.logger.info("SubcriberAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.debug(
                    "PublisherAppln::register_response - registration is a success")

                # set our next state to isready so that we can then send the isready message right away
                self.state = self.State.ISREADY

                # return a timeout of zero so that the event loop in its next iteration will immediately make
                # an upcall to us
                return 0

            else:
                self.logger.debug(
                    "PublisherAppln::register_response - registration is a failure with reason {}".format(reg_resp.reason))
                raise ValueError("Publisher needs to have unique id")

        except Exception as e:
            raise e

    ########################################
    # driver program
    ########################################

    def driver(self):
        ''' Driver program '''

        try:
            self.logger.info("SubscriberAppln::driver")

            # dump our contents (debugging purposes)
            self.dump()

            # First ask our middleware to keep a handle to us to make upcalls.
            # This is related to upcalls. By passing a pointer to ourselves, the
            # middleware will keep track of it and any time something must
            # be handled by the application level, invoke an upcall.
            self.logger.debug("SubscriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)

            self.state = self.State.REGISTER

            # Now simply let the underlying middleware object enter the event loop
            # to handle events. However, a trick we play here is that we provide a timeout
            # of zero so that control is immediately sent back to us where we can then
            # register with the discovery service and then pass control back to the event loop
            #
            # As a rule, whenever we expect a reply from remote entity, we set timeout to
            # None or some large value, but if we want to send a request ourselves right away,
            # we set timeout is zero.
            #
            self.mw_obj.event_loop(timeout=0)  # start the event loop

            self.logger.info("PublisherAppln::driver completed")

        except Exception as e:
            raise e


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Subscriber Application")

    # Now specify all the optional arguments we support
    # At a minimum, you will need a way to specify the IP and port of the lookup
    # service, the role we are playing, what dissemination approach are we
    # using, what is our endpoint (i.e., port where we are going to bind at the
    # ZMQ level)

    parser.add_argument("-n", "--name", default="sub",
                        help="Some name assigned to us. Keep it unique per subscriber")

    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this subscriber to advertise (default: localhost)")

    parser.add_argument("-p", "--port", type=int, default=5588,
                        help="Port number on which our underlying subscriber ZMQ service runs, default=5588")

    parser.add_argument("-d", "--discovery", default="localhost:5555",
                        help="IP Addr:Port combo for the discovery service, default localhost:5555")

    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=1,
                        help="Number of topics to listen to, currently restricted to max of 9")

    parser.add_argument("-c", "--config", default="config.ini",
                        help="configuration file (de fault: config.ini)")

    parser.add_argument("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument("-z", "--zookeeper", default='localhost:2181',
                        help="Zookeeper instance addy:port (default: localhost:2181)")
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
        logger = logging.getLogger("SubscriberAppln")

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(
            logger.getEffectiveLevel()))

        # Obtain a publisher application
        logger.debug("Main: obtain the subscriber appln object")
        sub_app = SubscriberAppln(logger)

        # configure the object
        logger.debug("Main: configure the subscriber appln object")
        sub_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the subscriber appln driver")
        sub_app.driver()

    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return


if __name__ == "__main__":

    # set underlying default logging capabilities
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    main()
