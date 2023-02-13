###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

import zmq

from CS6381_MW import discovery_pb2


class BrokerMW():
    def __init__(self, logger):
        self.logger = logger
        self.req = None
        self.pub = None
        self.sub = None
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True

        self.addr = None
        self.port = None
        self.timeout = None

    def configure(self, args):
        try:
            self.logger.info("BrokerMW: configure")
            self.timeout = args.timeout * 1000
            self.port = args.port
            self.addr = args.addr

            context = zmq.Context()
            self.poller = zmq.Poller()

            self.req = context.socket(zmq.REQ)
            self.poller.register(self.req, zmq.POLLIN)
            self.req.connect("tcp://" + args.discovery)

            self.sub = context.socket(zmq.SUB)
            self.poller.register(self.sub, zmq.POLLIN)

            self.pub = context.socket(zmq.PUB)
            self.pub.bind("tcp://*:" + str(self.port))

            self.logger.info("BrokerMW: configure: completed")
        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def event_loop(self, timeout=None):
        try:
            self.logger.info("BrokerMW: event_loop")
            while self.handle_events:
                events = dict(self.poller.poll(timeout))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()

                elif self.req in events:
                    timeout = self.handle_request()

                elif self.sub in events:
                    timeout = self.handle_sub()

                else:
                    raise Exception("BrokerMW: event_loop: unknown event")

            self.logger.info("BrokerMW: out of event loop")
        except Exception as e:
            raise e

    def handle_request(self):
        try:
            self.logger.info("BrokerMW: handle_request")
            buf = self.req.recv()
            self.logger.info("BrokerMW: handle_request: received request")

            request = discovery_pb2.DiscoveryReq()
            request.ParseFromString(buf)

            if request.msg_type == discovery_pb2.TYPE_REGISTER:
                return self.upcall_obj.handle_register(request.register_resp)

            elif request.msg_type == discovery_pb2.TYPE_ISREADY:
                return self.upcall_obj.handle_isready(request.isready_resp)

            elif request.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                return self.upcall_obj.handle_allpub_lookup(request.lookup_resp)

            else:
                raise Exception(
                    "BrokerMW: handle_request: unknown request type")

        except Exception as e:
            raise e

    def connect_to_pubs(self, pubs):
        try:
            self.logger.info("BrokerMW: connect_to_pubs")
            for pub in pubs:
                self.logger.info(
                    "BrokerMW: connect_to_pubs: connecting to " + pub)
                self.sub.connect("tcp://" + pub)
            self.sub.setsockopt(zmq.SUBSCRIBE, b"")
            self.logger.info("BrokerMW: connect_to_pubs: completed")
        except Exception as e:
            raise e

    def handle_sub(self):
        try:
            self.logger.info("BrokerMW: handle_sub")
            msg = self.sub.recv().decode("utf-8")
            self.pub.send(msg)
            self.logger.info("BrokerMW: handle_sub: forwarded msg")
            return self.timeout

        except Exception as e:
            raise e

    def register(self, name):
        try:
            self.logger.debug(
                "BrokerMW::register - populate the Registrant Info")
            reg_info = discovery_pb2.RegistrantInfo()  # allocate
            reg_info.id = name  # our id
            reg_info.addr = self.addr  # our advertised IP addr where we are publishing
            reg_info.port = self.port  # port on which we are publishing
            self.logger.debug(
                "BrokerMW::register - done populating the Registrant Info")

            # Next build a RegisterReq message
            self.logger.debug(
                "BrokerMW::register - populate the nested register req")
            register_req = discovery_pb2.RegisterReq()  # allocate
            register_req.role = discovery_pb2.ROLE_BOTH  # we are a subscriber
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            # copy contents of inner structure
            register_req.info.CopyFrom(reg_info)
            # this is how repeated entries are added (or use append() or extend ()
            register_req.topiclist[:] = []
            self.logger.debug(
                "BrokerMW::register - done populating nested RegisterReq")

            # Finally, build the outer layer DiscoveryReq Message
            self.logger.debug(
                "BrokerMW::register - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()  # allocate
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER  # set message type
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug(
                "BrokerMW::register - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug(
                "BrokerMW serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug(
                "BrokerMW::register - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send(buf2send)

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "BrokerMW::register - sent register message and now now wait for reply")

        except Exception as e:
            raise e

    def is_ready(self):
        try:
            self.logger.info("BrokerMW::is_ready")

            # we do a similar kind of serialization as we did in the register
            # message but much simpler as the message format is very simple.
            # Then send the request to the discovery service

            # The following code shows serialization using the protobuf generated code.

            # first build a IsReady message
            self.logger.debug(
                "BrokerMW::is_ready - populate the nested IsReady msg")
            isready_req = discovery_pb2.IsReadyReq()  # allocate
            # actually, there is nothing inside that msg declaration.
            self.logger.debug(
                "BrokerMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery Message
            self.logger.debug(
                "BrokerMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.isready_req.CopyFrom(isready_req)
            self.logger.debug(
                "BrokerMW::is_ready - done building the outer message")

            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString()
            self.logger.debug(
                "Stringified serialized buf = {}".format(buf2send))

            # now send this to our discovery service
            self.logger.debug(
                "BrokerMW::is_ready - send stringified buffer to Discovery service")
            # we use the "send" method of ZMQ that sends the bytes
            self.req.send(buf2send)

            # now go to our event loop to receive a response to this request
            self.logger.info(
                "BrokerMW::is_ready - request sent and now wait for reply")

        except Exception as e:
            raise e

    def allpub_lookup(self):
        try:
            self.logger.info("BrokerMW::allpub_lookup")

            request = discovery_pb2.DiscoveryReq()
            request.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS

            lookup_req = discovery_pb2.LookupPubByTopicReq()
            lookup_req.topiclist[:] = []

            request.lookup_req.CopyFrom(lookup_req)

            buf2send = request.SerializeToString()
            self.logger.debug(
                "Stringified serialized buf = {}".format(buf2send))

            self.req.send(buf2send)
        except Exception as e:
            raise e

    def disable_event_loop(self):
        self.logger.info("BrokerMW::disable_event_loop")
        self.handle_events = False
