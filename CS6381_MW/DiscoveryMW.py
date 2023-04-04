###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets

from CS6381_MW import discovery_pb2
import json


class DiscoveryMW():
    def __init__(self, logger):
        self.logger = logger
        self.rep = None
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True

        self.pub_socket = None
        self.pub_port = None
        self.sub_socket = None

    def configure(self, args):
        try:
            self.logger.info("DiscoveryMW: configure")
            context = zmq.Context()

            self.logger.info("DiscoveryMW: configure: create REP socket")
            self.poller = zmq.Poller()

            self.rep = context.socket(zmq.REP)
            self.poller.register(self.rep, zmq.POLLIN)

            self.pub_port = args.sub_port
            self.sync_pub_socket = context.socket(zmq.PUB)
            bind_string = "tcp://*:" + str(self.pub_port)
            self.sync_pub_socket.bind(bind_string)

            self.sync_sub_socket = context.socket(zmq.SUB)

            self.rep.bind("tcp://*:5555")
            self.logger.info("DiscoveryMW: configure: completed")
        except Exception as e:
            raise e

    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW: handle_request")
            buf = self.rep.recv()
            self.logger.info("DiscoveryMW: handle_request: received request")

            request = discovery_pb2.DiscoveryReq()
            request.ParseFromString(buf)

            if request.msg_type == discovery_pb2.TYPE_REGISTER:
                self.logger.info("DiscoveryMW: handle_request: register")
                return self.upcall_obj.handle_register(request.register_req)

            elif request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info("DiscoveryMW: handle_request: lookup")
                return self.upcall_obj.handle_lookup_topic(request.lookup_req)

            elif request.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                self.logger.info(
                    "DiscoveryMW: handle_request: lookup all pubs")
                return self.upcall_obj.handle_lookup_all_publishers(request.lookup_req)

            else:
                self.logger.info(
                    "DiscoveryMW: handle_request: unknown request")
                raise ValueError("Unknown request")
        except Exception as e:
            raise e

    def handle_state_sync(self):
        self.logger.info("handle_state_sync: state sync started")

        bytesRcvd = self.sync_sub_socket.recv().decode('utf-8')
        string_rcvd = bytesRcvd[bytesRcvd.find(":") + 1:]
        data_dict = json.loads(string_rcvd)

        self.upcall_obj.update_zookeeper_state(data_dict)
        return None

    def send_lookup(self, sockets, isAll=False):
        self.logger.info("DiscoveryMW::send_lookup")
        try:
            lookup_resp = discovery_pb2.LookupPubByTopicResp()
            self.logger.info("DiscoveryMW::before publist")
            self.logger.info("DiscoveryMW::sockets: %s", sockets)
            lookup_resp.publist[:] = sockets

            self.logger.info("DiscoveryMW::after publist")

            disc_resp = discovery_pb2.DiscoveryResp()
            if isAll:
                disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
            else:
                disc_resp.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC

            self.logger.info("DiscoveryMW::after assignment of DiscoveryResp")

            disc_resp.lookup_resp.CopyFrom(lookup_resp)
            buf2send = disc_resp.SerializeToString()

            self.logger.info("DiscoveryMW: send_lookup: sending response")
            self.rep.send(buf2send)
        except Exception as e:
            raise e

    def send_lookup_resp(self, sockets):
        self.send_lookup(sockets)

    def send_lookup_all_resp(self, sockets):
        self.send_lookup(sockets, True)

    def event_loop(self, timeout=None):
        try:
            self.logger.info("DiscoveryMW: event_loop")
            while self.handle_events:
                self.logger.info("DiscoveryMW: event_loop: waiting for events")
                events = dict(self.poller.poll(timeout=timeout))
                if self.rep in events:
                    self.logger.info("DiscoveryMW: event_loop: received event")
                    timeout = self.handle_request()

                if self.sync_sub_socket in events:
                    self.logger.info(
                        "DiscoveryMW: event_loop: received sync event")
                    timeout = self.handle_state_sync()

        except Exception as e:
            raise e

    def send_register_resp(self, status, reason=""):
        try:
            self.logger.info("DiscoveryMW: send_register_resp")
            register_resp = discovery_pb2.RegisterResp()
            register_resp.status = discovery_pb2.STATUS_SUCCESS if status else discovery_pb2.STATUS_FAILURE
            register_resp.reason = reason

            response = discovery_pb2.DiscoveryResp()
            response.register_resp.CopyFrom(register_resp)
            response.msg_type = discovery_pb2.TYPE_REGISTER

            buf2send = response.SerializeToString()
            self.logger.info(
                "DiscoveryMW: send_register_resp: sending response = {}".format(buf2send))

            self.rep.send(buf2send)

        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def subscribe_to_leader(self, addr, port):
        self.logger.info("DiscoveryMW: subscribe_for_updates_from_leader")
        self.sync_sub_socket.connect("tcp://" + addr + ":" + str(port))
        self.sync_sub_socket.setsockopt_string(zmq.SUBSCRIBE, "discovery")

        self.poller.register(self.sync_sub_socket, zmq.POLLIN)

    def publish_discovery_update(self, state):
        state_bytes = json.dumps(state).encode('utf-8')
        self.logger.info(f"Discovery SYNC updating the state: {str(state)}")
        self.sync_pub_socket.send(b'discovery:' + state_bytes)

    def send_unsubscribe_update(self, update_body):
        self.logger.info(f"Sending an UNSUBSCRIBE update: {str(update_body)}")
        update_as_bytes = json.dumps(update_body).encode('utf-8')
        self.sync_pub_socket.send(b'unsub:' + update_as_bytes)
        return

    def send_subscribe_update(self, update_body):
        self.logger.info(f"Sending an SUB update: {str(update_body)}")
        update_as_bytes = json.dumps(update_body).encode('utf-8')
        self.sync_pub_socket.send(b'sub:' + update_as_bytes)
        return
