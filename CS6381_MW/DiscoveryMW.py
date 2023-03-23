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
import json
import hashlib
import uuid

from CS6381_MW import discovery_pb2


class FingerTableEntity():
    def __init__(self, hash, data):
        self.hash = hash
        self.data = data
        self.dealer = None


class DiscoveryMW():
    def __init__(self, logger, lookup):
        self.logger = logger
        self.rep = None
        self.poller = None
        self.upcall_obj = None
        self.handle_events = True

        self.port = self.finger_table = []
        self.json_path = None
        self.dht_hash = None
        self.router = None
        self.lookup = lookup

    def configure(self, args):
        try:
            self.logger.info("DiscoveryMW: configure")
            context = zmq.Context()

            self.logger.info("DiscoveryMW: configure: create REP socket")
            self.poller = zmq.Poller()

            self.port = args.port
            self.name = args.name
            self.json_path = args.json_path_dht
            self.router = context.socket(zmq.ROUTER)

            self.poller.register(self.router, zmq.POLLIN)
            self.router.bind(f"tcp://*:{self.port}")

            self.logger.info("DiscoveryMW: configure: completed")

            if (self.lookup == "DHT"):
                self.setup_finger_table()

                for finger in self.finger_table:
                    self.logger.info(
                        f"DiscoveryMW: configure table: {finger.hash} {finger.data}")

                    finger.dealer = context.socket(zmq.DEALER)
                    finger.dealer.connect(
                        f"tcp://{finger.data['IP']}:{finger.data['port']}")
                    finger.dealer.setsockopt(
                        zmq.IDENTITY, bytes(uuid.uuid4().hex, 'utf-8'))
                    self.poller.register(finger.dealer, zmq.POLLIN)

        except Exception as e:
            raise e

    def setup_finger_table(self):
        # read the json path
        with open(self.json_path) as json_file:
            data = json.load(json_file)

            self.dht_hash = next(
                (n['hash'] for n in data['dht'] if n['id'] == self.name), None)
            data["dht"] = sorted(data["dht"], key=lambda x: x["hash"])

            address_space = (2 ** 48)
            for i in range(0, 48):
                new_hash = (self.dht_hash + (2 ** i)) % address_space

                # go over all dhts and find the smallest entry that is largest or equal to new hash
                successor = {
                    'id': '',
                    'hash': address_space,
                    'IP': '',
                    'port': 0,
                    'host': ''
                }

                for dht_node in data['dht']:
                    if (successor['hash'] > dht_node['hash'] and dht_node['hash'] >= new_hash):
                        successor = dht_node

                if (successor['hash'] == address_space):
                    successor = data['dht'][0]

                self.finger_table.append(
                    FingerTableEntity(successor['hash'], successor))

        return None

    def compute_entity_hash(self, register_req):
        unique_string = register_req.info.id
        if register_req.role != discovery_pb2.ROLE_SUBSCRIBER:
            unique_string += f":{register_req.info.addr}:{register_req.info.port}"

        # Compute the hash value
        hash_digest = hashlib.sha256(bytes(unique_string, "utf-8")).digest()
        num_bytes = 48 // 8
        hash_val = int.from_bytes(hash_digest[:num_bytes], "big")

        self.logger.info(
            f"compute_entity_hash: string {unique_string} hash {hash_val}")
        return hash_val

    def find_next_node(self, hash):
        self.logger.debug(
            f"This node's hash: {self.dht_hash}, {type(self.dht_hash)}")
        self.logger.debug(f"Searching Hash: {hash}, {type(hash)}")

        successor_in_finger_table = self.finger_table[0]
        is_responsible = False

        if (hash > self.dht_hash and hash <= successor_in_finger_table.hash) or (successor_in_finger_table.hash < self.dht_hash and (hash > self.dht_hash or hash < successor_in_finger_table.hash)):
            self.logger.debug(f"find_next_node: found it")
            is_responsible = True
        else:
            self.logger.debug(f"find_next_node: get another one")
            for entry in reversed(self.finger_table):
                if (self.dht_hash >= hash):
                    if (entry.hash > self.dht_hash or entry.hash < hash):
                        successor_in_finger_table = entry
                        break
                else:
                    if (entry.hash > self.dht_hash and entry.hash < hash):
                        successor_in_finger_table = entry
                        break

        return successor_in_finger_table, is_responsible

    def get_register_req_to_next_node(self, request, read_or_write):
        registrant_info = discovery_pb2.RegistrantInfo()
        registrant_info.id = request.register_req.info.id
        registrant_info.port = request.register_req.info.port
        registrant_info.addr = request.register_req.info.addr

        register_req = discovery_pb2.RegisterReq()
        register_req.info.CopyFrom(registrant_info)
        register_req.role = request.register_req.role
        register_req.topiclist[:] = request.register_req.topiclist

        final_request = discovery_pb2.DiscoveryReq()
        final_request.msg_type = discovery_pb2.TYPE_REGISTER
        final_request.register_req.CopyFrom(register_req)
        final_request.read_or_write = read_or_write
        final_request.timestamp = request.timestamp

        return final_request

    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW: handle_request")
            frames = self.router.recv_multipart()
            bytes = frames[-1]

            self.logger.info("DiscoveryMW: handle_request: received request")

            request = discovery_pb2.DiscoveryReq()
            request.ParseFromString(bytes)
            timestamp = request.timestamp

            if request.msg_type == discovery_pb2.TYPE_REGISTER:
                if self.upcall_obj.lookup != "DHT":
                    self.logger.info("DiscoveryMW: handle_request: register")
                    return self.upcall_obj.handle_register(request.register_req, frames, timestamp)

                if (request.read_or_write):
                    self.logger.info(
                        "DiscoveryMW::handle_request – handle register because this node is responsible for the hash")
                    return self.upcall_obj.handle_register(request.register_req, frames, timestamp)
                else:
                    # Compute the next node to forward request to
                    entity_hash = self.compute_entity_hash(
                        request.register_req)

                    # Find what node to forward the request to based on the hash
                    node, found_the_one = self.find_next_node(entity_hash)

                    # Construct the message
                    new_disc_req = self.get_register_req_to_next_node(
                        request, found_the_one)
                    buf2send = new_disc_req.SerializeToString()

                    self.logger.info(
                        f"DiscoveryMW::handle_request – Forward request (node {self.upcall_obj.name}) to node: {node.data['id']}, read_write={new_disc_req.read_or_write}")

                    frames[-1] = buf2send

                    # Send the message to the next node
                    node.dealer.send_multipart(frames)
                    return None
            elif request.msg_type == discovery_pb2.TYPE_ISREADY:
                self.logger.info("DiscoveryMW: handle_request: isready")
                return self.upcall_obj.handle_isready(request.isready_req, frames, timestamp)

            elif request.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                self.logger.info("DiscoveryMW: handle_request: lookup")
                return self.upcall_obj.handle_lookup_topic(request.lookup_req, frames, timestamp)

            elif request.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                self.logger.info(
                    "DiscoveryMW: handle_request: lookup all pubs")
                return self.upcall_obj.handle_lookup_all_publishers(request.lookup_req, frames, timestamp)

            else:
                self.logger.info(
                    "DiscoveryMW: handle_request: unknown request")
                raise ValueError("Unknown request")
        except Exception as e:
            raise e

    def send_lookup(self, sockets, frames, timestamp, isAll=False):
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

            disc_resp.timestamp = timestamp
            disc_resp.lookup_resp.CopyFrom(lookup_resp)
            buf2send = disc_resp.SerializeToString()

            self.logger.info("DiscoveryMW: send_lookup: sending response")
            frames[-1] = buf2send
            self.router.send_multipart(frames)
        except Exception as e:
            raise e

    def send_lookup_resp(self, sockets, frames, timestamp):
        self.send_lookup(sockets, frames, timestamp)

    def send_lookup_all_resp(self, sockets, frames, timestamp):
        self.send_lookup(sockets, frames, timestamp, True)

    def event_loop(self, timeout=None):
        try:
            self.logger.info("DiscoveryMW: event_loop")
            while self.handle_events:
                self.logger.info("DiscoveryMW: event_loop: waiting for events")
                events = dict(self.poller.poll(timeout=timeout))
                request_handled = False

                if not events:
                    # stop the application in case of a timeout
                    self.upcall_obj.handle_events = False
                    request_handled = True

                if not request_handled and self.router in events:
                    self.logger.info(
                        "DiscoveryMW: event_loop: received event in router")
                    timeout = self.handle_request()
                    request_handled = True

                if self.lookup == 'DHT' and not request_handled:
                    for finger in self.finger_table:
                        if finger.dealer in events:
                            self.logger.info(
                                "DiscoveryMW: event_loop: received event in dealer")
                            msg = finger.dealer.recv_multipart()
                            self.router.send_multipart(msg)
                            request_handled = True
                            break
                self.logger.info(
                    "DiscoveryMW: event_loop: request_handled: %s", request_handled)
        except Exception as e:
            raise e

    def send_isready(self, is_ready, frames, timestamp):
        try:
            self.logger.info("DiscoveryMW: send_isready")
            isready_resp = discovery_pb2.IsReadyResp()
            isready_resp.status = is_ready

            response = discovery_pb2.DiscoveryResp()
            response.isready_resp.CopyFrom(isready_resp)
            response.msg_type = discovery_pb2.TYPE_ISREADY
            response.timestamp = timestamp

            buf2send = response.SerializeToString()
            self.logger.info("DiscoveryMW: send_isready: sending response")

            frames[-1] = buf2send
            self.router.send_multipart(frames)

        except Exception as e:
            raise e

    def send_register_resp(self, status, reason, frames, timestamp):
        try:
            self.logger.info("DiscoveryMW: send_register_resp")
            register_resp = discovery_pb2.RegisterResp()
            register_resp.status = discovery_pb2.STATUS_SUCCESS if status else discovery_pb2.STATUS_FAILURE
            register_resp.reason = reason

            response = discovery_pb2.DiscoveryResp()
            response.register_resp.CopyFrom(register_resp)
            response.msg_type = discovery_pb2.TYPE_REGISTER
            response.timestamp = timestamp

            buf2send = response.SerializeToString()
            self.logger.info(
                "DiscoveryMW: send_register_resp: sending response = {}".format(buf2send))

            frames[-1] = buf2send
            self.router.send_multipart(frames)
        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    def send_isready_request_to_next_node(self, visited_nodes, registered_pubs, registered_subs, registered_brokers, frames, timestamp):
        dht_is_ready = discovery_pb2.DhtIsReady()
        dht_is_ready.visited_nodes[:] = list(visited_nodes)
        dht_is_ready.pubs[:] = list(registered_pubs)
        dht_is_ready.subs[:] = list(registered_subs)
        dht_is_ready.brokers[:] = list(registered_brokers)

        isready_req = discovery_pb2.IsReadyReq()
        isready_req.dht_is_ready.CopyFrom(dht_is_ready)

        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.isready_req.CopyFrom(isready_req)
        disc_req.msg_type = discovery_pb2.TYPE_ISREADY
        disc_req.timestamp = timestamp

        buf2send = disc_req.SerializeToString()

        frames[-1] = buf2send
        self.finger_table[0].dealer.send_multipart(frames)
        return None

    def send_lookup_request_to_next_node(self, topiclist, send_all, visited_nodes, added_sockets, frames, timestamp):
        self.logger.info("DiscoveryMW: send_lookup_request_to_next_node")
        lookup_req = discovery_pb2.LookupPubByTopicReq()
        lookup_req.visited_nodes[:] = list(visited_nodes)
        lookup_req.sockets_for_connection[:] = list(added_sockets)
        lookup_req.topiclist[:] = topiclist

        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC if not send_all else discovery_pb2.TYPE_LOOKUP_ALL_PUBS
        disc_req.lookup_req.CopyFrom(lookup_req)
        disc_req.timestamp = timestamp

        buf2send = disc_req.SerializeToString()
        frames[-1] = buf2send
        self.finger_table[0].dealer.send_multipart(frames)
        return None
