"""
This program is to create a DHT ring using the hash values of the data
it helps us to verify the correctness of our main program implemenation
and the values in the node register
Author: Jay Barot
Date: 03/01/2022
"""

import argparse
import hashlib
import json
import logging
import pprint


class AbstractDhtRingBuilder:

    def __init__(self):
        self.bits = None
        self.data = None
        self.dht_database = None

    def config(self, args):
        self.bits = args.bits_hash
        self.data = args.data
        # read the dht.json file to get the hash values of the nodes
        # generated by the experiment generator
        self.dht_database = json.load(open("../dht_15.json"))

    """
    This function calculates the hash value for the given id
    :param bits_hash: number of bits to be used for the hash value
    :ptype bits_hash: <int>
    :param id: id for which the hash value is to be calculated
    :ptype id: <str>
    :return: hash value
    """

    def hash_func(self, bits_hash, id):
        # first get the digest from hashlib and then take the desired number of bytes from the
        # lower end of the 256 bits hash. Big or little endian does not matter.
        # this is how we get the digest or hash value
        hash_digest = hashlib.sha256(bytes(id, "utf-8")).digest()
        # figure out how many bytes to retrieve
        # otherwise we get float which we cannot use below
        num_bytes = int(bits_hash/8)
        # take lower N number of bytes
        hash_val = int.from_bytes(hash_digest[:num_bytes], "big")
        return hash_val

    """
    This function sorts the dictionary by value
    :param dict: dictionary to be sorted
    :ptype dict: <dict>
    :return: sorted dictionary
    """

    def sort_dict(self, dict):
        sorted_dict = sorted(dict.items(), key=lambda x: x[1])
        return sorted_dict

    def driver(self):
        # empty dictionary to store the id and hash
        result = {}

        # read the dht.json file and store the id and hash in a dictionary
        for item in self.dht_database["dht"]:
            result[item["id"]] = item["hash"]

        # calculate the hash for the data and store it in a dictionary
        for item in self.data:
            result[item] = self.hash_func(self.bits, item)

        # sort the dictionary by value
        # print this sorted dictionary representing DHT ring nodes in ascending order
        pprint.pprint(self.sort_dict(result))


def parseCmdLineArgs():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(
        description="Visualize DHT Ring Application")

    # dht.json (10 dht's)
#   parser.add_argument ("-d", "--data", type=list, default=["weather", "humidity", "airquality","light", "pressure", "temperature", "sound", "altitude", "location", "sub1", "sub2", "sub3", "sub4", "sub5", "pub1:10.0.0.15:7777", "pub2:10.0.0.11:7777", "pub3:10.0.0.16:7777", "pub4:10.0.0.9:7777", "pub5:10.0.0.19:7777"])

    # dht_5.json
#   parser.add_argument ("-d", "--data", type=list, default=["weather", "humidity", "airquality","light", "pressure", "temperature", "sound", "altitude", "location", "sub1", "sub2", "sub3", "sub4", "sub5", "pub1:10.0.0.10:7777", "pub2:10.0.0.13:7777", "pub3:10.0.0.2:7777", "pub4:10.0.0.10:7776", "pub5:10.0.0.2:7776"])

    # dht_15.json
    parser.add_argument("-d", "--data", type=list, default=["weather", "humidity", "airquality", "light", "pressure", "temperature", "sound", "altitude", "location",
                        "sub1", "sub2", "sub3", "sub4", "sub5", "pub1:10.0.0.2:7777", "pub2:10.0.0.14:7777", "pub3:10.0.0.14:7776", "pub4:10.0.0.11:7777", "pub5:10.0.0.10:7777"])

    parser.add_argument("-b", "--bits_hash", type=int, choices=[8, 16, 24, 32, 40, 48, 56, 64], default=48,
                        help="Number of bits of hash value to test for collision: allowable values between 6 and 64 in increments of 8 bytes, default 48")

    return parser.parse_args()


def main():
    # first parse the arguments
    args = parseCmdLineArgs()

    # Obtain a publisher application
    ring = AbstractDhtRingBuilder()

    ring.config(args)
    ring.driver()


if __name__ == "__main__":
    main()
