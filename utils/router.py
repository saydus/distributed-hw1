
import zmq

# Create a ZeroMQ context
context = zmq.Context()

# Initialize sockets
router_sock1 = context.socket(zmq.ROUTER)
router_sock2 = context.socket(zmq.ROUTER)
req_sock = context.socket(zmq.REQ)
dealer_sock = context.socket(zmq.DEALER)

# Establish socket connections
router_sock1.bind("tcp://:5555")
router_sock2.bind("tcp://:5556")
req_sock.connect("tcp://localhost:5555")
dealer_sock.connect("tcp://localhost:5556")


# Configure poller
poller = zmq.Poller()
poller.register(req_sock, zmq.POLLIN)
poller.register(router_sock1, zmq.POLLIN)
poller.register(dealer_sock, zmq.POLLIN)
poller.register(router_sock2, zmq.POLLIN)

# Send initial message
msg = [b"Initial Hello"]
print(f"REQ: {msg}\n")
req_sock.send_multipart(msg)

# Loop to handle messages
while True:
    # Wait for socket events
    socket_events = dict(poller.poll())

    # Process REQ socket input
    if req_sock in socket_events:
        # Receive and print message
        msg = req_sock.recv_multipart()
        print(f"\nREQ received: {msg}")
        break

    # Process first ROUTER socket input
    if router_sock1 in socket_events:
        # Pass message to DEALER socket
        msg = router_sock1.recv_multipart()
        print(f"ROUTER1 -> DEALER1 -> ROUTER2: {msg}")
        dealer_sock.send_multipart(msg)

    # Process DEALER socket input
    if dealer_sock in socket_events:
        # Pass message to REQ socket
        msg = dealer_sock.recv_multipart()
        print(f"DEALER -> ROUTER1 -> REQ: {msg}")
        router_sock1.send_multipart(msg)

    # Process second ROUTER socket input
    if router_sock2 in socket_events:
        # Get message from second ROUTER socket
        msg = router_sock2.recv_multipart()

        # Append "World" to message
        msg[-1] += b" World"

        # Send modified message
        print(f"ROUTER2 -> DEALER1: {msg}")
        router_sock2.send_multipart(msg)
