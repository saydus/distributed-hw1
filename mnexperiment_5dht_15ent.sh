h4 python3 DiscoveryAppln.py -n disc1 -j dht3_ent4.json -p 5555 -P 2 -S 2 > disc1.out 2>&1 &
h11 python3 DiscoveryAppln.py -n disc2 -j dht3_ent4.json -p 5555 -P 2 -S 2 > disc2.out 2>&1 &
h16 python3 DiscoveryAppln.py -n disc3 -j dht3_ent4.json -p 5555 -P 2 -S 2 > disc3.out 2>&1 &
h7 python3 PublisherAppln.py -n pub2 -P 2 -S 2 -j dht3_ent4.json -a 10.0.0.7 -p 7777 -T 6 -f 1 -i 3000 > pub2.out 2>&1 &
h18 python3 PublisherAppln.py -n pub1 -P 2 -S 2 -j dht3_ent4.json -a 10.0.0.18 -p 7777 -T 6 -f 0.75 -i 1000 > pub1.out 2>&1 &
h4 python3 SubscriberAppln.py -n sub2 -P 2 -S 2 -j dht3_ent4.json -T 9 > sub2.out 2>&1 &
h17 python3 SubscriberAppln.py -n sub1 -P 2 -S 2 -j dht3_ent4.json -T 9 > sub1.out 2>&1 &
h15 python3 BrokerAppln.py -n broker1 -j dht3_ent4.json > broker1.out 2>&1 &
