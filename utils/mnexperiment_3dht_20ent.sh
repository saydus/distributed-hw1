h2 python3 DiscoveryAppln.py -n disc3 -j dht3_ent20.json -p 5555 -P 10 -S 10 > disc3.out 2>&1 &
h14 python3 DiscoveryAppln.py -n disc1 -j dht3_ent20.json -p 5555 -P 10 -S 10 > disc1.out 2>&1 &
h19 python3 DiscoveryAppln.py -n disc2 -j dht3_ent20.json -p 5555 -P 10 -S 10 > disc2.out 2>&1 &
h1 python3 PublisherAppln.py -n pub7 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.1 -p 7777 -T 8 -f 4 -i 3000 > pub7.out 2>&1 &
h5 python3 PublisherAppln.py -n pub10 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.5 -p 7777 -T 5 -f 0.25 -i 3000 > pub10.out 2>&1 &
h7 python3 PublisherAppln.py -n pub9 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.7 -p 7777 -T 9 -f 3 -i 3000 > pub9.out 2>&1 &
h8 python3 PublisherAppln.py -n pub4 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.8 -p 7777 -T 9 -f 3 -i 2000 > pub4.out 2>&1 &
h8 python3 PublisherAppln.py -n pub5 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.8 -p 7776 -T 6 -f 4 -i 3000 > pub5.out 2>&1 &
h9 python3 PublisherAppln.py -n pub8 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.9 -p 7777 -T 8 -f 2 -i 2000 > pub8.out 2>&1 &
h10 python3 PublisherAppln.py -n pub1 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.10 -p 7777 -T 6 -f 2 -i 2000 > pub1.out 2>&1 &
h10 python3 PublisherAppln.py -n pub6 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.10 -p 7776 -T 5 -f 0.75 -i 2000 > pub6.out 2>&1 &
h12 python3 PublisherAppln.py -n pub3 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.12 -p 7777 -T 7 -f 0.25 -i 1000 > pub3.out 2>&1 &
h20 python3 PublisherAppln.py -n pub2 -P 10 -S 10 -j dht3_ent20.json -a 10.0.0.20 -p 7777 -T 9 -f 0.25 -i 3000 > pub2.out 2>&1 &
h4 python3 SubscriberAppln.py -n sub10 -P 10 -S 10 -j dht3_ent20.json -T 5 > sub10.out 2>&1 &
h5 python3 SubscriberAppln.py -n sub9 -P 10 -S 10 -j dht3_ent20.json -T 8 > sub9.out 2>&1 &
h8 python3 SubscriberAppln.py -n sub8 -P 10 -S 10 -j dht3_ent20.json -T 6 > sub8.out 2>&1 &
h9 python3 SubscriberAppln.py -n sub6 -P 10 -S 10 -j dht3_ent20.json -T 8 > sub6.out 2>&1 &
h13 python3 SubscriberAppln.py -n sub3 -P 10 -S 10 -j dht3_ent20.json -T 6 > sub3.out 2>&1 &
h14 python3 SubscriberAppln.py -n sub7 -P 10 -S 10 -j dht3_ent20.json -T 9 > sub7.out 2>&1 &
h16 python3 SubscriberAppln.py -n sub5 -P 10 -S 10 -j dht3_ent20.json -T 9 > sub5.out 2>&1 &
h18 python3 SubscriberAppln.py -n sub4 -P 10 -S 10 -j dht3_ent20.json -T 7 > sub4.out 2>&1 &
h20 python3 SubscriberAppln.py -n sub1 -P 10 -S 10 -j dht3_ent20.json -T 8 > sub1.out 2>&1 &
h20 python3 SubscriberAppln.py -n sub2 -P 10 -S 10 -j dht3_ent20.json -T 9 > sub2.out 2>&1 &
