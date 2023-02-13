rm -fr *.out  # remove any existing out files
# now run each instance. The > redirects standard output to the
# named file after it, and then the 2> redirects the standard error
# to the arg named after it, which happens to the same file name
# as where we redirected the standard output. The final & is to
# push the command into the background and let it run.
#
# You can use such a logic and additional params to create
# all kinds of experimental scenarios.
python3 DiscoveryAppln.py -P 1 -S 1 > discovery.out 2>&1 &
python3 PublisherAppln.py -T 9 -n pub1 -p 5570 > pub1.out 2>&1 &
python3 SubscriberAppln.py -T 5 -n sub1 > sub1.out 2>&1 