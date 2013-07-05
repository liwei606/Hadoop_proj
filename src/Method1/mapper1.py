#!/usr/bin/env python

import sys
from collections import defaultdict

combiner = defaultdict(float)

for line in sys.stdin:
    line = line.strip()
    space_index = line.index(' ')
    colon_index = line.index(':')
    source = line[:space_index]
    pr = line[space_index + 1:colon_index]
    dests = line[colon_index + 1:].split(' ')
    if dests[0]:
        p = float(pr) / len(dests)
	for dest in dests:
	    combiner[int(dest)] += p
    else:
	combiner[0] += float(pr)
    print source + '\t:' + ' '.join(dests)

for key, value in combiner.iteritems():
    print str(key) + '\t' + str(value)
