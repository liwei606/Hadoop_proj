#!/usr/bin/env python

import sys

total = 5716808
beta = 0.95

m = float(open('/home/hadoop/missing', 'r').readline())
new_pr = ((1 - beta) + beta * m) / total
for line in sys.stdin:
    line = line.strip()
    tab_index = line.index('\t')
    colon_index = line.index(':')
    source = line[:tab_index]
    pr = float(line[tab_index + 1:colon_index])
    pr = beta * pr + new_pr
    dests = line[colon_index + 1:]
    print source + ' ' + str(pr) + ':' + dests
