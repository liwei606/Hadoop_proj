#!/usr/bin/env python

import sys
import os

def emit(s, p, d):
    if s == 0:
	missing = open('/home/hadoop/missing', 'w')
	missing.write(str(p) + '\n')
        missing.close()
        # os.system("scp /home/hadoop/missing slave1:~/missing")
        # os.system("scp /home/hadoop/missing slave2:~/missing")
        os.system("scp /home/hadoop/missing master:~/missing")
    else:
	print '%s\t%s:%s' % (s, str(p), d)


current_node = None
current_p = 0.0
dests = ''

for line in sys.stdin:
    line = line.strip()
    tab_index = line.index('\t')
    node = int(line[:tab_index])
    if current_node != node:
	if current_node != None:
	    emit(current_node, current_p, dests)
	    current_p = 0.0
	    dests = ''
        current_node = node
    if line[tab_index + 1] == ':':
	dests = line[tab_index + 2:]
    else:
        current_p += float(line[tab_index + 1:])

if current_node == node:
    emit(current_node, current_p, dests)
