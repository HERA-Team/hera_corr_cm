#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Gather correlator status info and log them into M&C
"""
from __future__ import absolute_import, division, print_function

import sys
import os
import time
import redis
import traceback
import socket

from astropy.time import Time

from hera_mc import mc

MONITORING_INTERVAL = 60  # seconds

REDISHOST = 'redishost'

parser = mc.get_mc_argument_parser()
args = parser.parse_args()
db = mc.connect_to_mc_db(args)

hostname = socket.gethostname()
script_redis_key = 'status:script:{host:s}:{file:s}'.format(host=hostname, file=__file__)

this_daemon = os.path.basename(__file__)

daemons = [
    this_daemon,
    'hera_corr_cmd_handler.py',
    'hera_corr_log_cm_bridge.py',
    'hera_wr_redis_monitor.py',
    'hera_node_cmd_check.py',
    'hera_node_keep_alive.py',
    'hera_node_receiver.py',
    'hera_snap_redis_monitor.py',
]


while True:
    r = redis.Redis(REDISHOST)

    # Use a single session unless there's an error that isn't fixed by a rollback.
    with db.sessionmaker() as session:
        while True:
            r.set(script_redis_key, "alive", ex=MONITORING_INTERVAL*2)
            for daemon in daemons:
                state = 'errored'
                for k in r.scan_iter('status:script:*:*{daemon:s}'.format(daemon=daemon)):
                    host, daemon_path = k.split(':')[2:]
                    state = 'good'#r[k]
                try:
                    session.add_daemon_status(daemon,
                                              host, Time.now(), state)
                    session.commit()
                except Exception as e:
                    print('{t} -- error storing daemon status'.format(
                        t=time.asctime()), file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
                    traceback_str = traceback.format_exc()
                    session.rollback()
                    continue
            time.sleep(MONITORING_INTERVAL)
