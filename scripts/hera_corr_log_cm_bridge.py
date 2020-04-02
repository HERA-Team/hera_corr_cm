#!/usr/bin/env python

from __future__ import print_function, absolute_import

import argparse
import logging
import redis
import socket
import json
from hera_corr_cm.handlers import add_default_log_handlers
from hera_mc import mc
from astropy.time import Time

hostname = socket.gethostname()

allowed_levels = ["DEBUG", "INFO", "NOTIFY", "WARNING", "ERROR", "CRITICAL"]
logging.addLevelName(logging.INFO+1, "NOTIFY")

parser = argparse.ArgumentParser(description='Subscribe to the redis-based log stream',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-r', dest='redishost', type=str, default='redishost',
                    help='Host servicing redis requests')
parser.add_argument('-l', dest='level', type=str, default="NOTIFY",
                    help='Don\'t log messages below this level. '
                         'Allowed values are {vals:}'.format(vals=allowed_levels))

args = parser.parse_args()

if args.level not in allowed_levels:
    print("Selected log level not allowed. Allowed levels are:", allowed_levels)
    print("Defaulting to WARNING")
    level = logging.WARNING
else:
    level = logging.getLevelName(args.level)

logger = add_default_log_handlers(logging.getLogger(__file__))

print('Connecting to redis server {host:s}'.format(host=args.redishost))
r = redis.Redis(args.redishost)
script_redis_key = "status:script:{host:s}:{file:s}".format(host=hostname, file=__file__)
ps = r.pubsub()
ps.subscribe('log-channel')

print('Connecting to MC database')
db = mc.connect_to_mc_db(None)
session = db.sessionmaker()
subsystem = 'correlator'

print('Entering redis poll loop')
while(True):
    # Set an expiring redis key so we know if this script dies
    r.set(script_redis_key, "alive", ex=30)
    try:
        mess = ps.get_message(ignore_subscribe_messages=True, timeout=5.)
        if mess is not None:
            try:
                decoded = json.loads(mess["data"])
                msg = decoded['formatted']
                logtime = Time.now()
                msg_level = decoded['levelno']
                if msg_level >= level:
                    # Re-code level because HeraMC logs 1 as most severe, and python logging calls critical:50, debug:10
                    severity = max(1, 100 // decoded['levelno'])
                    session.add_subsystem_error(logtime, subsystem, severity, msg, testing=False)
            except:
                pass
    except KeyboardInterrupt:
        r.set(script_redis_key, "killed by KeyboardInterrupt", timeout=600)
        exit()
