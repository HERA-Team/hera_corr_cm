import argparse
import logging
import redis
from hera_corr_cm import helpers
from hera_mc import mc
from astropy.time import Time

parser = argparse.ArgumentParser(description='Subscribe to the redis-based log stream',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-r', dest='redishost', type=str, default='redishost',
                    help ='Host servicing redis requests')

args = parser.parse_args()

logger = helpers.add_default_log_handlers(logging.getLogger(__file__))

print('Connecting to redis server %s' % args.redishost)
r = redis.Redis(args.redishost)
script_redis_key = "status:script:%s" % __file__
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
            decoded = json.loads(mess["data"])
            msg = decoded['formatted']
            logtime = self.Time(time.time(), format="unix")
            # Re-code level because HeraMC logs 1 as most severe, and python logging calls critical:50, debug:10
            severity = max(1, 100 / decoded['levelno'])
            session.add_subsystem_error(logtime, subsystem, severity, msg, testing=False)
    except KeyboardInterrupt:
        r.set(script_redis_key, "killed by KeyboardInterrupt")
        exit()

