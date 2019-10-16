import logging
import logging.handlers
import sys
import time
import redis
import json
import socket

'''
A Redis-based log handler from:
http://charlesleifer.com/blog/using-redis-pub-sub-and-irc-for-error-logging-with-python/
'''

logger = logging.getLogger(__name__)
NOTIFY = logging.INFO + 1
logging.addLevelName(NOTIFY, "NOTIFY")

IS_INITIALIZED_ATTR = "_hera_has_default_handlers"

class RedisHandler(logging.Handler):
    def __init__(self, channel, conn, *args, **kwargs):
        logging.Handler.__init__(self, *args, **kwargs)
        self.channel = channel
        self.redis_conn = conn

    def emit(self, record):
        attributes = [
            'name', 'msg', 'levelname', 'levelno', 'pathname', 'filename',
            'module', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
            'thread', 'threadName', 'process', 'processName',
        ]
        record_dict = dict((attr, getattr(record, attr)) for attr in attributes)
        record_dict['formatted'] = self.format(record)
        try:
            self.redis_conn.publish(self.channel, json.dumps(record_dict))
        except UnicodeDecodeError:
            self.redis_conn.publish(self.channel, 'UnicodeDecodeError on emit!')
        except redis.RedisError:
            pass

class HeraMCHandler(logging.Handler):
    def __init__(self, subsystem, *args, **kwargs):
        from hera_mc import mc
        from astropy.time import Time
        self.Time = Time
        logging.Handler.__init__(self, *args, **kwargs)
        self.subsystem = subsystem
        parser = mc.get_mc_argument_parser()
        args = parser.parse_args()
        db = mc.connect_to_mc_db(args)
        self.session = db.sessionmaker()

    def emit(self, record):
        # Re-code level because HeraMC logs 1 as most severe, and python logging calls critical:50, debug:10
        severity = max(1, 100 // record.levelno)
        message = self.format(record)
        logtime = self.Time(time.time(), format="unix")
        self.session.add_subsystem_error(logtime, self.subsystem, severity, message)



def add_default_log_handlers(logger, redishostname='redishost', fglevel=logging.INFO, bglevel=NOTIFY, include_mc=False, mc_level=logging.WARNING):
    if getattr(logger, IS_INITIALIZED_ATTR, False):
        return logger
    setattr(logger, IS_INITIALIZED_ATTR, True)

    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    formatter = logging.Formatter('%(asctime)s - %(name)20s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(fglevel)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    syslog_handler = logging.handlers.SysLogHandler(address='/dev/log')
    syslog_handler.setLevel(bglevel)
    syslog_handler.setFormatter(formatter)
    logger.addHandler(syslog_handler)

    if include_mc:
        try:
            mc_handler = HeraMCHandler("correlator")
            mc_handler.setLevel(mc_level)
            mc_handler.setFormatter(formatter)
            logger.addHandler(mc_handler)
        except:
            logger.warn("Failed to add HERA MC log handler")

    redis_host = redis.Redis(redishostname, socket_timeout=1)
    try:
        redis_host.ping()
    except redis.ConnectionError:
        logger.warn("Couldn't connect to redis server "
                    "at {host}".format(host=redishostname))
        return logger

    redis_handler = RedisHandler('log-channel', redis_host)
    redis_handler.setLevel(bglevel)
    redis_handler.setFormatter(formatter)
    logger.addHandler(redis_handler)

    return logger

def log_notify(log, message=None):
    msg = (message
           or "{logname} starting on {host}".format(logname=log.name,
                                                    host=socket.gethostname()
                                                    )
           )
    log.log(NOTIFY, msg)
