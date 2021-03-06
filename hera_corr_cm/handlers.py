from __future__ import print_function

import logging
import logging.handlers
import sys
import redis
import json
import socket

logger = logging.getLogger(__name__)
NOTIFY = logging.INFO + 1
logging.addLevelName(NOTIFY, "NOTIFY")

IS_INITIALIZED_ATTR = "_hera_has_default_handlers"


class RedisHandler(logging.Handler):
    """
    Redis-based log handler.

    http://charlesleifer.com/blog/using-redis-pub-sub-and-irc-for-error-logging-with-python/
    """

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
    def __init__(self, subsystem, channel, conn, *args, **kwargs):
        from astropy.time import Time
        self.Time = Time
        logging.Handler.__init__(self, *args, **kwargs)
        self.subsystem = subsystem
        self.channel = channel
        self.redis_conn = conn

    def emit(self, record):
        # Re-code level because HeraMC logs 1 as most severe, and python logging
        # calls critical:50, debug:10
        severity = max(1, 100 // record.levelno)
        record_dict = {
            "subsystem": self.subsystem,
            "levelno": record.levelno,
            "severity": severity,
            "message": self.format(record),
            "logtime": self.Time.now().unix,
        }
        self.redis_conn.publish(self.channel, json.dumps(record_dict))


def log_notify(log, message=None):
    """Nofify upon start."""
    msg = (message
           or "{logname} starting on {host}".format(logname=log.name,
                                                    host=socket.gethostname()
                                                    )
           )
    log.log(NOTIFY, msg)


def add_default_log_handlers(logger, redishostname='redishost', fglevel=logging.INFO,
                             bglevel=NOTIFY, include_mc=True, mc_level=logging.WARNING):
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

    redis_host = redis.StrictRedis(redishostname, socket_timeout=1, decode_responses=True)
    try:
        redis_host.ping()
    except redis.ConnectionError:
        logger.warn("Couldn't connect to redis server at {}".format(redishostname))
        return logger

    redis_handler = RedisHandler('log-channel', redis_host)
    redis_handler.setLevel(bglevel)
    redis_handler.setFormatter(formatter)
    logger.addHandler(redis_handler)

    if include_mc:
        mc_handler = HeraMCHandler("correlator", "mc-log-channel", redis_host)
        mc_handler.setLevel(mc_level)
        mc_handler.setFormatter(formatter)
        logger.addHandler(mc_handler)

    return logger
