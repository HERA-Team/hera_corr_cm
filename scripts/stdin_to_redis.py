#!/usr/bin/env python
def main():
    import sys
    import argparse
    import redis
    import json
    import logging
    import socket

    parser = argparse.ArgumentParser(description='Echo stdin to a redis pubsub channel',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-r', dest='redishost', type=str, default='redishost',
                        help ='Hostname of redis server')
    parser.add_argument('-l', dest='level', type=str, default="WARNING",
                        help ='level')
    parser.add_argument('-c', dest='channel', type=str, default="log-channel",
                        help ='Channel to which stdin should be published.')
    args = parser.parse_args()

    r = redis.Redis(args.redishost)

    # Get this machines hostname to prepend to the log message
    myhost = socket.gethostname()

    allowed_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    if args.level not in allowed_levels:
        print("Selected log level not allowed. Allowed levels are:", allowed_levels)
        print("Defaulting to WARNING")
        level = logging.WARNING
    else:
        level = getattr(logging, args.level)

    while True:
        line = sys.stdin.readline()
        if line:
            # To conform with the python logging redis handler,
            # we should send logs as a json string, containing
            # a dictionary with at least the entries
            # "formatted" : The log message string
            # "levelno"   : The python logging spec log level
            mesg = json.dumps({"formatted":"%s:%s: %s" % (args.level, myhost, line), "levelno": level})
            r.publish(args.channel, mesg)

if __name__ == "__main__":
    main()
