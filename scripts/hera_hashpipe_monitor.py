#!/usr/bin/env python

from influxdb import InfluxDBClient
import time
import argparse
from hera_corr_cm import HeraCorrCM
import socket
hostname = socket.gethostname()

script_redis_key = "status:script:{host:s}:{file:s}".format(host=hostname, file=__file__)

# Set an expiring redis key so we know if this script dies


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retrieve hashpipe stats from redis and add to influxdb.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--db_hose",
        dest="DB_HOST",
        type=str,
        default="qmaster",
        help="Name of host running the influx database."
    )
    parser.add_argument(
        "--db_port",
        dest="DB_PORT",
        type=int,
        default=8086,
        help="Port where influxdb is accepting connections."
    )
    parser.add_argument(
        "--db_name",
        dest="DB_NAME",
        type=str,
        default="correlator_monitor",
        help="Name of influx database to connect to."
    )
    parser.add_argumen(
        "--poll_time",
        dest="POLL_TIME",
        type=int,
        default=1,
        help="The polling rate of the monitor in seconds."
    )
    args = parser.parse_args()

    corr_cm = HeraCorrCM()
    db = InfluxDBClient(args.DB_HOST, args.DB_PORT, database=args.DB_NAME)
    while True:
        hashpipe_stats = corr_cm.get_hashpipe_status()
        for key in hashpipe_stats:
            db.write_points(hashpipe_stats[key], batch_size=32)

        # Let redis know this script is working as expected
        corr_cm.r.set(script_redis_key, "alive", ex=30)
        time.sleep(args.POLL_TIME)
