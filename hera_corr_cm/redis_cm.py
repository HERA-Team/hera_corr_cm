"""Redis functions for CM data."""

import sys
import logging
import logging.handlers
import time
import redis
import json
import socket
import numpy as np
from astropy.coordinates import EarthLocation
import astropy.units as u

# this is identical to six.string_type but we don't want six dependence
if sys.version_info.major > 2:
    string_type = str
else:
    string_type = basestring


logger = logging.getLogger(__name__)
NOTIFY = logging.INFO + 1
logging.addLevelName(NOTIFY, "NOTIFY")

IS_INITIALIZED_ATTR = "_hera_has_default_handlers"


def write_snap_hostnames_to_redis(redishost='redishost'):
    """
    Write the snap hostnames to redis.

    This allows hera_mc to map hostnames to SNAP part numbers.
    """
    if isinstance(redishost, string_type):
        connection_pool = redis.ConnectionPool(host=redishost, decode_responses=True)
        redishost = redis.Redis(connection_pool=connection_pool)
    snap_host = {}
    snap_list = list(json.loads(redishost.hget('corr:map', 'all_snap_inputs')).keys())
    for snap in snap_list:
        try:
            true_name, aliases, addresses = socket.gethostbyaddr(snap)
        except:  # noqa
            logger.error('Failed to gethostbyname for host %s' % snap)
            continue
        snap_host[snap] = aliases[-1]
    redhash = {}
    redhash['snap_host'] = json.dumps(snap_host)
    redhash['snap_host_update_time'] = time.time()
    redhash['snap_host_update_time_str'] = time.ctime(redhash['snap_host_update_time'])
    redishost.hmset('corr:map', redhash)


def hera_antpol_to_ant_pol(antpol):
    """Parse antpol."""
    antpol = antpol.lower().lstrip('h')
    pol = antpol[-1]
    ant = int(antpol[:-1])
    return ant, pol


def get_snaps_from_redis():
    """Read SNAPs from redis - from CM, config and correlator viewpoints."""
    import yaml

    connection_pool = redis.ConnectionPool(host="redishost", decode_responses=True)
    r = redis.Redis(connection_pool=connection_pool)
    snaps_cm_list = list(json.loads(r.hget('corr:map', 'all_snap_inputs')).keys())
    snap_to_host = json.loads(r.hget('corr:map', 'snap_host'))
    snaps = {'cm': [], 'cfg': [], 'corr': []}
    for snap in snaps_cm_list:
        try:
            snaps['cm'].append(snap_to_host[snap])
        except KeyError:
            continue
    snaps['cfg'] = list(yaml.safe_load(r.hget('init_configuration', 'config'))['fengines'].keys())
    snaps['corr'] = list(r.hgetall('corr:snap_ants').keys())
    return snaps


def read_maps_from_redis(redishost='redishost'):
    """Read subset of corr:map."""
    if isinstance(redishost, string_type):
        connection_pool = redis.ConnectionPool(host=redishost, decode_responses=True)
        redishost = redis.Redis(connection_pool=connection_pool)
    if not redishost.exists('corr:map'):
        return None
    x = redishost.hgetall('corr:map')
    x['update_time'] = float(x['update_time'])
    x['ant_to_snap'] = json.loads(x['ant_to_snap'])
    x['snap_to_ant'] = json.loads(x['snap_to_ant'])
    return x


def read_cminfo_from_redis(return_as, redishost='redishost'):
    """
    Read location information from redis.

    This is a redis replacement for the cminfo methods that used hera_mc.  For
    "drop-in" replacement in how it gets used in various places there are two
    different return options per the "return_as" parameter.

    Parameters
    ----------
    return_as : str
                If return_as 'dict': returns dict as per old cminfo
                If return_as 'namespace':  returns as a Namespace for other places
    redishost : str
                Name of redis host.

    Returns
    -------
    dict (or Namespace) of cminfo

    """
    if not isinstance(return_as, string_type):
        raise ValueError(
            "Input return_as must be a string type."
        )

    if return_as.lower() not in ["dict", "dictionary", "namespace"]:
        return ValueError(
            "Input return_as must be one of {}".format(["dict", "dictionary", "namespace"])
        )

    cminfo = {}

    redis_pool = redis.ConnectionPool(host=redishost, decode_responses=True)
    redishost = redis.Redis(connection_pool=redis_pool)

    cminfo_redis = redishost.hgetall("cminfo")

    for k in cminfo_redis.keys():
        cminfo[k] = json.loads(cminfo_redis[k])

    # return if dictionary tpye was desired
    if return_as.lower().startswith('dict'):
        return cminfo

    from argparse import Namespace

    loc = Namespace()
    for key, val in cminfo.items():
        setattr(loc, key, val)

    loc.observatory = EarthLocation(
        lat=loc.cofa_lat * u.degree,
        lon=loc.cofa_lon * u.degree,
        height=loc.cofa_alt * u.m
    )
    loc.antenna_numbers = np.array(loc.antenna_numbers).astype(int)
    loc.antenna_names = np.asarray([str(a) for a in loc.antenna_names])
    loc.number_of_antennas = len(loc.antenna_numbers)

    return loc
