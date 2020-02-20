"""Redis functions for CM data."""

import logging
import logging.handlers
import time
import redis
import json
import socket
import numpy as np
from astropy.coordinates import EarthLocation
import astropy.units as u

logger = logging.getLogger(__name__)
NOTIFY = logging.INFO + 1
logging.addLevelName(NOTIFY, "NOTIFY")

IS_INITIALIZED_ATTR = "_hera_has_default_handlers"


def write_snap_hostnames_to_redis(redishost='redishost'):
    """
    Write the snap hostnames to redis.

    This allows hera_mc to map hostnames to SNAP part numbers.
    """
    if isinstance(redishost, str):
        redishost = redis.Redis(redishost)
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


def read_maps_from_redis(redishost='redishost'):
    """Read subset of corr:map."""
    if isinstance(redishost, str):
        redishost = redis.Redis(redishost)
    if not redishost.exists('corr:map'):
        return None
    x = redishost.hgetall('corr:map')
    x['update_time'] = float(x['update_time'])
    x['ant_to_snap'] = json.loads(x['ant_to_snap'])
    x['snap_to_ant'] = json.loads(x['snap_to_ant'])
    return x


def read_cminfo_from_redis(redishost='redishost', return_namespace=False):
    """
    Read location information from redis.

    Parameters
    ----------
    redishost : str
                Name of redis host.
    return_namespace : bool
                If True, will rewrite dict to Namespace, converting/adding a few things.

    Returns
    -------
    dict (or Namespace) of cminfo
    """
    cminfo = {'antenna_numbers': None, 'antenna_names': None,               # Make identical to cminfo  # noqa
              'antenna_positions': None, 'antenna_alts': None,              # returned by hera_mc
              'antenna_utm_eastings': None, 'antenna_utm_northings': None,  # These are same keys
              'correlator_inputs': None, 'cm_version': None}
    redishost = redis.Redis('redishost')
    for k in cminfo.keys():
        cminfo[k] = json.loads(redishost.hget('corr:map', k))
    cofa_info = json.loads(redishost.hget('corr:map', 'cofa'))
    cminfo['cofa_lat'] = cofa_info['lat']
    cminfo['cofa_lon'] = cofa_info['lon']
    cminfo['cofa_alt'] = cofa_info['alt']
    cofa_xyz = json.loads(redishost.hget('corr:map', 'cofa_xyz'))
    cminfo['cofa_X'] = cofa_xyz[0]
    cminfo['cofa_Y'] = cofa_xyz[1]
    cminfo['cofa_Z'] = cofa_xyz[2]

    if not return_namespace:
        return cminfo

    from argparse import Namespace
    loc = Namespace()
    for key, val in cminfo.items():
        setattr(loc, key, val)
    loc.COFA_telescope_location = [cofa_xyz['X'], cofa_xyz['Y'], cofa_xyz['Z']]
    loc.cofa_info = cofa_info
    loc.observatory = EarthLocation(lat=loc.cofa_info['lat']*u.degree,
                                    lon=loc.cofa_info['lon']*u.degree,
                                    height=loc.cofa_info['alt']*u.m)
    loc.antenna_numbers = np.array(loc.antenna_numbers).astype(int)
    loc.antenna_names = np.asarray([str(a) for a in loc.antenna_names])
    loc.number_of_antennas = len(loc.antenna_numbers)
    # Compute uvw for baselines
    loc.all_antennas_enu = {}
    for i, antnum in enumerate(loc.antenna_numbers):
        ant_enu = (cminfo['antenna_utm_eastings'][i],
                   cminfo['antenna_utm_northings'][i],
                   cminfo['antenna_alts'][i])
        loc.all_antennas_enu[antnum] = np.array(ant_enu)
    return loc
