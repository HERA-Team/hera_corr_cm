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


def get_snaps_from_redis():
    """Read SNAPs from redis - from CM, config and correlator viewpoints."""
    import yaml

    r = redis.Redis('redishost')
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
    if isinstance(redishost, str):
        redishost = redis.Redis(redishost)
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

    if not return_as.lower().startswith('dict'):
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
