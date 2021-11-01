"""HERA MC class."""
from __future__ import print_function, absolute_import

import sys
import time
import redis
import logging
import yaml
import json
import dateutil.parser
import datetime
import numpy as np
from .handlers import add_default_log_handlers
from . import __package__, __version__

# this is identical to six.string_type but we don't want six dependence
if sys.version_info.major > 2:
    string_type = str
else:
    string_type = basestring
N_CHAN = 16384
SAMPLE_RATE = 500e6

LOGGER = add_default_log_handlers(logging.getLogger(__name__))


class HeraCorrCM(object):
    """
    Encapsulate an interface to the HERA correlator.

    Enable control of SNAP boards, and X-engines via a redis message store.
    """

    # A class-wide variable to hold redis connections.
    # This prevents multiple instances of HeraCorrCM
    # from creating lots and lots (and lots) of redis connections
    redis_connections = {}

    def __init__(self, redishost="redishost", logger=LOGGER, danger_mode=False, include_fpga=False):
        """
        Create a connection to the correlator via a redis server.

        Args:
            redishost (str): The hostname of the machine
                on which the correlators redis server
                is running.
            logger (logging.Logger): A logging instance. If not provided,
                the class will instantiate its own.
            include_fpga (Boolean): If True, instantiate a connection to HERA
                F-engines.
            danger_mode (Boolean): If True, disables the
                                   only-allow-command-when-not-observing checks.
        """
        self.logger = logger
        self.danger_mode = danger_mode
        # If the redishost is one we've already connected to, use it again.
        # Otherwise, add it.
        # Also share response channels. This opens the door to all sorts of
        # unintended consequences if you try to multithread access to
        # multiple HeraCorrCM instances in the same program. But in most cases
        # sharing means the code will just do The Right Thing, and won't leave
        # a trail of a orphaned connections.
        if redishost not in list(self.redis_connections.keys()):
            self.redis_connections[redishost] = redis.Redis(redishost, max_connections=100, decode_responses=True)
        self.r = self.redis_connections[redishost]

    def is_recording(self):
        """
        Check if recording.

        Returns: recording_state, UNIX time of last state change (float)
        recording_state is True if the correlator is currently taking data.

        Note: in the case that the correlator is not running, but we don't know
              when it stopped (e.g., because it was not shutdown gracefully)
              the returned time will be `None`
        """
        if not self.r.exists("corr:is_taking_data"):
            return False, None
        else:
            x = self._hgetall("corr:is_taking_data")
            return x["state"] == "True", float(x["time"])

    def _conv_float(self, v):
        """Try and convert v into a float. If we can't, return None."""
        try:
            return float(v)
        except ValueError:
            return None

    def _conv_int(self, v):
        """Try and convert v into an int. If we can't, return None."""
        try:
            return int(v)
        except ValueError:
            return None

    def next_start_time(self):
        """
        Return next start time.

        Return the last trigger time (as a UNIX timestamp float) sent to the correlator.
        If this is in the future, the correlator is waiting to start taking data.
        If no valid timestamp exists, return 0.
        """
        if self.r.exists("corr:trig_time"):
            return float(self.r["corr:trig_time"])
        else:
            return 0.0

    def secs_to_n_spectra(self, secs):
        """Return the number of spectra in a given interval of `secs` seconds."""
        return secs / ((2.0 * N_CHAN) / SAMPLE_RATE)

    def n_spectra_to_secs(self, n):
        """Return the time interval in seconds corresponding to `n` spectra."""
        return n * ((2.0 * N_CHAN) / SAMPLE_RATE)

    def phase_switch_is_on(self):
        """
        Check phase switch state.

        Returns: enable_state, UNIX timestamp (float) of last state change
        enable_state is True if phase switching is on. Else False.
        """
        x = self._hgetall("corr:status_phase_switch")
        return x["state"] == "on", float(x["time"])

    def update_config(self, configfile):
        """
        Update the correlator configuration.

        Defines low level set up parameters such as walshing functions,
        IP addresses, band selection, etc.

        Args:
            configfile: A path to a valid correlator configuration
                yaml file.
        """
        with open(configfile, "r") as fh:
            upload_time = time.time()
            self.r.hmset("snap_configuration", {"config": fh.read(), "upload_time": upload_time,
                         "upload_time_str": time.ctime(upload_time)})

    def get_config(self):
        """
        Get the currently loaded configuration, as a processed yaml string.

        Returns: last update time (UNIX timestamp float), Configuration structure,
        configuration hash
        """
        config = self.r.hget("snap_configuration", "config")
        config_time = self.r.hget("snap_configuration", "upload_time")
        md5 = self.r.hget("snap_configuration", "md5")
        return float(config_time), yaml.load(config, Loader=yaml.FullLoader), md5

    def noise_diode_is_on(self):
        """
        Return if noise diode is on.

        Returns: enable_state, UNIX timestamp (float) of last state change
        enable_state is True if noise diode is on. Else False.
        """
        x = self._hgetall("corr:status_noise_diode")
        return x["state"] == "on", float(x["time"])

    def load_is_on(self):
        """
        Return if load is on.

        Returns: enable_state, UNIX timestamp (float) of last state change
        enable_state is True if load is on. Else False.
        """
        x = self._hgetall("corr:status_load")
        return x["state"] == "on", float(x["time"])

    def get_eq_coeffs(self, ant, pol):
        """
        Get the currently loaded gain coefficients for a given feed.

        inputs:
            ant (integer): HERA antenna number to query
            pol (string): Polarization to query (must be 'e' or 'n')
        returns:
            time (UNIX timestamp float), coefficients (numpy array of floats)
            or False, in the case of a failure
        """
        try:
            v = {key: val for key, val in self.r.hgetall('eq:ant:{ant:d}:{pol}'
                                                         .format(ant=ant, pol=pol)).items()}  # noqa
        except KeyError:
            self.logger.error("Failed to get antenna coefficients from redis. Does antenna exist?")
            return False
        try:
            t = float(v['time'])
        except:
            self.logger.error("Failed to cast EQ coefficient upload time to float")
            return False
        try:
            coeffs = np.array(json.loads(v['values']), dtype=np.float)
        except:
            self.logger.error("Failed to cast EQ coefficients to numpy float array")
            return False
        return t, coeffs

    def get_bit_stats(self):
        """
        Return bit statistics from F-engines.

        Different antennas / stats are sampled at different
        times, so each is accompanied by a timestamp.
        """
        raise NotImplementedError('There is no code here.')

    def _get_status_keys(self, stattype):
        """
        Get a list of keys which exist in redis.

        of the form status:`class`:*, and return a dictionary, keyed by these values,
        each entry of which is a redis HGETALL of this key.

        Args:
            stattype (str): Type of status keys to look for. e.g. "snap"
                         gets all "status:snap:*" keys
        Returns: dictionary of HGETALL of found keys.
            eg: {
                    {"heraNode1Snap0" : {"temp":55.3, "ip_address": "10.0.1.100", ... },
                    {"heraNode1Snap1" : {"temp":61.4, "ip_address": "10.0.1.101", ... },
                }
        """
        keystart = "status:{stat}:".format(stat=stattype)
        rv = {}
        for key in self.r.scan_iter(keystart + "*"):
            rv[key.lstrip(keystart)] = self._hgetall(key)
        return rv

    def _hgetall(self, rkey):
        """
        Generate a wrapper around self.r.hgetall(rkey).

        Converts the keys and values of the resulting byte array to a string.
        """
        return {key: val for key, val in self.r.hgetall(rkey).items()}

    def get_f_status(self):
        """
        Return a dictionary of snap status flags.

        Keys of returned dictionaries are snap hostnames. Values of this dictionary are
        status key/val pairs.

        These keys are:
            pmb_alert (bool) : True if SNAP PSU controllers have issued an alert. False otherwise.
            pps_count (int)  : Number of PPS pulses received since last programming cycle
            serial (str)     : Serial number of this SNAP board
            temp (float)     : Reported FPGA temperature (degrees C)
            uptime (int)     : Multiples of 500e6 ADC clocks since last programming cycle
            last_programmed (datetime) : Last time this FPGA was programmed
            timestamp (datetime) : Asynchronous timestamp that these status entries were gathered

            Unknown values return the string "None"
        """
        stats = self._get_status_keys("snap")
        conv_methods = {
            'pmb_alert': lambda x: bool(int(x)),
            'pps_count': int,
            'serial': str,
            'temp': float,
            'uptime': int,
            'last_programmed': dateutil.parser.parse,
            'timestamp': dateutil.parser.parse,
        }
        rv = {}
        for host, val in stats.items():
            rv[host] = {}
            for key, convfunc in conv_methods.items():
                try:
                    rv[host][key] = convfunc(stats[host][key])
                except:
                    rv[host][key] = "None"
        return rv

    def get_ant_status(self):
        """
        Return a dictionary of antenna status flags.

        Keys of returned dictionaries are of the form "<antenna number>:"<e|n>". Values of
        this dictionary are status key/val pairs.

        These keys are:
            adc_mean (float)  : Mean ADC value (in ADC units)
            adc_rms (float)   : RMS ADC value (in ADC units)
            adc_power (float) : Mean ADC power (in ADC units squared)
            f_host (str)      : The hostname of the SNAP board to which this antenna is connected
            host_ant_id (int) : The SNAP ADC channel number (0-7) to which this antenna is connected
            pam_atten (int)   : PAM attenuation setting for this antenna (dB)
            pam_power (float) : PAM power sensor reading for this antenna (dBm)
            pam_voltage (float)   : PAM voltage sensor reading for this antenna (V)
            pam_current (float)   : PAM current sensor reading for this antenna (A)
            pam_id (list of ints) : Bytewise serial number of this PAM
            fem_voltage (float)   : FEM voltage sensor reading for this antenna (V)
            fem_current (float)   : FEM current sensor reading for this antenna (A)
            fem_id (list)         : Bytewise serial number of this FEM
            fem_switch(str)       : Switch state for this FEM ('antenna', 'load', or 'noise')
            fem_e_lna_power(bool) : True if East-pol LNA is powered
            fem_n_lna_power(bool) : True if North-pol LNA is powered
            fem_imu_theta (float) : IMU-reported theta (degrees)
            fem_imu_phi (float)   : IMU-reported phi (degrees)
            fem_temp (float)      : FEM temperature sensor reading for this antenna (C)
            fft_of (bool)         : True if there was an FFT overflow
            eq_coeffs (list of floats) : Digital EQ coefficients for this antenna
            histogram (list of ints) : Two-dim list: [[bin_centers][counts]] represent ADC histogram
            autocorrelation (list of floats) : Autocorrelation spectrum
            timestamp (datetime) : Asynchronous timestamp that these status entries were gathered

            Unknown values return the string "None"
        """
        stats = self._get_status_keys("ant")
        conv_methods = {
            'adc_mean': float,
            'adc_rms': float,
            'adc_power': float,
            'f_host': str,
            'host_ant_id': int,
            'pam_atten': int,
            'pam_power': float,
            'pam_voltage': float,
            'pam_current': float,
            'pam_id': json.loads,
            'fem_temp': float,
            'fem_voltage': float,
            'fem_current': float,
            'fem_id': json.loads,
            'fem_switch': str,
            'fem_e_lna_power': lambda x: (x == 'True'),
            'fem_n_lna_power': lambda x: (x == 'True'),
            'fem_imu_theta': float,
            'fem_imu_phi': float,
            'fft_of': lambda x: (x == 'True'),
            'eq_coeffs': json.loads,
            'histogram': json.loads,
            'autocorrelation': json.loads,
            'timestamp': dateutil.parser.parse,
        }
        rv = {}
        for host, val in stats.items():
            rv[host] = {}
            for key, convfunc in conv_methods.items():
                try:
                    rv[host][key] = convfunc(stats[host][key])
                except:
                    rv[host][key] = "None"
        return rv

    def get_snaprf_status(self):
        """
        Return a dictionary of SNAP input stats.

        Keys of returned dictionaries are of the form "<SNAP hostname>:"<SNAP input number>".
        Values of this dictionary are status key/val pairs.

        These keys are:
            eq_coeffs (list of floats) : Digital EQ coefficients for this antenna
            histogram (list of ints) : Two-dim list: [[bin_centers][counts]] represent ADC histogram
            autocorrelation (list of floats) : Autocorrelation spectrum
            timestamp (datetime) : Asynchronous timestamp that these status entries were gathered

            Unknown values return the string "None"
        """
        stats = self._get_status_keys("snaprf")
        conv_methods = {
            'eq_coeffs': json.loads,
            'histogram': json.loads,
            'autocorrelation': json.loads,
            'timestamp': dateutil.parser.parse,
        }
        rv = {}
        for host, val in stats.items():
            rv[host] = {}
            for key, convfunc in conv_methods.items():
                try:
                    rv[host][key] = convfunc(stats[host][key])
                except:
                    rv[host][key] = "None"
        return rv

    def get_x_status(self):
        """Return a dictionary of X-engine status flags."""
        raise NotImplementedError("No code for get_x_status.")

    def get_hashpipe_status(self):
        """Return a dictionary of hashpipe status flags from redis.

        Returns
        -------
        dict of lists
            Upper lever dictionary is keyed by hashpipe keys in redis.
            The value for each key is a list of dicts of all measurements in that redis hash.
            Sub dictionaries are formatted to be properly ingested by an influxdb as json dicts.

        """
        rv = {}
        for key in self.r.scan_iter("hashpipe:*/status"):
            data_points = rv.setdefault(key, [])
            _, _, host, pipeline, _ = key.split("/")
            vals = self.r.hgetall(key)
            timestamp = datetime.datetime.utcnow().isoformat()
            tags = {"host": host, "pipeline_id": pipeline}
            measurement = "hashpipes"
            for k in vals.keys():
                json_body = []
                # there are many floats, but redis casts everything
                # as strings. Try to recast as a number first.
                try:
                    if vals[k] == "True":
                        vals[k] = 1
                    elif vals[k] == "False":
                        vals[k] = 0
                    fields = {k: float(vals[k])}
                    if np.isnan(fields[k]):
                        continue
                except:
                    if isinstance(vals[k], (bytes)):
                        vals[k] = vals[k].decode("utf-8")
                    fields = {k: vals[k]}

                json_body += [
                    {
                        "measurement": measurement,
                        "tags": tags,
                        "time": timestamp,
                        "fields": fields,
                    }
                ]
                data_points.extend(json_body)

        return rv

    def get_feed_status(self):
        """Return a dictionary of feed sensor values."""
        raise NotImplementedError("No code for get_feed_status.")

    def get_version(self):
        """
        Return the version of various software modules in dictionary form.

        Keys of this dictionary are software packages, e.g. "hera_corr_cm", or of the form
        <package>:<script> for daemonized processes, e.g. "udpSender:hera_node_receiver.py".
        The values of this dictionary are themselves dicts, with keys:
            "version" : A version string for this package
            "timestamp" : A datetime object indicating when this version was last reported to redis

        There is one special key, "snap", in the top-level of the returned dictionary. This stores
        software and configuration states at the time the SNAPs were last initialized with the
        `hera_snap_feng_init.py` script. For the "snap" dictionary keys are:
            "version" : version string for the hera_corr_f package.
            "init_args" : arguments passed to the inialization script at runtime
            "config" : Configuration structure used at initialization time
            "config_timestamp" : datetime instance indicating when this file was updated in redis
            "config_md5" : MD5 hash of this config file
            "timestamp" : datetime object indicating when the initialization script was called.
        """
        rv = {}
        for key in self.r.scan_iter("version:*"):
            newkey = key.lstrip("version:")
            rv[newkey] = {}
            x = self._hgetall(key)
            rv[newkey]["version"] = x["version"]
            rv[newkey]["timestamp"] = dateutil.parser.parse(x["timestamp"])

        # Add this package
        rv[__package__] = {"version": __version__,
                           "timestamp": datetime.datetime.now()
                           }

        # SNAP init is a special case
        snap_init = self._hgetall("init_configuration")
        rv["snap"] = {}
        rv["snap"]["version"] = snap_init["hera_corr_f_version"]
        rv["snap"]["init_args"] = snap_init["init_args"]
        rv["snap"]["config"] = yaml.load(snap_init["config"], Loader=yaml.FullLoader)
        rv["snap"]["config_timestamp"] = datetime.datetime.utcfromtimestamp(float(snap_init["config_time"]))  # noqa
        rv["snap"]["config_md5"] = snap_init["md5"]
        rv["snap"]["timestamp"] = datetime.datetime.utcfromtimestamp(float(snap_init["init_time"]))

        return rv

    def run_correlator_test(self):
        """
        Run a correlator test using inbuilt test vector generators.

        Will take a few minutes to run. Returns True or False.
        """
        raise NotImplementedError("No code in run_correlator_test")
