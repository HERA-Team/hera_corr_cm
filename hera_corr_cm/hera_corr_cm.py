from __future__ import print_function

import time
import redis
import logging
import yaml
import json
import dateutil.parser
import datetime
import numpy as np
from .helpers import add_default_log_handlers
from . import __package__, __version__

OK = True
ERROR = False

N_CHAN = 16384
SAMPLE_RATE = 500e6

LOGGER = add_default_log_handlers(logging.getLogger(__name__))


class HeraCorrCM(object):
    """
    A class to encapsulate an interface to
    the HERA correlator (i.e., SNAP boards,
    and X-engines) via a redis message store.
    """
    # A class-wide variable to hold redis connections.
    # This prevents multiple instances of HeraCorrCM
    # from creating lots and lots (and lots) of redis connections
    redis_connections = {}
    response_channels = {}

    def __init__(self, redishost="redishost", logger=LOGGER, danger_mode=False, include_fpga=False):
        """
        Create a connection to the correlator
        via a redis server.

        Args:
            redishost (str): The hostname of the machine
                on which the correlators redis server
                is running.
            logger (logging.Logger): A logging instance. If not provided,
                the class will instantiate its own.
            include_fpga (Boolean): If True, instantiate a connection to HERA
                F-engines.
            danger_mode (Boolean): If True, disables the only-allow-command-when-not-observing checks.
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
            self.redis_connections[redishost] = redis.Redis(redishost, max_connections=100)
            self.response_channels[redishost] = self.redis_connections[redishost].pubsub()
            self.response_channels[redishost].subscribe("corr:response")
            self.response_channels[redishost].get_message(timeout=0.1)  # flush "I've just subscribed" message
        self.r = self.redis_connections[redishost]
        self.corr_resp_chan = self.response_channels[redishost]

    def _get_response(self, command, timeout=10):
        """
        Get the correlator's response to `command` issued
        at `time`.

        Args:
            command: The command (JSON string) issued to the correlator.
            timeout: How many seconds to wait for a correlator response.

        Returns:
            Whatever the correlator returns for the given command
        """
        try:
            sent_message = json.loads(command)
        except:
            self.logger.error("Failed to decode sent command")
        target_time = sent_message["time"]
        target_cmd = sent_message["command"]
        # This loop only gets activated if we get a response which
        # isn't for us.
        while(True):
            message = self.corr_resp_chan.get_message(timeout=timeout)
            if message is None:
                self.logger.error("Timed out waiting for a correlator response")
                return
            try:
                message = json.loads(message["data"])
            except:
                self.logger.warning("Got a non-JSON message on the correlator response channel")
                continue
            if ((message["command"] == target_cmd) and (message["time"] == target_time)):
                return message["args"]
            else:
                self.logger.warning("Received a correlator response that wasn't meant for us")

    def _send_message(self, command, **kwargs):
        """
        Send a command to the correlator via the corr:message
        pub-sub channel

        Args:
            command: correlator command
            **kwargs: optional arguments for this command

        Returns:
            correlator response to this command
        """
        message = json.dumps({"command": command,
                              "time": time.time(),
                              "args": kwargs})
        listeners = self.r.publish("corr:message", message)
        if listeners == 0:
            self.logger.error("Sent command {cmd} "
                              "but no-one is listening!".format(cmd=command)
                              )
            return None
        else:
            return message

    def is_recording(self):
        """
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
        """
        Try and convert v into a float. If we can't, return None.
        """
        try:
            return float(v)
        except ValueError:
            return None

    def _conv_int(self, v):
        """
        Try and convert v into an int. If we can't, return None.
        """
        try:
            return int(v)
        except ValueError:
            return None

    def next_start_time(self):
        """
        Return the last trigger time (as a UNIX timestamp float) sent to the correlator.
        If this is in the future, the correlator is waiting to start taking data.
        If no valid timestamp exists, return 0.
        """
        if self.r.exists("corr:trig_time"):
            return float(self.r["corr:trig_time"])
        else:
            return 0.0

    def _require_not_recording(self):
        recording, recording_time = self.is_recording()
        if recording:
            if self.danger_mode:
                self.logger.warning("Corelator is recording, but command blocks disabled!")
                return True
            else:
                self.logger.error("Correlator is recording!")
                return False
        else:
            return True

    def secs_to_n_spectra(self, secs):
        """
        Return the number of spectra in a given interval of `secs` seconds.
        """
        return secs / ((2.0 * N_CHAN) / SAMPLE_RATE)

    def n_spectra_to_secs(self, n):
        """
        Return the time interval in seconds corresponding to `n` spectra.
        """
        return n * ((2.0 * N_CHAN) / SAMPLE_RATE)

    def take_data(self, starttime, duration, acclen, tag=None):
        """
        Start data collection on the correlator.

        Args:
            starttime (integer): Unix time at which to start taking
                data, in ms.
            duration (float): Duration of observation in seconds.
                After this time, the correlator will stop
                recording. E.g., for an accumulation time of 10 seconds,
                and a duration of 65 seconds, the correlator will record
                7 samples.
            acclen (integer): The number of spectra to accumulate per
                correlator dump. Will be rounded to a multiple of 2048. Use the
                secs_to_n_spectra() method to convert a desired integration
                time to a number of spectra.
            tag (string or None): Tag which will end up in data files as a header
                entry.

        Returns: Unix time (in ms) at which the correlator has been
            instructed to start recording.
        """
        recording, recording_time = self.is_recording()
        if recording:
            self.logger.error("Cannot start correlator -- it is already taking data")
            return ERROR
        else:
            sent_message = self._send_message("record", starttime=starttime, duration=duration, tag=tag, acclen=acclen)
            if sent_message is None:
                return ERROR
            response = self._get_response(sent_message, timeout=30)
            if response is None:
                return ERROR
            try:
                # correlator always rounds down
                # in ms
                time_diff = starttime - response["starttime"]
            except:
                self.logger.error("Couldn't parse "
                                  "response {rsp}".format(rsp=response)
                                  )
                return ERROR

            if time_diff > 100:
                self.logger.warning("Time difference between "
                                    "commanded and accepted start "
                                    "time is {diff:f}ms".format(diff=time_diff)
                                    )
                return ERROR
            self.logger.info("Starting correlator at time {start} "
                             "({diff:.3f}ms before commanded)"
                             .format(start=time.ctime(response["starttime"] / 1000.),
                                     diff=time_diff)
                             )
            return response["starttime"]

    def stop_taking_data(self):
        """
        Stop the correlator data collection process.
        """
        sent_message = self._send_message("stop")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message, timeout=40)
        if response is None:
            return ERROR
        return OK

    def phase_switch_disable(self, timeout=10):
        """
        Disables phase switching.
        Blocked if the correlator is recording.
        """
        if not self._require_not_recording():
            return ERROR
        sent_message = self._send_message("phase_switch", activate=False)
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if not self.phase_switch_is_on()[0]:
            return OK
        return ERROR

    def phase_switch_enable(self):
        """
        Enables phase switching. Uses phase switch
        settings in the correlator active configuration.
        Blocked if the correlator is recording.
        """
        if not self._require_not_recording():
            return ERROR
        sent_message = self._send_message("phase_switch", activate=True)
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if self.phase_switch_is_on()[0]:
            return OK
        return ERROR

    def phase_switch_is_on(self):
        """
        Returns: enable_state, UNIX timestamp (float) of last state change
        enable_state is True if phase switching is on. Else False.
        """
        x = self._hgetall("corr:status_phase_switch")
        return x["state"] == "on", float(x["time"])

    def update_config(self, configfile):
        """
        Updates the correlator configuration, which defines
        low level set up parameters such as walshing functions,
        IP addresses, band selection, etc.

        Args:
            configfile: A path to a valid correlator configuration
                yaml file.
        """
        with open(configfile, "r") as fh:
            upload_time = time.time()
            self.r.hmset("snap_configuration", {"config": fh.read(), "upload_time": upload_time, "upload_time_str": time.ctime(upload_time)})

    def get_config(self):
        """
        Get the currently loaded configuration, as a
        processed yaml string.
        Returns: last update time (UNIX timestamp float), Configuration structure, configuration hash
        """
        config = self.r.hget("snap_configuration", "config")
        config_time = self.r.hget("snap_configuration", "upload_time")
        md5 = self.r.hget("snap_configuration", "md5")
        return float(config_time), yaml.load(config, Loader=yaml.FullLoader), md5

    def restart(self):
        """
        Restart (power cycle) the correlator, returning it to the settings
        in the current configuration. Will reset ADC delay calibrations.
        Returns OK or ERROR
        """
        stop_stat = self._stop()
        start_stat = self._start()
        if (stop_stat == OK) and (start_stat == OK):
            return OK
        else:
            return ERROR

    def _stop(self):
        """
        Stop the X-Engines and data catcher.
        """
        self.logger.info("Issuing Hard Stop command")
        # Try and be gracious
        self.stop_taking_data()
        is_recording, is_recording_time = self.is_recording()  # This is a stupid definition.
        if is_recording:
            self.logger.warning("Data taking failed to end gracefully")

        # Whether or not data taking stopped, hard stop everything
        sent_message = self._send_message("hard_stop")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message, timeout=120)
        if response is None:
            return ERROR
        return OK

    def _start(self):
        """
        Start the X-Engines and data catcher.
        """
        self.logger.info("Issuing Hard Start command")
        sent_message = self._send_message("start")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message, timeout=300)
        if response is None:
            return ERROR
        return OK

    def antenna_enable(self, ant=None):
        """
        Enables antenna state. Used to turn off noise diode and load.
        inputs:
            ant (integer): HERA antenna number to switch to antenna. Set to None for all antennas.
        returns:
            ERROR or OK
        """
        if (ant is not None) and (not isinstance(ant, int)):
            self.logger.error("Invalid `ant` argument. Should be integer or None")
            return ERROR
        if not self._require_not_recording():
            return ERROR
        sent_message = self._send_message("rf_switch", ant=ant, input_sel="antenna")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if "err" in response:
            self.logger.error(response["err"])
            return ERROR
        if (ant is not None) or (not self.noise_diode_is_on()[0] and not self.load_is_on()[0]):
            return OK
        return ERROR

    def noise_diode_enable(self, ant=None):
        """
        Enable FEM noise diodes.
        inputs:
            ant (integer): HERA antenna number to switch to noise. Set to None to switch all antennas.
        returns:
            ERROR or OK
        """
        if (ant is not None) and (not isinstance(ant, int)):
            self.logger.error("Invalid `ant` argument. Should be integer or None")
            return ERROR
        if not self._require_not_recording():
            return ERROR
        sent_message = self._send_message("rf_switch", ant=ant, input_sel="noise")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if "err" in response:
            self.logger.error(response["err"])
            return ERROR
        if (ant is not None) or (self.noise_diode_is_on()[0] and not self.load_is_on()[0]):
            return OK
        return ERROR

    def noise_diode_disable(self, ant=None):
        """
        Disable FEM noise diodes.
        inputs:
            ant (integer): HERA antenna number to switch to noise. Set to None to switch all antennas.
        returns:
            ERROR or OK
        """
        self.antenna_enable(ant=ant)

    def load_enable(self, ant=None):
        """
        Enable FEM load terminator.
        inputs:
            ant (integer): HERA antenna number to switch to load. Set to None to switch all antennas.
        returns:
            ERROR or OK
        """
        if (ant is not None) and (not isinstance(ant, int)):
            self.logger.error("Invalid `ant` argument. Should be integer or None")
            return ERROR
        if not self._require_not_recording():
            return ERROR
        sent_message = self._send_message("rf_switch", ant=ant, input_sel="load")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if "err" in response:
            self.logger.error(response["err"])
            return ERROR
        if (ant is not None) or (self.load_is_on()[0] and not self.noise_diode_is_on()[0]):
            return OK
        return ERROR

    def load_disable(self, ant=None):
        """
        Disable FEM load terminator.
        inputs:
            ant (integer): HERA antenna number to switch to noise. Set to None to switch all antennas.
        returns:
            ERROR or OK
        """
        self.antenna_enable(ant=ant)

    def noise_diode_is_on(self):
        """
        Returns: enable_state, UNIX timestamp (float) of last state change
        enable_state is True if noise diode is on. Else False.
        """
        x = self._hgetall("corr:status_noise_diode")
        return x["state"] == "on", float(x["time"])

    def load_is_on(self):
        """
        Returns: enable_state, UNIX timestamp (float) of last state change
        enable_state is True if load is on. Else False.
        """
        x = self._hgetall("corr:status_load")
        return x["state"] == "on", float(x["time"])

    def set_eq_coeffs(self, ant, pol, coeffs):
        """
        Set the gain coefficients for a given feed.
        inputs:
            ant (integer): HERA antenna number to query
            pol (string): Polarization to query (must be 'e' or 'n')
            coeffs (numpy.array): Coefficients to load.
        returns:
            ERROR or OK
        """
        coeffs_list = coeffs.tolist()
        sent_message = self._send_message("snap_eq", ant=ant, pol=pol, coeffs=coeffs_list)
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if "err" in response:
            self.logger.error(response["err"])
            return ERROR
        return OK

    def get_eq_coeffs(self, ant, pol):
        """
        Get the currently loaded gain coefficients for a given feed.
        inputs:
            ant (integer): HERA antenna number to query
            pol (string): Polarization to query (must be 'e' or 'n')
        returns:
            time (UNIX timestamp float), coefficients (numpy array of floats)
            or ERROR, in the case of a failure
        """
        try:
            v = {key.decode(): val.decode() for key, val in self.r.hgetall('eq:ant:{ant:d}:{pol}'.format(ant=ant, pol=pol)).items()}
        except KeyError:
            self.logger.error("Failed to get antenna coefficients from redis. Does this antenna exist?")
            return ERROR
        try:
            t = float(v['time'])
        except:
            self.logger.error("Failed to cast EQ coefficient upload time to float")
            return ERROR
        try:
            coeffs = np.array(json.loads(v['values']), dtype=np.float)
        except:
            self.logger.error("Failed to cast EQ coefficients to numpy float array")
            return ERROR
        return t, coeffs

    def get_pam_atten(self, ant, pol):
        """
        Get the currently loaded pam attenuation value for a given feed.
        inputs:
            ant (integer): HERA antenna number to query
            pol (string): Polarization to query (must be 'e' or 'n')
        returns:
            ERROR, or attenuation value in dB (integer)
        """
        sent_message = self._send_message("pam_atten", ant=ant, pol=pol, rw="r")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if "err" in response:
            self.logger.error(response["err"])
            return ERROR
        if "val" not in response:
            return ERROR
        return response["val"]

    def set_pam_atten(self, ant, pol, atten):
        """
        Get the currently loaded pam attenuation value for a given feed.
        inputs:
            ant (integer): HERA antenna number to query
            pol (string): Polarization to query (must be 'e' or 'n')
            atten (integer): Attenuation value in dB
        returns:
            OK or ERROR
        """
        sent_message = self._send_message("pam_atten", ant=ant, pol=pol, rw="w", val=atten)
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if "err" in response:
            self.logger.error(response["err"])
            return ERROR
        return OK

    def get_bit_stats(self):
        """
        Returns bit statistics from F-engines.
        Different antennas / stats are sampled at different
        times, so each is accompanied by a timestamp.
        """

    def _get_status_keys(self, stattype):
        """
        Get a list of keys which exist in redis of the form
        status:`class`:*, and return a dictionary, keyed by these values,
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
            rv[key.decode().lstrip(keystart)] = self._hgetall(key)
        return rv

    def _hgetall(self, rkey):
        """
        A wrapper around self.r.hgetall(rkey) which converts (.decode()'s)
        the keys and values of the resulting byte array to a string.
        """
        return {key.decode(): val.decode() for key, val in self.r.hgetall(rkey).items()}

    def get_f_status(self):
        """
        Returns a dictionary of snap status flags. Keys of returned dictionaries are
        snap hostnames. Values of this dictionary are status key/val pairs.

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
        Returns a dictionary of antenna status flags. Keys of returned dictionaries are
        of the form "<antenna number>:"<e|n>". Values of this dictionary are status key/val pairs.

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
            eq_coeffs (list of floats) : Digital EQ coefficients for this antenna
            histogram (list of ints) : Two-dimensional list: [[bin_centers][counts]] representing ADC histogram
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
        Returns a dictionary of SNAP input stats. Keys of returned dictionaries are
        of the form "<SNAP hostname>:"<SNAP input number>". Values of this dictionary are status key/val pairs.

        These keys are:
            eq_coeffs (list of floats) : Digital EQ coefficients for this antenna
            histogram (list of ints) : Two-dimensional list: [[bin_centers][counts]] representing ADC histogram
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
        """
        Returns a dictionary of X-engine status flags.
        """

    def get_feed_status(self):
        """
        Returns a dictionary of feed sensor values.
        """

    def get_version(self):
        """
        Returns the version of various software modules in dictionary form.
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
            newkey = key.decode().lstrip("version:")
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
        rv["snap"]["config_timestamp"] = datetime.datetime.utcfromtimestamp(float(snap_init["config_time"]))
        rv["snap"]["config_md5"] = snap_init["md5"]
        rv["snap"]["timestamp"] = datetime.datetime.utcfromtimestamp(float(snap_init["init_time"]))

        return rv

    def run_correlator_test(self):
        """
        Run a correlator test using inbuilt test vector generators.
        Will take a few minutes to run. Returns OK or ERROR.
        """
