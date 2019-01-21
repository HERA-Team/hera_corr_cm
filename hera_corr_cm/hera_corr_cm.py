import time
import redis
import logging
import yaml
import json
from helpers import add_default_log_handlers

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

    def __init__(self, redishost="redishost", logger=LOGGER):
        """
        Create a connection to the correlator
        via a redis server.

        Args:
            redishost: The hostname of the machine
                on which the correlators redis server
                is running.
            logger: A logging instance. If not provided,
                the class will instantiate its own.
        """
        self.logger = logger
        # If the redishost is one we've already connected to, use it again.
        # Otherwise, add it.
        # Also share response channels. This opens the door to all sorts of
        # unintended consequences if you try to multithread access to
        # multiple HeraCorrCM instances in the same program. But in most cases
        # sharing means the code will just do The Right Thing, and won't leave
        # a trail of a orphaned connections.
        if redishost not in self.redis_connections.keys():
            self.redis_connections[redishost] = redis.Redis(redishost, max_connections=100)
            self.response_channels[redishost] = self.redis_connections[redishost].pubsub()
            self.response_channels[redishost].subscribe("corr:response")
            self.response_channels[redishost].get_message(timeout=0.1) # flush "I've just subscribed" message
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
        target_cmd  = sent_message["command"]
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
        message = json.dumps({"command":command, "time":time.time(), "args":kwargs})
        listeners = self.r.publish("corr:message", message)
        if listeners == 0:
            self.logger.error("Sent command %s but no-one is listening!" % command)
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
            x = self.r.hgetall("corr:is_taking_data")
            return x["state"] == "True", float(x["time"])

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
            self.logger.error("Correlator is recording!")
            return False
        else:
            return True

    def secs_to_n_spectra(self, secs):
        """
        Return the number of spectra in a given interval of `secs` seconds.
        """
        return secs / ((2.0*N_CHAN) / SAMPLE_RATE)

    def n_spectra_to_secs(self, n):
        """
        Return the time interval in seconds corresponding to `n` spectra.
        """
        return n * ((2.0*N_CHAN) / SAMPLE_RATE)


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

        Returns: Unix time at which the correlator has been
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
                # in ms
                time_diff = starttime - response["starttime"] # correlator always rounds down
            except:
                self.logger.error("Couldn't parse response %s" % response)
                return ERROR
           
            if time_diff  > 100:
                self.logger.warning("Time difference between commanded and accepted start time is %fms" % time_diff)
                return ERROR
            self.logger.info("Starting correlator at time %s (%.3fms before commanded)" % (time.ctime(response["starttime"] / 1000.), time_diff))
            return response["starttime"]

    def stop_taking_data(self):
        """
        Stop the correlator data collection process.
        """
        sent_message = self._send_message("stop")
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
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
        x = self.r.hgetall("corr:status_phase_switch")
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
            self.r.hmset("snap_configuration", {"config":fh.read(), "upload_time":upload_time, "upload_time_str":time.ctime(upload_time)})


    def get_config(self):
        """
        Get the currently loaded configuration, as a
        processed yaml string.
        Returns: last update time (UNIX timestamp float), Configuration structure, configuration hash
        """
        config      = self.r.hget("snap_configuration", "config")
        config_time = self.r.hget("snap_configuration", "upload_time")
        md5         = self.r.hget("snap_configuration", "md5")
        return float(config_time), yaml.load(config), md5

    def restart(self):
        """
        Restart (power cycle) the correlator, returning it to the settings
        in the current configuration. Will reset ADC delay calibrations.
        """

    def noise_diode_enable(self):
        """
        Enables noise diodes on all antennas. Blocked if the correlator
        is recording.
        """
        if not self._require_not_recording():
            return ERROR
        sent_message = self._send_message("noise_diode", activate=True)
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if self.noise_diode_is_on()[0]:
            return OK
        return ERROR

    def noise_diode_disable(self):
        """
        Disables noise diodes on all antennas. Blocked if the correlator
        is recording.
        """
        if not self._require_not_recording():
            return ERROR
        sent_message = self._send_message("noise_diode", activate=False)
        if sent_message is None:
            return ERROR
        response = self._get_response(sent_message)
        if response is None:
            return ERROR
        if not self.noise_diode_is_on()[0]:
            return OK
        return ERROR

    def noise_diode_is_on(self):
        """
        Returns: enable_state, UNIX timestamp (float) of last state change
        enable_state is True if noise diode is on. Else False.
        """
        x = self.r.hgetall("corr:status_noise_diode")
        return x["state"] == "on", float(x["time"])

    def get_bit_stats(self):
        """
        Returns bit statistics from F-engines.
        Different antennas / stats are sampled at different
        times, so each is accompanied by a timestamp.
        """

    def get_f_status(self):
        """
        Returns a dictionary of snap status flags. 
        """

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
        Returns a dictionary of git hashes for various software/firmware modules
        """

    def run_correlator_test(self):
        """
        Run a correlator test using inbuilt test vector generators.
        Will take a few minutes to run. Returns OK or ERROR.
        """
