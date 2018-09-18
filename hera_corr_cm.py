import time
import redis
import logging
import yaml
import json
from helpers import add_default_log_handlers

OK = True
ERROR = False

LOGGER = add_default_log_handlers(logging.getLogger(__name__))

class HeraCorrCM(object):
    """
    A class to encapsulate an interface to
    the HERA correlator (i.e., SNAP boards,
    and X-engines) via a redis message store.
    """
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
        self.r = redis.Redis(redishost)
        self.logger = logger
        self.corr_resp_chan = self.r.pubsub()
        self.corr_resp_chan.subscribe("corr:response")
        self.corr_resp_chan.get_message(timeout=0.1) # flush "I've just subscribed" message

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
            self.logger.error("Sent command but no-one is listening!")
            return None
        else:
            return message

    def is_recording(self):
        """
        Return True if the correlator is currently taking data.
        """
        return self.r.exists("corr:is_taking_data")

    def _require_not_recording(self):
        if self.is_recording():
            self.logger.error("Correlator is recording!")
            return False
        else:
            return True

    def take_data(self, starttime, duration, acclen, tag=None):
        """
        Start data collection on the correlator.
        
        Args:
            starttime: Unix time at which to start taking
                data. Actual start time will be rounded to
                nearest ?? ms.
            duration: Duration of observation in seconds.
                After this time, the correlator will stop
                recording.
            acclen: Accumulation length in spectra.
            tag: Tag which will end up in data files as a header
                entry.

        Returns: Unix time at which the correlator has been
            instructed to start recording.
        """
        if self.is_recording():
            self.logger.error("Cannot start correlator -- it is already taking data")
            return ERROR
        else:
            sent_message = self._send_message("record", starttime=starttime, duration=duration, tag=tag, acclen=acclen)
            response = self._get_response(sent_message)
            if response is None:
                self.logger.error("Tried to start taking data and got no response!")
                return ERROR
            try:
                time_diff = starttime - response["starttime"] # correlator always rounds down
            except:
                self.logger.error("Couldn't parse response %s" % response)
                return ERROR
           
            if time_diff  > 0.1:
                self.logger.warning("Time difference between commanded and accepted start time is %f" % time_diff)
                return ERROR
            self.logger.info("Starting correlator at time %s" % time.ctime(response["starttime"]))
            return OK

    def phase_switch_disable(self, timeout=10):
        """
        Disables phase switching.
        Blocked if the correlator is recording.
        """
        if not self._require_not_recording():
            return ERROR
        self._send_message("phase_switch", activate=False)
        t = time.time()
        while (time.time() < (t + timeout)):
            if not self.phase_switch_is_on():
                return OK
            time.sleep(0.1)
        return ERROR
            

    def phase_switch_enable(self):
        """
        Enables phase switching. Uses phase switch
        settings in the correlator active configuration.
        Blocked if the correlator is recording.
        """
        if not self._require_not_recording():
            return ERROR
        self._send_message("phase_switch", activate=False)
        t = time.time()
        while (time.time() < (t + timeout)):
            if self.phase_switch_is_on():
                return OK
            time.sleep(0.1)
        return ERROR

    def phase_switch_is_on(self):
        """
        Returns True if phase switching is on. Else False.
        """
        return self.r["corr:status_phase_switch"] == "on"
            

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
        Return the currently loaded configuration, as a
        processed yaml string.
        """
        config  = self.r.hget("snap_configuration", "config")
        return yaml.load(config)

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
        self._send_message("noise_diode", activate=True)
        t = time.time()
        while (time.time() < (t + timeout)):
            if self.noise_diode_is_on():
                return OK
            time.sleep(0.1)
        return ERROR

    def noise_diode_disable(self):
        """
        Disables noise diodes on all antennas. Blocked if the correlator
        is recording.
        """
        if not self._require_not_recording():
            return ERROR
        self._send_message("noise_diode", activate=False)
        t = time.time()
        while (time.time() < (t + timeout)):
            if not self.noise_diode_is_on():
                return OK
            time.sleep(0.1)
        return ERROR

    def noise_diode_is_on(self):
        """
        Returns True if noise diode is on. Else False.
        """
        return self.r["corr:status_noise_diode"] == "on"

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
