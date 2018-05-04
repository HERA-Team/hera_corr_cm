import redis
import logging

OK = True
ERROR = False

LOGGER = logging.getLogger(__name__)

class HeraCorrCM(object):
    """
    A class to encapsulate an interface to
    the HERA correlator (i.e., SNAP boards,
    and X-engines) via a redis message store.
    """
    def __init__(self, redisthost="redishost", logger=LOGGER):
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

    def take_data(self, starttime, duration, tag=None):
        """
        Start data collection on the correlator.
        
        Args:
            starttime: Unix time at which to start taking
                data. Actual start time will be rounded to
                nearest ?? ms.
            duration: Duration of observation in seconds.
                After this time, the correlator will stop
                recording.
            tag: Tag which will end up in data files as a header
                entry.

        Returns: Unix time at which the correlator has been
            instructed to start recording.
        """

    def phase_switch_disable(self):
        """
        Disables phase switching.
        Blocked if the correlator is recording.
        """

    def phase_switch_enable(self):
        """
        Enables phase switching. Uses phase switch
        settings in the correlator active configuration.
        Blocked if the correlator is recording.
        """

    def phase_switch_is_on(self):
        """
        Returns True if phase switching is on. Else False.
        """

    def update_config(self, configfile):
        """
        Updates the correlator configuration, which defines
        low level set up parameters such as walshing functions,
        IP addresses, band selection, etc.

        Args:
            configfile: A path to a valid correlator configuration
                yaml file.
        """

    def get_config(self):
        """
        Return the currently loaded configuration, as a
        processed yaml string.
        """

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

    def noise_diode_disable(self):
        """
        Disables noise diodes on all antennas. Blocked if the correlator
        is recording.
        """

    def noise_diode_is_on(self):
        """
        Returns True if noise diode is on. Else False.
        """

    def is_recording(self):
        """
        Returns: True if the correlator is currently recording,
            otherwise False. Used both for status information,
            and to detect and block configuration changes during
            observations.
        """

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

