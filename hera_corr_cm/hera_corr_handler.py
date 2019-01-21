import logging
import redis
import time
import json
import logging

from subprocess import Popen, PIPE
import helpers
from hera_corr_cm import HeraCorrCM

CATCHER_HOST = "hera-sn1"
FILE_TIME_MS = 200000

class HeraCorrHandler(object):
    def __init__(self, redishost="redishost", logger=helpers.add_default_log_handlers(logging.getLogger(__name__)), testmode=False):
        self.logger = logger
        self.redishost = redishost
        self.testmode = testmode

        self.cm = HeraCorrCM(redishost=self.redishost)
        self.r = redis.Redis(self.redishost)
        self.cmd_chan = self.r.pubsub()
        self.cmd_chan.subscribe("corr:message")
        self.cmd_chan.get_message(timeout=0.1)

    def process_command(self):
        message = self.cmd_chan.get_message(timeout=5)
        if message is not None:
            self._cmd_handler(message["data"])

    def _send_response(self, command, time, **kwargs):
        message_dict = {"command":command, "time":time, "args":kwargs}
        n = self.r.publish("corr:response", json.dumps(message_dict))
    
    def _start_capture(self, starttime, duration, acclen, tag):
        """
        Start data capture. First issues a stop, and waits 20 seconds.
        args:
            starttime: UNIX time for start trigger in ms
            duration: Number of seconds to record for
            acclen: number of 2048-spectra blocks per accumulation
            tag: human-friendly string with which to tag data
        """
        self._stop_capture()
        time.sleep(20)
        self.logger.info("Starting correlator")
        proc = Popen(["hera_ctl.py", "start", "-n", "%d" % acclen, "-t", "%f" % (starttime / 1000.)])
        proc.wait()
        nfiles = int((1000. * duration) / FILE_TIME_MS)
        self.logger.info("Taking data on %s: %d files of length %d ms" % (CATCHER_HOST, nfiles, FILE_TIME_MS))
        proc = Popen(["hera_catcher_take_data.py", "-m", "%d" % FILE_TIME_MS, "-n", "%d" % nfiles, "-t", tag, CATCHER_HOST])
    
    def _stop_capture(self):
        self.logger.info("Stopping correlator")
        proc = Popen(["hera_ctl.py", "stop"])
        proc.wait()
    
    def _cmd_handler(self, message):
        d = json.loads(message)
        command = d["command"]
        time = d["time"]
        args = d["args"]
        self.logger.info("Got command: %s" % command)
        self.logger.info("       args: %s" % args)
        if self.testmode:
            return
        if command == "record":
            self._start_capture(args["starttime"], args["duration"], args["acclen"], args["tag"])
            starttime = float(self.r["corr:trig_time"]) * 1000 #Send in ms
            self._send_response(command, time, starttime=starttime)
        elif command == "stop":
            self._stop_capture()
            self._send_response(command, time)
