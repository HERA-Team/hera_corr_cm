from __future__ import print_function

import logging
import redis
import time
import json

from subprocess import Popen, PIPE
from . import helpers
from .hera_corr_cm import HeraCorrCM

CATCHER_HOST = "hera-sn1"
SNAP_HOST = "hera-snap-head"
SNAP_USER = "hera"
SNAP_ENVIRONMENT = "~/.venv/bin/activate"
X_HOSTS = ["px%d" % i for i in range(1, 17)]
X_PIPES = 2

ERROR = False
OK = True

class HeraCorrHandler(object):
    def __init__(self, redishost="redishost", logger=helpers.add_default_log_handlers(logging.getLogger(__name__)), testmode=False):
        self.logger = logger
        self.redishost = redishost
        self.testmode = testmode

        self.cm = HeraCorrCM(redishost=self.redishost, include_fpga=False)
        self.r = redis.Redis(self.redishost)
        self.cmd_chan = self.r.pubsub()
        self.cmd_chan.subscribe("corr:message")
        self.cmd_chan.get_message(timeout=0.1)

    def process_command(self):
        message = self.cmd_chan.get_message(timeout=5)
        if message is not None:
            self._cmd_handler(message["data"])

    def _send_response(self, command, time, **kwargs):
        message_dict = {"command": command,
                        "time": time,
                        "args": kwargs
                        }
        n = self.r.publish("corr:response", json.dumps(message_dict))

    def _gpu_is_on(self):
        """
        Returns True if GPUSTAT is "on" for the all nodes
        """
        on = True
        for host in X_HOSTS:
            for pipe in range(X_PIPES):
                on = on and self.r.hget("hashpipe://{host}/{pipe:d}/status".format(host=host, pipe=pipe), "INTSTAT") == "on"
        return on

    def _gpu_is_off(self):
        """
        Returns True if GPUSTAT is "off" for the all nodes
        """
        off = True
        for host in X_HOSTS:
            for pipe in range(X_PIPES):
                off = off and self.r.hget("hashpipe://{host}/{pipe:d}/status".format(host=host, pipe=pipe), "INTSTAT") == "off"
        return off

    def _outthread_is_blocked(self):
        """
        Returns True if OUTSTAT is "blocked" for the all nodes
        """
        blocked = True
        for host in X_HOSTS:
            for pipe in range(X_PIPES):
                blocked = blocked and self.r.hget("hashpipe://{host}/{pipe:d}/status".format(host=host, pipe=pipe), "OUTSTAT") == "blocked"
        return blocked

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
        self.logger.info("Starting correlator")
        # Hack which assumes that there is a BDA config file
        # resulting in 4 GPU integrations going in to each
        # output sample
        acclen = acclen//4

        # For BDA files, the file length is fixed relative to the
        # underlying integration rate.
        # duration = Nt_per_file * Nsamp_bda * acclen * time_demux * 2 
        file_duration_ms = 2 * 2 * (acclen * 2) * X_PIPES * 2 * 8192/500e6 * 1000
        proc = Popen(["hera_ctl.py",
                      "start",
                      "-n", "{len:d}".format(len=acclen),
                      "-t", "{start:f}".format(start=starttime / 1000.)
                      ]
                     )
        proc.wait()
        # If the duration is less than the default file time, take one file of length duration.
        # Else take files of default size, rounding down the total number of files.
        if (1000 * duration) < file_duration_ms:
            file_time_ms = 1000 * duration
            nfiles = 1
        else:
            file_time_ms = file_duration_ms
            nfiles = int((1000. * duration) / file_duration_ms)
        self.logger.info("Taking data on {host:s}: "
                         "{nfile:d} files of length "
                         "{len:d} ms".format(host=CATCHER_HOST,
                                             nfile=nfiles,
                                             len=file_time_ms)
                         )
        proc = Popen(["hera_catcher_take_data.py",
                      "-m", "{time:d}".format(time=file_time_ms),
                      "-n", "{nfile:d}".format(nfile=nfiles),
                      "--tag", tag, CATCHER_HOST
                      ]
                     )
        proc.wait()

    def _xtor_down(self):
        self.logger.info("Issuing hera_catcher_down.sh")
        proc2 = Popen(["hera_catcher_down.sh"])
        proc2.wait()
        self.logger.info("Issuing xtor_down.sh")
        proc1 = Popen(["xtor_down.sh"])
        proc1.wait()

    def _xtor_up(self, input_power_target=None, output_rms_target=None):

        # For BDA snap_init has to happen first to ensure the antennas in the config file are correct.
        self.logger.info("Issuing hera_snap_feng_init.py -P -s -e -i")
        proc3 = Popen(["ssh",
                       "{user:s}@{host:s}".format(user=SNAP_USER, host=SNAP_HOST),
                       "source", SNAP_ENVIRONMENT,
                       "&&",
                       "hera_snap_feng_init.py",
                       "-P", "-s", "-e", "-i", "--noredistapcp"])
        proc3.wait()
        if int(proc3.returncode) != 0:
            self.logger.error("Error running hera_snap_feng_init.py")
            return ERROR
        if input_power_target is not None:
            self.logger.info("Issuing input balance "
                             "with target {pow:f}".format(pow=input_power_target)
                             )
            proc3 = Popen(["ssh",
                           "{user:s}@{host:s}".format(user=SNAP_USER, host=SNAP_HOST),
                           "source", SNAP_ENVIRONMENT,
                           "&&",
                           "hera_snap_input_power_eq.py",
                           "-e", "{pow:f}".format(pow=input_power_target),
                           "-n", "{pow:f}".format(pow=input_power_target)
                           ]
                          )
            proc3.wait()
            if int(proc3.returncode) != 0:
                self.logger.error("Error running hera_snap_input_power_eq.py")
                return ERROR
        if output_rms_target is not None:
            self.logger.info("Issuing output balance "
                             "with target {rms:f}".format(rms=output_rms_target)
                             )
            proc3 = Popen(["ssh",
                           "{user:s}@{host:s}".format(user=SNAP_USER, host=SNAP_HOST),
                           "source", SNAP_ENVIRONMENT,
                           "&&",
                           "hera_snap_output_power_eq.py",
                           "--rms", "{rms:f}".format(rms=output_rms_target)
                           ]
                          )
            proc3.wait()
            if int(proc3.returncode) != 0:
                self.logger.error("Error running hera_snap_output_power_eq.py")
                return ERROR

        self.logger.info("Issuing xtor_up.py --runtweak px{1..16}")
        proc1 = Popen(["xtor_up.py", "--runtweak", "--redislog"] + X_HOSTS)
        self.logger.info("Issuing hera_catcher_up.py")
        proc2 = Popen(["hera_catcher_up.py", "--redislog", CATCHER_HOST])
        proc1.wait()
        proc2.wait()
        return OK

    def _stop_capture(self):
        self.logger.info("Stopping correlator")
        proc = Popen(["hera_catcher_stop_data.py", CATCHER_HOST])
        proc.wait()
        stop_time = time.time()
        TIMEOUT = 30
        self.logger.info("Waiting for catcher to stop")
        while(time.time() - stop_time) < TIMEOUT:
            recording, update_time = self.cm.is_recording()
            if not recording:
                self.logger.info("Correlator is not recording")
                break
        # If X engines are already stopped do nothing
        if self._gpu_is_off():
            time.sleep(1)
            if self._outthread_is_blocked():
                return
        proc = Popen(["hera_ctl.py", "stop"])
        proc.wait()
        self.logger.info("Waiting for correlator to stop")
        stop_time = time.time()
        TIMEOUT = 30
        while(time.time() - stop_time) < TIMEOUT:
            if self._gpu_is_off():
                break
            time.sleep(1)
        time.sleep(1)
        while(time.time() - stop_time) < TIMEOUT:
            if self._outthread_is_blocked():
                self.logger.info("X-Engines have stopped")
                return
        self.logger.warning("X-Engines failed to stop in %d seconds" % TIMEOUT)

    def _cmd_handler(self, message):
        d = json.loads(message)
        command = d["command"]
        time = d["time"]
        args = d["args"]
        self.logger.info("Got command: {cmd:s}".format(cmd=command))
        self.logger.info("       args: {args:s}".format(args=args))
        if command == "record":
            if not self.testmode:
                self._start_capture(args["starttime"],
                                    args["duration"],
                                    args["acclen"],
                                    args["tag"]
                                    )
            starttime = float(self.r["corr:trig_time"]) * 1000  # Send in ms
            self._send_response(command, time, starttime=starttime)
        elif command == "stop":
            if not self.testmode:
                self._stop_capture()
            self._send_response(command, time)
        elif command == "hard_stop":
            if not self.testmode:
                self._xtor_down()
            self._send_response(command, time)
        elif command == "start":
            if not self.testmode:
                self._xtor_up()
            self._send_response(command, time)
