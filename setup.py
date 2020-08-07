#! /usr/bin/env python

import os
import glob
from setuptools import setup, find_packages

VERSION = "0.0.1"
try:
    import subprocess
    ver = VERSION + '-' + subprocess.check_output(['git', 'describe', '--abbrev=8', '--always', '--dirty', '--tags']).strip().decode('utf-8')
except:
    ver = VERSION
    print('Couldn\'t get version from git. Defaulting to {ver:s}'.format(ver=ver))
print('Version is: {ver:s}'.format(ver=ver))

# Generate a __version__.py file with this version in it
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'hera_corr_cm', '__version__.py'), 'w') as fh:
    fh.write('__version__ = "{ver:s}"'.format(ver=ver))

PACKAGES = find_packages()
print(PACKAGES)
REQUIRES = ["redis", "pyyaml"]

setup_args = dict(name="hera_corr",
                  maintainer="HERA Team",
                  description="Library for interfacing with the HERA correlator",
                  url="https://github.com/HERA-Team/hera_corr_cm",
                  version=VERSION,
                  packages=PACKAGES,
                  scripts=glob.glob('scripts/*'),
                  requires=REQUIRES)

if __name__ == '__main__':
    setup(**setup_args)
    if ver.endswith("dirty"):
        print("********************************************")
        print("* You are installing from a dirty git repo *")
        print("*      One day you will regret this.       *")
        print("*                                          *")
        print("*  Consider cleaning up and reinstalling.  *")
        print("********************************************")
