#! /usr/bin/env python

import os
import glob
from setuptools import setup, find_packages

VERSION = "0.0.1"

PACKAGES = find_packages()
print PACKAGES
REQUIRES = ["redis", "pyaml"]

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
