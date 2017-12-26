#!/usr/bin/env python

from __future__ import print_function

import sys,time,urllib,traceback,glob,os,os.path

from distutils.core import setup #, Extension, Command
#from distutils.command.install_data import install_data

scripts = """
htextract htmapper
""".split()

setup(
    name = 'htpar',
    version = 'v0.0',
    author = "Thomas Breuel",
    description = "Simple map-reduce like tool for HTTP.",
    packages = ["htpar"],
    scripts = scripts,
    )
