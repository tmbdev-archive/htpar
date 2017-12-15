import argparse
import sys
import os
import os.path
import re
import StringIO
import tarfile
import warnings
import time
import imp
import tempfile
from dask.distributed import Client
from contextlib import closing

def splitallext(path):
    """Helper method that splits off all extension.

    Returns base, allext.

    :param path: path with extensions
    :returns: path with all extensions removed

    """
    match = re.match(r"^(.*?/?[^.]+)[.]([^/]*)$", path)
    if not match:
        return None, None
    return match.group(1), match.group(2)

def base_plus_ext(fname):
    """Splits pathnames into the file basename plus the extension."""
    return splitallext(fname)

def dir_plus_file(fname):
    """Splits pathnames into the dirname plus the filename."""
    return os.path.split(fname)

def last_dir(fname):
    """Splits pathnames into the last dir plus the filename."""
    dirname, plain = os.path.split(fname)
    prefix, last = os.path.split(dirname)
    return last, plain

class Matcher(object):
    def __init__(self, regex):
        self.regex = re.compile(regex)
    def match(self, x):
        match = self.regex.search(x)
        if not match:
            raise ValueError("{}: no match".format(self.regex))
        return match.group(1), match.group(2)

def get_keyfun(name):
    if name=="base_plus_ext":
        return base_plus_ext
    elif name=="dir_plus_file":
        return dir_plus_file
    elif name=="last_dir":
        return last_dir
    elif "(" in name:
        matcher = Matcher(name)
        return matcher.match
    else:
        raise ValueError("{}: unknown key function".format(name))