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
from contextlib import closing
import numpy as np

def split_sharded_path(path):
    """Split a path containing shard notation into prefix, format, suffix, and number."""
    match = re.search(r"^(.*)@([0-9]+)(.*)", path)
    if not match:
        return path, None
    prefix = match.group(1)
    num = int(match.group(2))
    suffix = match.group(3)
    fmt = "%%0%dd" % len(match.group(2))
    return prefix+fmt+suffix, num

def path_shards(path):
    fmt, n = split_sharded_path(path)
    if n is None:
        yield fmt
    else:
        for i in range(n):
            yield (fmt % i)

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

def autodecode1(data, tname):
    extension = re.sub(r".*\.", "", tname).lower()
    if extension in ["png", "jpg", "jpeg"]:
        import numpy as np
        data = StringIO.StringIO(data)
        try:
            import imageio
            return np.array(imageio.imread(data, format=extension))
        except:
            pass
        import scipy.misc
        return scipy.misc.imread(data)
    if extension in ["json", "jsn"]:
        import simplejson
        return simplejson.loads(data)
    if extension in ["pyd", "pickle"]:
        import pickle
        return pickle.loads(data)
    if extension in ["mp", "msgpack", "msg"]:
        import msgpack
        return msgpack.unpackb(data)
    return data

def autodecode(sample):
    return {k: autodecode1(v, k) for k, v in sample.items()}

def autoencode1(data, tname):
    extension = re.sub(r".*\.", "", tname).lower()
    if extension in ["png", "jpg", "jpeg"]:
        import imageio
        if isinstance(data, np.ndarray):
            if data.dtype in [np.dtype("f"), np.dtype("d")]:
                assert np.amin(data) >= 0.0, (data.dtype, np.amin(data))
                assert np.amax(data) <= 1.0, (data.dtype, np.amax(data))
                data = np.array(255 * data, dtype='uint8')
            elif data.dtype in [np.dtype("uint8")]:
                pass
            else:
                raise ValueError("{}: unknown image array dtype".format(data.dtype))
        else:
            raise ValueError("{}: unknown image type".format(type(data)))
        stream = StringIO.StringIO()
        imageio.imsave(stream, data, format=extension)
        result = stream.getvalue()
        del stream
        return result
    if extension in ["json", "jsn"]:
        import simplejson
        return simplejson.dumps(data)
    if extension in ["pyd", "pickle"]:
        import pickle
        return pickle.dumps(data)
    if extension in ["mp", "msgpack", "msg"]:
        import msgpack
        return msgpack.packb(data)
    return data

def autoencode(sample):
    return {k: autoencode1(v, k) for k, v in sample.items()}

