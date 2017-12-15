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

###
### Tar file iterators.
###

def tarrecords(fileobj, keys):
    """Iterate over tar streams."""
    current_count = 0
    current_prefix = None
    current_sample = None
    stream = tarfile.open(fileobj=fileobj, mode="r|*")
    for tarinfo in stream:
        if not tarinfo.isreg():
            continue
        fname = tarinfo.name
        if fname is None:
            warnings.warn("tarinfo.name is None")
            continue
        prefix, suffix = keys(fname)
        if prefix is None:
            warnings.warn("prefix is None for: %s" % (tarinfo.name,))
            continue
        if prefix != current_prefix:
            if current_sample is not None and \
               not current_sample.get("__bad__", False):
                yield current_sample
            current_prefix = prefix
            current_sample = dict(__key__=prefix)
        try:
            data = stream.extractfile(tarinfo).read()
        except tarfile.ReadError, e:
            print "tarfile.ReadError at", current_count
            print "file:", tarinfo.name
            print e
            current_sample["__bad__"] = True
        else:
            current_sample[suffix] = data
            current_count += 1
    if len(current_sample.keys()) > 0:
        yield current_sample
    try: del archive
    except: pass

class TarRecords(object):
    """Write records to tar streams."""
    def __init__(self, stream):
        self.tarstream = tarfile.open(fileobj=stream, mode="w:gz")

    def __enter__(self):
        """Context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager."""
        self.close()

    def close(self):
        """Close the tar file."""
        self.tarstream.close()

    def write(self, key, obj):
        """Write a dictionary to the tar file.

        This applies the conversions and renames that the TarWriter
        was configured with.

        :param str key: basename for the tar file entry
        :param str obj: dictionary of objects to be stored
        :returns: size of the entry

        """
        total = 0
        for k in sorted(obj.keys()):
            if k.startswith("_"): continue
	    v = obj[k]
	    if isinstance(v, unicode):
	       v = codecs.encode(v, "utf-8")
            assert isinstance(v, (str, buffer)),  \
                "converter didn't yield a string: %s" % ((k, type(v)),)
            now = time.time()
            ti = tarfile.TarInfo(key + "." + k)
            ti.size = len(v)
            ti.mtime = now
            ti.mode = 0o666
            ti.uname = "bigdata"
            ti.gname = "bigdata"
            content = StringIO.StringIO(v)
            self.tarstream.addfile(ti, content)
            total += ti.size
        return total

