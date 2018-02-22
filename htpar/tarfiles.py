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
import hashlib
from contextlib import closing
from random import randint

import utils
import storage

###
### Tar file iterators.
###

def tarrecords(fileobj, keys=utils.base_plus_ext, decode=utils.autodecode):
    """Iterate over tar streams."""
    if decode is None:
        decode = lambda x: x
    current_count = 0
    current_prefix = None
    current_sample = None
    if isinstance(fileobj, str):
        fileobj = storage.storage.open_read(fileobj)
        closer = fileobj
    else:
        closer = None
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
                yield decode(current_sample)
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
        yield decode(current_sample)
    try: del archive
    except: pass
    if closer is not None:
        closer.close()

class TarRecords(object):
    """Write records to tar streams."""
    def __init__(self, stream, encode=utils.autoencode):
        if isinstance(stream, str):
            stream = storage.storage.open_write(stream)
            self.fileobj = stream
        else:
            self.fileobj = None
        self.tarstream = tarfile.open(fileobj=stream, mode="w:gz")
        self.encode = encode or (lambda x: x)

    def __enter__(self):
        """Context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager."""
        self.close()

    def close(self):
        """Close the tar file."""
        self.tarstream.close()
        if self.fileobj is not None:
            self.fileobj.close()

    def write(self, obj):
        """Write a dictionary to the tar file.

        This applies the conversions and renames that the TarWriter
        was configured with.

        :param str key: basename for the tar file entry
        :param str obj: dictionary of objects to be stored
        :returns: size of the entry

        """
        total = 0
        obj = self.encode(obj)
        key = obj["__key__"]
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

def tarshards(spec):
    for path in utils.path_shards(spec):
        for record in tarrecords(path):
            yield record

def default_shard_function(obj, nshards):
    key = obj["__key__"]
    assert isinstance(key, (str, unicode))
    return int(hashlib.md5(key).hexdigest()[:7], 16) % nshards

class TarShards(object):
    """Write records to multiple tar streams using a sharding function.

    FIXME: distribute this with Dask
    """
    def __init__(self, spec, dest=storage.storage, shard_function=None, override_shards=False):
        self.streams = []
        for path in utils.path_shards(spec):
            stream = dest.open_write(path)
            tarstream = TarRecords(stream)
            self.streams.append((tarstream, path, stream))
        self.override_shards = override_shards
        self.shard_function = shard_function or default_shard_function
        print "[opened %d output streams]" % len(self.streams)

    def get_shard(self, obj):
        nshards = self.nshards()
        if not self.override_shards:
            if "__shard__" in obj:
                shard = obj["__shard__"]
                assert shard >= 0 and shard < nshards
                return shard
            if "__hash__" in obj:
                hash = obj["__hash__"]
                assert isinstance(hash, int)
                if hash < 0: hash = -hash
                return hash % nshards
        return self.shard_function(obj, nshards)

    def nshards(self):
        return len(self.streams)

    def __enter__(self):
        """Context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager."""
        self.close()

    def close(self):
        """Close the tar file."""
        for ts, p, s in self.streams:
            try:
                ts.close()
                s.close()
            except:
                pass

    def write(self, obj):
        key = obj["__key__"]
        bucket = self.get_shard(obj)
        size = self.streams[bucket][0].write(obj)
        return (bucket, size)

###
### Helper for shuffling.
###

def shuffler(data, bufsize=1000, initial=100):
    """Shuffle the data in the stream.

    This uses a buffer of size `bufsize`. Shuffling at
    startup is less random; this is traded off against
    yielding samples quickly.

    :param: data: iterator
    :param: bufsize: buffer size for shuffling
    :param: initial: buffer this many elements before returning data
    :returns: mapper over iterator

    """
    assert initial <= bufsize
    buf = []
    startup = True
    def get():
        if data is None:
            return False
        try:
            buf.append(data.next())
            return True
        except StopIteration:
            return False
    while 1:
        if not get(): data = None
        if len(buf) < bufsize:
            if not get(): data = None
        if len(buf) == 0: break
        if startup and len(buf) < initial:
            continue
        else:
            startup = False
        k = randint(0, len(buf)-1)
        buf[k], buf[-1] = buf[-1], buf[k]
        yield buf.pop()
