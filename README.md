A set of simple Python/Dask-based parallel computing tools operating
over sharded tar files.

Planned components:

 - htmapper: process records in sharded tar files in parallel
 - htextract: extract data from a collection of sharded tar files
 - htshuffle: shuffle and reshard a collection of sharded tar files
 - htsort: sort, reduce, and reshard a collection of sharded tar files
 - htupload: upload a dataset as a collection of sharded tar files

Together, these programs give you the functionality of map-reduce
systems, but they are conceptually simpler and use HTTP GET/PUT for
storage; they can be used with Amazon S3, Google Cloud Storage, and
NGINX servers.

The sharded tar files can be used as input to the `dlinputs` library
for PyTorch.

These tools use Dask for distributed processing. By default, jobs
just run multithreaded on the local machine, but you can start up
a `dask-scheduler` and multiple `dask-worker` processes on different
machines. In the cloud, you can start up everything using `dask-kubernetes`
or similar tools.

htmapper
--------

The `htmapper` command line program takes small Python scripts as
input and applies them to each sample; the script must define
a `process_sample` function, which gets the training sample as dictionary
and needs to return another dictionary with the processed sample. Both
inputs and outputs are just the raw binary contents of the fields of
the sample; if you want to perform image processing, you need to decode
them first.  The program parallelizes over the shards using Dask.

{{{
    $ cat script.py
    def process_sample(record):
        if "bin.png" not in record: return None
        if "lines.png" not in record: return None
        return {"__key__": record["__key__"],
                "png": record["bin.png"],
                "lines.png": record["lines.png"]}
    $ htmapper -c localhost:8786 -p script.py \
        gs://tmbg1000/Volume_@1000.tgz http://eunomia/processed-@1000.tgz
}}}

htextract
---------

The `htextract` script lets you quickly extract and collect fields
from a collection of sharded tar files. It is useful for collecting
all the keys in a dataset, or getting statistics about classes, etc.

{{{
    $ htextract gs://tmbdata/uw3-lines.tgz -f text
    ...
    $ 
}}}

htshuffle
---------

Example:

{{{
    $ htshuffle gs://tmbg1000/Volume_@1000.tgz http://eunomia/shuffled-@2000.tgz
}}}

htsort
------

Example:

{{{
    htsort gs://tmbg1000/Volume_@1000.tgz http://eunomia/shuffled-@2000.tgz
}}}
