This is a simple web server setup you can use for read/write serving
and for caching cloud-based datasets.

You need Docker and docker-compose (`pip install docker-compose`).
To run it, just type, in the current directory:

{{{
    docker-compose up
}}}

You probably want to adjust where the `/data` directory points
in the `docker-compose.yml` file.

The `nginx-extra.conf` file also shows how to set up data caching.

To set a password, enable the lines in `nginx-extra.conf` and
`docker-compose.yml` that refer to the `htpasswd` file.

You can set the password using the Apache2 `htpasswd` command:

{{{
  sudo htpasswd -c ./htpasswd user
}}}

The configuration file actually sets up a Webdav server, which you can
use not just for sequential reading and writing, but access from various
file managers and command line tools. For example, on Linux, you can get
an FTP-like interface with:

{{{
    cadaver http://localhost:18880/
}}}

This is useful for file management operations. However, the DL and
map-reduce pipelines just need GET/PUT operations.
