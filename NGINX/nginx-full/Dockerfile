FROM ubuntu:16.04
MAINTAINER tmbdev <tmbdev@gmail.com>
RUN apt-get update && apt-get upgrade -y -o DPkg::Options::=--force-confold
RUN apt-get install -y nginx-full
COPY nginx.conf /etc/nginx
EXPOSE 80
