FROM ubuntu:focal
ENV DEBIAN_FRONTEND="noninteractive"
RUN apt-get update && apt-get -y install sudo lsb-release wget tzdata libsnappy-dev \
    libpq5 libpq-dev software-properties-common build-essential rsync curl
RUN add-apt-repository -y ppa:deadsnakes/ppa
