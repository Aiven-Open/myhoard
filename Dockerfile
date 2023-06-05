ARG PYTHON_VERSION=3.11

###############################################
### Common base for building Percona tools ####
###############################################
FROM debian:bullseye as builder-percona

# We provide default values, but they can be overridden at build time.
ARG MYSQL_VERSION=8.0.30
ARG PERCONA_SERVER_VERSION=${MYSQL_VERSION}-22
ARG PERCONA_XTRA_VERSION=${MYSQL_VERSION}-23
ENV MYSQL_VERSION=${MYSQL_VERSION}
ENV PERCONA_SERVER_VERSION=${PERCONA_SERVER_VERSION}
ENV PERCONA_XTRA_VERSION=${PERCONA_XTRA_VERSION}

RUN apt update && \
    apt upgrade && \
    # From https://percona.community/blog/2022/04/05/percona-server-raspberry-pi/
    # - apt-get, + wget
    apt install -y build-essential pkg-config cmake devscripts debconf debhelper automake bison ca-certificates \
        libcurl4-gnutls-dev libaio-dev libncurses-dev libssl-dev libtool libgcrypt20-dev zlib1g-dev lsb-release \
        python3-docutils build-essential rsync libdbd-mysql-perl libnuma1 socat librtmp-dev libtinfo5 liblz4-tool \
        liblz4-1 liblz4-dev libldap2-dev libsasl2-dev libsasl2-modules-gssapi-mit libkrb5-dev wget \
        libreadline-dev libudev-dev libev-dev libev4 libprocps-dev vim-common
# Download boost and percona-xtrabackup
RUN wget https://boostorg.jfrog.io/artifactory/main/release/1.77.0/source/boost_1_77_0.tar.gz && \
    tar -zxvf boost_1_77_0.tar.gz


###############################################
### Build Percona XtraBackup ##################
###############################################
FROM builder-percona AS builder-percona-xtrabackup

RUN wget https://downloads.percona.com/downloads/Percona-XtraBackup-LATEST/Percona-XtraBackup-${PERCONA_XTRA_VERSION}/source/tarball/percona-xtrabackup-${PERCONA_XTRA_VERSION}.tar.gz && \
    tar -zxvf percona-xtrabackup-${PERCONA_XTRA_VERSION}.tar.gz
# Build percona-xtrabackup
RUN cd percona-xtrabackup-${PERCONA_XTRA_VERSION} && \
    mkdir "arm64-build" && \
    cd "arm64-build" && \
    cmake  .. -DCMAKE_BUILD_TYPE=Release -DWITH_BOOST=/boost_1_77_0 -DCMAKE_INSTALL_PREFIX=/usr/local/xtrabackup && \
    make -j$(nproc) && \
    make install


###############################################
### Build Percona Server ######################
###############################################
FROM builder-percona AS builder-percona-server

RUN wget https://downloads.percona.com/downloads/Percona-Server-LATEST/Percona-Server-${PERCONA_SERVER_VERSION}/source/tarball/percona-server-${PERCONA_SERVER_VERSION}.tar.gz && \
    tar -zxvf percona-server-${PERCONA_SERVER_VERSION}.tar.gz
# Build percona-xtrabackup
RUN cd percona-server-${PERCONA_SERVER_VERSION} && \
    mkdir "arm64-build" && \
    cd "arm64-build" && \
    cmake  .. -DCMAKE_BUILD_TYPE=Release -DWITH_BOOST=/boost_1_77_0 -DCMAKE_INSTALL_PREFIX=/usr/local/mysql -DWITH_ZLIB=bundled && \
    make -j$(nproc) && \
    make install


###############################################
### Build Myhoard #############################
###############################################
FROM python:${PYTHON_VERSION}-bullseye

ARG MYSQL_VERSION=8.0.30
ENV MYSQL_VERSION=${MYSQL_VERSION}

RUN apt-get update && apt-get install -y \
    sudo lsb-release wget tzdata libsnappy-dev libpq5 libpq-dev software-properties-common build-essential rsync curl git libaio1 libmecab2 psmisc \
  && rm -rf /var/lib/apt/lists/*
ADD scripts /src/scripts
ADD Makefile /src/
WORKDIR /src
RUN make clean
RUN sudo scripts/remove-default-mysql
RUN sudo scripts/install-mysql-packages ${MYSQL_VERSION}

COPY --from=builder-percona-xtrabackup /usr/local/xtrabackup/bin /usr/bin
COPY --from=builder-percona-xtrabackup /usr/local/xtrabackup/lib /usr/lib
COPY --from=builder-percona-server /usr/local/mysql/bin /usr/bin
COPY --from=builder-percona-server /usr/local/mysql/lib /usr/lib

ADD requirement* /src/
RUN scripts/install-python-deps
RUN sudo scripts/create-user

ADD . /src/
RUN git config --global --add safe.directory /src
RUN python -m pip install -e .
