# Dockerfile to build public pycarol image
FROM python:3.6

ENV DEBIAN_FRONTEND="noninteractive" \
    LANG="en_US.UTF-8" \
    LANGUAGE="en_US.UTF-8" \
    LC_ALL="en_US.UTF-8"

ARG PYCAROL_VERSION

RUN pip install pycarol==${PYCAROL_VERSION} && \
    apt-get update && \
    apt-get install locales locales-all -y && \
    rm -rf /var/lib/apt/lists/* && \
    dpkg-reconfigure locales && \
    locale-gen en_US.UTF-8
