FROM debian:12

RUN export DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true && \
    apt update && apt install -y --no-install-recommends \
        build-essential \
        curl \
        python3-dev \
        python3-pip \
        python3-venv \
        vim

RUN python3 -m venv /opt/venv && \
    . /opt/venv/bin/activate && \
    python3 -m pip install elasticsearch python-geohash

COPY app/ /opt/app

ENTRYPOINT /opt/app/entrypoint
