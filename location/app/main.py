#!/usr/bin/python3

from fastapi import Security, Depends, FastAPI, HTTPException, Header, Request, Response
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.logger import logger
from typing import Any, Dict, List, Union, Annotated
from fastapi.staticfiles import StaticFiles

from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse

import uvicorn

from contextlib import asynccontextmanager

import asyncio
import concurrent.futures
import contextlib
import geohash
import json
import yaml
import logging
import os
import pprint
import requests
import threading
import socket
import subprocess
import time
import hashlib
from datetime import datetime, date, time, timezone
from logging.config import dictConfig
from pprint import pprint, pformat


log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",

        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    "loggers": {
        "mylogger": {"handlers": ["default"], "level": "DEBUG"},
    },
}

dictConfig(log_config)

log = logging.getLogger('mylogger')

app = FastAPI()


def es(origdata):

    # make a copy so we can add to it for es
    data = origdata.copy()

    d = datetime.now(timezone.utc)
    idate = d.strftime("%Y.%m")

    data["timestamp"] = d.isoformat()

    # Elasticsearch connection details
    url = f"http://elasticsearch.elasticsearch:9200/job-locations-{idate}/_doc"
    headers = {"Content-Type": "application/json"}

    # Send the POST request
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def subdomain(ip):

    try:
        hostname, _, _ = socket.gethostbyaddr(ip)
    except:
        return "N/A"

    log.info(f"Remote DNS: {hostname}")

    # clean it up - we also want to remove the first part of the name,
    # which usually results in the resource we ran on
    if hostname.count(".") > 1:
        hostname = hostname.split(".", 1)[1]

    # make sure we have a name we want to keep
    if hostname.count(".") == 0:
        return None
    if "local" in hostname or \
       "private" in hostname or \
       ".cluster" in hostname:
        return "N/A"

    return hostname



@app.get("/")
def home(request: Request):

    # create a base data package
    data = {
        "ip": "N/A",
        "organization": "N/A",
        "subdomain": "N/A",
        "latitude": 0,
        "longitude": 0,
        "geohash": 0,
    }

    # Attempt to retrieve the forward for header
    client_ip = request.headers.get("X-Forwarded-For")

    if client_ip is None:
        client_ip = request.client.host

    data["ip"] = client_ip

    # geoip lookup
    url = f"https://data.isi.edu/geoip/{client_ip}"
    response = requests.get(url)
    if response.status_code == 200:
        j = response.json()
        if "asnOrganization" in j:
            data["organization"] = j["asnOrganization"]
        for attr in [ \
                "latitude", \
                "longitude" \
            ]:
            if attr in j:
                data[attr] = j[attr]

    data["subdomain"] = subdomain(data["ip"])

    # if we have lat/lon, also include a geohash
    try:
        ghash = geohash.encode(
                float(data["latitude"]),
                float(data["longitude"]), 
                precision=4)
        data["geohash"] = ghash
    except:
        pass

    # also insert into es
    es(data)

    log.info(data)

    # want to output yaml
    out = {"location": data}
    out = yaml.dump(out, indent=4)

    return Response(out, media_type='text/yaml')


if __name__ == '__main__':

    uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=8000,
            proxy_headers=True,
            forwarded_allow_ips='*'
    )


