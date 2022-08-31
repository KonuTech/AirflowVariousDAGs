#!/bin/bash

status_code=$(curl --write-out %{http_code} --silent --output /dev/null ${URL})
if [[ "$status_code" -ne 200 ]] ; then
  echo "Site status changed to $status_code"
  exit 1
else
  echo "Site status code 200"
  exit 0
fi
