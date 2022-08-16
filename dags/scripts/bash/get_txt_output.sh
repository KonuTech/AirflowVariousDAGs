#!/bin/bash

#cut -c 5-7,59- ${ingest_path}/${input} | sed 's/^[[:space:]]*//' | sed -e 's/\s/,/g' > ${output_path}/${output}
cut -c 59- ${ingest_path}/${input} | sed 's/^[[:space:]]*//' | sed -e 's/\s/,/g' | sed 's/^M//g' | tr -d '\r' > ${output_path}/${output}
