#!/bin/bash

cut -f5-7 --output-delimiter=',' ${ingest_path}/${input} | sed 's/^M//g' | tr -d '\r' > ${output_path}/${output}
