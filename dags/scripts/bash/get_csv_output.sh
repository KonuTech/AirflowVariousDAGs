#!/bin/bash

cut -d, -f1-4 --output-delimiter=',' ${ingest_path}/${input} | sed 's/^M//g' | tr -d '\r' > ${output_path}/${output}
