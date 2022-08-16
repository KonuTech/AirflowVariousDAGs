#!/bin/bash

cut -d, -f1-4 --output-delimiter=',' ${ingest_path}/${input} > ${output_path}/${output}
