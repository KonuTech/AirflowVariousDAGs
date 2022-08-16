#!/bin/bash

cut -f5-7 --output-delimiter=',' ${ingest_path}/${input} > ${output_path}/${output}
