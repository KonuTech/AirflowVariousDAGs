#!/bin/bash

cut -c 59- ${ingest_path}/${input} | sed -e 's/\s/,/g' > ${output_path}/${output}
