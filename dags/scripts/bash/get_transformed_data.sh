#!/bin/bash

sed '1!s/[^,]*/\U&/4' ${input_path}/${input} > ${output_path}/${output}
