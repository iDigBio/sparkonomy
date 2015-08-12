#!/bin/bash

rm -rf sn_parsed* spark.log uniquenames_parsed*
time spark-submit sn_parser.py &> spark.log
cat sn_parsed/part-* > sn_parsed.txt
#cat sn_parsed_intermediate/part-* > sn_parsed_intermediate.txt
cat sn_parsed_format_by_key/part-* > sn_parsed_format_by_key.txt
sed "s%^('\(.*\)', \(.*\))$%\2 \1%" sn_parsed.txt | sort -n -r > sn_parsed_massaged.txt
cat uniquenames_parsed/part-* > uniquenames_parsed.txt
