#!/usr/bin/env bash
  java -jar target/oxo1-sssom2neo-1.0-SNAPSHOT.jar \
      --input $INPUT_DIR \
      --ols-url $1 \
      --output-datasources $OUTPUT_DIR/datasources.csv \
      --output-edges $OUTPUT_DIR/ols_mappings.csv \
      --output-nodes $OUTPUT_DIR/ols_terms.csv
