#!/usr/bin/env bash

pip3 install -r requirements.txt

scp $OUTPUT_DIR/datasources.csv $NEO4J_HOME/import/
python3 OxoNeo4jLoader.py -W -d datasources.csv -c config.ini

scp $OUTPUT_DIR/ols_terms.csv $NEO4J_HOME/import
scp $OUTPUT_DIR/ols_mappings.csv $NEO4J_HOME/import
python3 OxoNeo4jLoader.py -c config.ini -t ols_terms.csv -m ols_mappings.csv