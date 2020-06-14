#!/bin/bash
mkdir -p data/raw
mkdir -p data/processed
curl "https://opendata.arcgis.com/datasets/0ef47379cbae44e88267c01eaec2ff6e_31.geojson" --output data/raw/wards.json
curl "https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments/Arrests%20by%20Year%2C%202019.csv" --output data/raw/Arrests2019.csv
curl "https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments/Arrests%20by%20Year%2C%202018.csv" --output data/raw/Arrests2018.csv
curl "https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments/Arrests%202017%20Public.csv" --output data/raw/Arrests2017.csv
curl "https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments/Arrests%202016%20Public.csv" --output data/raw/Arrests2016.csv
curl "https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments/Arrests%202015%20Public.csv" --output data/raw/Arrests2015.csv
curl "https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments/Arrests%202014%20Public.csv" --output data/raw/Arrests2014.csv
curl "https://mpdc.dc.gov/sites/default/files/dc/sites/mpdc/publication/attachments/Arrests%202013%20Public.csv" --output data/raw/Arrests2013.csv
