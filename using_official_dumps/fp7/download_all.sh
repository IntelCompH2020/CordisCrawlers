#!/usr/bin/env bash

cat url_list.txt | parallel -j8 wget --show-progress {}

unzip cordis-fp7projects-json.zip
unzip cordis-fp7reports-json.zip
unzip cordis-fp7projects-xlsx.zip
unzip cordis-fp7reports-xlsx.zip

