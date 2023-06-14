#!/usr/bin/env bash

cat url_list.txt | parallel -j8 wget --show-progress {}

unzip cordis-h2020projectPublications-json.zip
unzip cordis-h2020projects-json.zip
unzip cordis-h2020reports-json.zip
unzip cordis-h2020projectDeliverables-json.zip
unzip cordis-h2020projectDeliverables-xlsx.zip
unzip cordis-h2020projectPublications-xlsx.zip
unzip cordis-h2020projects-xlsx.zip
unzip cordis-h2020reports-xlsx.zip

