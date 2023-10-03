#!/usr/bin/env bash

cat url_list.txt | parallel -j8 wget --show-progress {}

unzip cordis-HORIZONprojects-json.zip
unzip cordis-HORIZONprojects-xlsx.zip
unzip cordis-HORIZONprojectDeliverables-json.zip
unzip cordis-HORIZONprojectDeliverables-xlsx.zip
unzip cordis-HORIZONreports-json.zip
unzip cordis-HORIZONreports-xlsx.zip
unzip cordis-HORIZONprojectPublications-xlsx.zip


