# CordisCrawlers



##### 1st script : `crawl_all_ids.py`

This file is a script which uses the json api of Cordis to extract all project metadata. 
It should run once to collect all the petadata which are then stored in a file named "all_projids_data.json"

##### 2nd script : `crawl_by_project_id.py`

This file is a script which uses the json API of Cordis and the json API of OpenAIRE to collect all the data of a project and store them in a mongoDB.
The script should get the "all_projids_data.json" file (extracted from 1st script) as input.


##### 3rd script : `collect_all_pic_info.py`

This file traverses through all collected projects' data and uses the json API of CORDIS to collect all metadata about participants and coordinators of each project.

![This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No. 101004870. H2020-SC6-GOVERNANCE-2018-2019-2020 / H2020-SC6-GOVERNANCE-2020](https://github.com/IntelCompH2020/.github/blob/main/profile/banner.png)

