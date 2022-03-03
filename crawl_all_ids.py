
from pprint import pprint
from tqdm import tqdm
import urllib.request, json

total_pages = 1400

all_projids_data = []
for page in tqdm(range(1, total_pages)):
    # link = 'https://cordis.europa.eu/search/en?q=contenttype%3D%27project%27&p={}&num=100&srt=Relevance:decreasing'.format(page)
    # link = 'https://cordis.europa.eu/api/search/results?q=contenttype=%27project%27&p={}&num=100&srt=Relevance:decreasing'.format(page)
    link = 'https://cordis.europa.eu/api/search/results?q=contenttype=project&p={}&num=100'.format(page)
    with urllib.request.urlopen(link, timeout=50) as url:
        page_data = json.loads(url.read().decode())
    for item in page_data['payload']['results']:
        # d = {
        #     'acronym'            : item['acronym'],
        #     'rcn'                : item['rcn'],
        #     'Grant_agreement_ID' : item['reference']
        # }
        item['Grant_agreement_ID'] = item['reference']
        all_projids_data.append(item)

print(len(all_projids_data))
with open('all_projids_data.json', 'w') as fp:
    json.dump(all_projids_data, fp, indent=4, sort_keys=True)
    fp.close()

'''

python3.6 crawl_all_ids.py

'''
