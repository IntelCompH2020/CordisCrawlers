
import pymongo, pickle, traceback, time, sys, re, requests
from pprint import pprint
import urllib.request, json
from tqdm import tqdm
import argparse
from json import loads

def get_the_json(link):
    error_counter = 0
    for i in range(10):
        try:
            with urllib.request.urlopen(link) as url:
                page_data = json.loads(url.read().decode())
                return page_data
        except Exception as e:
            error_counter += 1
            if(error_counter>=5):
                # print(link)
                # traceback.print_exc()
                # tb = traceback.format_exc()
                # print(tb)
                return None
            else:
                time.sleep(2)

def get_clean_j(response):
    as_text = response.text.strip()
    as_text = as_text.replace(u'$', u'_$')
    as_text = as_text.replace('\r\n', ' ')
    as_text = as_text.replace('\n', ' ')
    # as_text     = as_text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
    for i in range(5):
        as_text = re.sub(r" 0(\d)", r" \1", as_text)
    as_text = re.sub(r" :\s+(\d+)\.\s+}", r" : \1.0 }", as_text)
    as_text = as_text.replace(': 266.e10 }', ': "266.e10" }')
    as_text = as_text.replace(': 266.e1 }', ': "266.e1" }')
    as_text = as_text.replace('"NTU "KhPI" Bulletin: Power and heat engineering processes and equipment"' , '"NTU \\"KhPI\\" Bulletin: Power and heat engineering processes and equipment"')
    return as_text

def get_the_json_2(link):
    for i in range(10):
        as_text = ''
        try:
            response    = requests.get(link, verify=False, timeout=2*60)
            as_text     = get_clean_j(response)
            return loads(as_text)
            # return json.loads(as_text)
        except:
            if(i>=5):
                print(as_text)
                raise Exception('Exceeded 5 times trying for link: {}'.format(link))
            else:
                time.sleep(2)

################################################################################################

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

parser              = argparse.ArgumentParser()
parser.add_argument("--mongo_ip",         type=str, default='127.0.0.1',             help="mongo ip",                                                  required=False)
parser.add_argument("--mongo_port",       type=int, default=27017,  nargs="?",       help="mongo port",                                                required=False)
parser.add_argument("--mongo_dbname",     type=str, default='cordis_Feb_2022_ALL',   help="mongo database name",                                       required=True)
parser.add_argument("--project_ids_path", type=str, default='all_projids_data.json', help="The path for project ids",                                  required=True)
parser.add_argument("--recreate_index",   type=str, default='False',                 help="Should i recreate the index? This will delete everything.", required=True)
parser.add_argument("--ind_from",         type=int, default=0,                       help="", required=False)
parser.add_argument("--ind_to",           type=int, default=-1,                      help="", required=False)

################################################################################################

args                = parser.parse_args()
mongo_ip            = args.mongo_ip
mongo_dbname        = args.mongo_dbname
mongo_port          = args.mongo_port
project_ids_path    = args.project_ids_path
recreate_index      = str2bool(args.recreate_index)
ind_from            = args.ind_from
ind_to              = args.ind_to

################################################################################################

all_projids_data    = json.load(open(project_ids_path))
all_projids_data    = sorted(all_projids_data, key= lambda x : int(x['rcn']))
if ind_to == -1:
    all_projids_data    = all_projids_data[ind_from:]
else:
    all_projids_data    = all_projids_data[ind_from:ind_to]

################################################################################################

client              = pymongo.MongoClient(host=mongo_ip,port=mongo_port)
dbname              = mongo_dbname
################################################ We
cordis_projects     = client[dbname]['projects_0_1']
################################################
cordis_programs     = client[dbname]['programmes_0_1']
################################################
cordis_reports      = client[dbname]['reports_0_1']
################################################
cordis_results      = client[dbname]['results_0_1']
################################################
cordis_news         = client[dbname]['news_0_1']
cordis_rest_news    = client[dbname]['rest_news_0_1']
################################################
cordis_events       = client[dbname]['events_0_1']
################################################
cordis_openaire     = client[dbname]['openaire_original_api_0_1']
################################################
if(recreate_index):
    ################################################
    cordis_news.drop()
    cordis_results.drop()
    cordis_projects.drop()
    cordis_programs.drop()
    cordis_reports.drop()
    cordis_events.drop()
    cordis_openaire.drop()
    ################################################
    cordis_projects.create_index("information.id", unique=True)
    cordis_projects.create_index("information.rcn", unique=True)
    cordis_programs.create_index("rcn", unique=True)
    cordis_reports.create_index("rcn", unique=True)
    cordis_results.create_index("rcn", unique=True)
    cordis_news.create_index("rcn", unique=True)
    cordis_events.create_index("rcn", unique=True)
    cordis_openaire.create_index("project_id", unique=True)
    ################################################
################################################

skip_links          = []
these_were_faulty   = []
pbar                = tqdm(all_projids_data)
for item in pbar:
    ####################################################################
    # link        = 'https://cordis.europa.eu/api/details?contenttype=project&rcn={}&lang=en'.format(project_rcn)
    project_rcn     = item['rcn']
    # link            = 'https://cordis.europa.eu/api/details?contenttype=project&rcn={}&lang=en&paramType=id'.format(project_rcn)
    link            = 'https://cordis.europa.eu/api/details?contenttype=project&rcn={}&lang=en&paramType=rcn'.format(project_rcn)
    # print(link)
    proj_data       = get_the_json(link)
    if(proj_data is None):
        print(link)
        continue
    else:
        proj_data   = proj_data['payload']
    try:
        cordis_projects.insert_one(proj_data)
    except pymongo.errors.DuplicateKeyError:
        pbar.set_description('Found proj_data')
    except:
        print(20*'-')
        pprint(proj_data)
        print(20*'-')
    ####################################################################
    for item in proj_data['information']['fundedUnder']:
        program_rcn = item['rcn']
        linki = 'https://cordis.europa.eu/api/details?contenttype=programme&rcn={}&lang=en'.format(program_rcn)
        prog_data = get_the_json(linki)
        if(prog_data is None):
            continue
        else:
            prog_data = prog_data['payload']
        try:
            cordis_programs.insert_one(prog_data)
        except pymongo.errors.DuplicateKeyError:
            pbar.set_description('Found fundedUnder programme: {}'.format(program_rcn))
    ####################################################################
    # REPORTS
    for item in proj_data['information']['relatedResultsReport']:
        report_rcn = item['rcn']
        report_data = get_the_json('https://cordis.europa.eu/api/details?contenttype=result&rcn={}&lang=en'.format(report_rcn))
        if(report_data is None):
            continue
        else:
            report_data = report_data['payload']
        try:
            cordis_reports.insert_one(report_data)
        except pymongo.errors.DuplicateKeyError:
            pbar.set_description('Found relatedResultsReport result')
    ####################################################################
    # RESULTS
    for item in proj_data['information']['relatedResultsInBrief']:
        try:
            results_rcn = item['rcn']
        except:
            results_rcn = item
        results_data = get_the_json('https://cordis.europa.eu/api/details?contenttype=result&rcn={}&lang=en'.format(results_rcn))
        if(results_data is None):
            continue
        else:
            results_data = results_data['payload']
        try:
            cordis_results.insert_one(results_data)
        except pymongo.errors.DuplicateKeyError:
            pbar.set_description('Found relatedResultsInBrief result')
    ####################################################################
    # TOPICS
    for item in proj_data['objective']['topics']:
        program_rcn = item['rcn']
        prog_data = get_the_json('https://cordis.europa.eu/api/details?contenttype=programme&rcn={}&lang=en'.format(program_rcn))
        if(prog_data is None):
            continue
        else:
            prog_data = prog_data['payload']
        try:
            cordis_programs.insert_one(prog_data)
        except pymongo.errors.DuplicateKeyError:
            pbar.set_description('Found topics programme')
    ####################################################################
    # NEWS
    for item in proj_data['relatedContent']['news']+proj_data['relatedContent']['externalNews']:
        try:
            news_rcn            = item['rcn']
            news_data           = get_the_json('https://cordis.europa.eu/api/details?contenttype=news&rcn={}&lang=en'.format(news_rcn))
            if(news_data is None):
                continue
            else:
                news_data       = news_data['payload']
            news_data['rcn']    = news_rcn
            try:
                cordis_news.insert_one(news_data)
            except pymongo.errors.DuplicateKeyError:
                pbar.set_description('Found news')
        except:
            print('NEWS:')
            item['ralated_project_rcn'] = project_rcn
            pprint(item)
            cordis_rest_news.insert_one(item)
            print(20 *'-')
            # news_rcn = item
    ####################################################################
    # EVENTS
    for item in proj_data['relatedContent']['events']:
        event_rcn    = item['rcn']
        event_data   = get_the_json('https://cordis.europa.eu/api/details?contenttype=event&rcn={}&lang=en'.format(event_rcn))
        if(event_data is None):
            continue
        else:
            event_data = event_data['payload']
        event_data['rcn'] = event_rcn
        try:
            cordis_events.insert_one(event_data)
        except pymongo.errors.DuplicateKeyError:
            pass
    ####################################################################
    # project_id = project_rcn
    project_id = proj_data['information']['id']
    if(cordis_openaire.find_one({'project_id':project_id}) is None):
        link = 'http://api.openaire.eu/search/publications?format=json&projectID={}&size=100'.format(project_id)
        if(link in skip_links):
            continue
        try:
            #################################################################
            openaire_data       = get_the_json_2(link)
            if(openaire_data is None):
                continue
            else:
                openaire_data   = openaire_data['response']
            openaire_data['project_id'] = project_id
            cordis_openaire.insert_one(openaire_data)
            #################################################################
        except pymongo.errors.DuplicateKeyError:
            print('Found : {}'.format(project_id))
        except:
            these_were_faulty.append(link)
            print(link)
            print(traceback.format_exc())

# ---------------------------------------------------------------------------------------

'''
/home/dpappas/MONGO/mongodb-linux-x86_64-ubuntu1604-4.4.6//bin/mongod \
--dbpath   /home/dpappas/MONGO/mongodb-linux-x86_64-ubuntu1604-4.4.6/data \
--logpath  /home/dpappas/MONGO/mongodb-linux-x86_64-ubuntu1604-4.4.6/logs

python3.6 crawl_by_project_id.py \
--recreate_index=True \
--mongo_dbname=cordis_Feb_2022_ALL \
--project_ids_path=/home/dpappas/all_projids_data.json \
--ind_from=0 \
--ind_to=30000

python3.6 crawl_by_project_id.py \
--recreate_index=False \
--mongo_dbname=cordis_Feb_2022_ALL \
--project_ids_path=/home/dpappas/all_projids_data.json \
--ind_from=30000 \
--ind_to=60000

python3.6 crawl_by_project_id.py \
--recreate_index=False \
--mongo_dbname=cordis_Feb_2022_ALL \
--project_ids_path=/home/dpappas/all_projids_data.json \
--ind_from=60000 \
--ind_to=90000

python3.6 crawl_by_project_id.py \
--recreate_index=False \
--mongo_dbname=cordis_Feb_2022_ALL \
--project_ids_path=/home/dpappas/all_projids_data.json \
--ind_from=90000 \
--ind_to=140000

# 132305

'''


