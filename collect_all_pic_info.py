
from pprint import pprint
from tqdm import tqdm
import pymongo, traceback, time, json, sys, argparse
import urllib.request
import mechanize
try:
    from http.cookiejar import LWPCookieJar
except ImportError:
    from cookielib import LWPCookieJar

def get_br():
    # Browser
    br = mechanize.Browser()
    # Cookie Jar
    cj = LWPCookieJar()
    br.set_cookiejar(cj)
    # Browser options
    br.set_handle_equiv(True)
    br.set_handle_gzip(True)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.set_handle_robots(False)
    # Follows refresh 0 but not hangs on refresh > 0
    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)
    # curl 'https://www.zomato.com/praha/toms-burger-restaurant-vinohrady-praha-2' -H 'Host: www.zomato.com' -H 'User-Agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:57.0) Gecko/20100101 Firefox/57.0' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' -H 'Accept-Language: en-US,en;q=0.5' --compressed -H 'Connection: keep-alive' -H 'Upgrade-Insecure-Requests: 1' -H 'Cache-Control: max-age=0'
    br.addheaders = [
        (
            'User-agent',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:58.0) Gecko/20100101 Firefox/58.0'
        ),
        ( 'Accept', '*/*' ),
        ( 'Host', 'cordis.europa.eu' ),
        ( 'Accept-Language', 'en-US,en;q=0.5' ),
        ( 'Accept-Encoding', 'gzip, deflate' )
    ]
    return br

def get_json_mechanize(link):
    for i in range(10):
        try:
            br          = get_br()
            page_data   = json.loads(br.open(link, timeout=50.0).read().decode())
            return page_data
        except:
            if(i>=5):
                raise Exception('Exceeded 5 times trying for link: {}'.format(link))
            else:
                time.sleep(2)

def get_the_json(link):
    for i in range(10):
        try:
            with urllib.request.urlopen(link, timeout=50) as url:
                page_data = json.loads(url.read().decode())
                return page_data
        except:
            if(i>=5):
                raise Exception('Exceeded 5 times trying for link: {}'.format(link))
                # print(link)
                # traceback.print_exc()
                # tb = traceback.format_exc()
                # print(tb)
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
parser.add_argument("--mongo_ip",           type=str, default='127.0.0.1',              help="mongo ip",                                                    required=True)
parser.add_argument("--mongo_port",         type=int, default=27017,  nargs="?",        help="mongo port",                                                  required=True)
parser.add_argument("--mongo_dbname",       type=str, default='cordis_May_2021_ALL',    help="mongo database name",                                         required=True)
parser.add_argument("--recreate_index",     type=str, default='False',                  help="Should i recreate the index? This will delete everything.",   required=True)
parser.add_argument("--skip",               type=int, default=0,                        help="How many should mongo skip",                                  required=False)
parser.add_argument("--limit",              type=int, default=1000000,                  help="The limit of mongo db",                                       required=False)

################################################################################################

args                = parser.parse_args()
mongo_ip            = args.mongo_ip
mongo_dbname        = args.mongo_dbname
mongo_port          = args.mongo_port
recreate_index      = str2bool(args.recreate_index)
skip                = args.skip
limit               = args.limit

################################################################################################

client          = pymongo.MongoClient(host=mongo_ip, port=mongo_port)
################################################
cordis_projects = client[mongo_dbname]['projects_0_1']
cordis_orgs     = client[mongo_dbname]['organizations_0_1']
if recreate_index:
    cordis_orgs.drop()
    cordis_orgs.create_index("publicOrganizationData.pic", unique=True)

# example link : https://ec.europa.eu/info/funding-tenders/opportunities/api/orgProfile/data.json?pic=999997833

for cp in tqdm(cordis_projects.find().skip(skip).limit(limit), total=limit, smoothing=1):
    if('coordinator' in cp['organizations']):
        for part in cp['organizations']['coordinator']:
            link = 'https://ec.europa.eu/info/funding-tenders/opportunities/api/orgProfile/data.json?pic={}'.format(part['organizationId'])
            if(cordis_orgs.find_one({'publicOrganizationData.pic': part['organizationId']}) is None):
                try:
                    part_data = get_the_json(link)['organizationProfile']
                    cordis_orgs.insert_one(part_data)
                except:
                    try:
                        part_data = get_json_mechanize(link)['organizationProfile']
                        cordis_orgs.insert_one(part_data)
                    except:
                        print(link)
    if('coordinators' in cp['organizations']):
        for part in cp['organizations']['coordinators']:
            link = 'https://ec.europa.eu/info/funding-tenders/opportunities/api/orgProfile/data.json?pic={}'.format(part['organizationId'])
            if(cordis_orgs.find_one({'publicOrganizationData.pic': part['organizationId']}) is None):
                try:
                    part_data = get_the_json(link)['organizationProfile']
                    cordis_orgs.insert_one(part_data)
                except:
                    try:
                        part_data = get_json_mechanize(link)['organizationProfile']
                        cordis_orgs.insert_one(part_data)
                    except:
                        print(link)
    if('participants' in cp['organizations']):
        for part in cp['organizations']['participants']:
            link = 'https://ec.europa.eu/info/funding-tenders/opportunities/api/orgProfile/data.json?pic={}'.format(part['organizationId'])
            if(cordis_orgs.find_one({'publicOrganizationData.pic': part['organizationId']}) is None):
                try:
                    part_data = get_the_json(link)['organizationProfile']
                    cordis_orgs.insert_one(part_data)
                except:
                    try:
                        part_data = get_json_mechanize(link)['organizationProfile']
                        cordis_orgs.insert_one(part_data)
                    except:
                        print(link)
    if('partners' in cp['organizations']):
        for part in cp['organizations']['partners']:
            link = 'https://ec.europa.eu/info/funding-tenders/opportunities/api/orgProfile/data.json?pic={}'.format(part['organizationId'])
            if(cordis_orgs.find_one({'publicOrganizationData.pic': part['organizationId']}) is None):
                try:
                    part_data = get_the_json(link)['organizationProfile']
                    cordis_orgs.insert_one(part_data)
                except:
                    try:
                        part_data = get_json_mechanize(link)['organizationProfile']
                        cordis_orgs.insert_one(part_data)
                    except:
                        print(link)


