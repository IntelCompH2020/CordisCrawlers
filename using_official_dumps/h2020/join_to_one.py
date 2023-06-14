
import pandas as pd
from openpyxl import load_workbook
from tqdm import tqdm

# def load_xlsx_dato(my_path):
#     return pd.read_excel(my_path).fillna('N/A').to_dict('records')

def load_data_from_excel(xlsx_fpath):
    wb = load_workbook(xlsx_fpath)
    xl_data = {}
    for sheet_name in wb.sheetnames:
        temp_data = []
        sheet = wb[sheet_name]
        for row in tqdm(sheet.iter_rows()):
            temp_data.append([str(item.value).strip() if item.value else None for item in row])
        xl_data[sheet_name] = temp_data
    ######################
    return xl_data

while(True):
    f1 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/euroSciVoc.xlsx')['euroSciVoc']
    print(f1[0])
    f2 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/legalBasis.xlsx')['legalBasis']
    print(f2[0])
    f3 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/organization.xlsx')['organization']
    print(f3[0])
    f4 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/projectDeliverables.xlsx')['projectDeliverables']
    print(f4[0])
    f5 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/projectPublications.xlsx')['']
    print(f5[0])
    f6 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/project.xlsx')['']
    print(f6[0])
    f7 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/reportSummaries.xlsx')['']
    print(f7[0])
    f8 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/topics.xlsx')['']
    print(f8[0])
    f9 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/webItem.xlsx')['']
    print(f9[0])
    f10 = load_data_from_excel('/media/disk1/dpappas_data/cordis_dumps/h2020/xlsx/webLink.xlsx')['']
    print(f10[0])
    break






