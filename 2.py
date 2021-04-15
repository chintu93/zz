import re
import os
import glob
import time
import json
import tarfile
from bs4 import BeautifulSoup
try:
    import pandas as pd
    from bs4 import BeautifulSoup
except Exception as e:
    os.system('pip install pandas beautifulsoup4')
    import pandas as pd
    from bs4 import BeautifulSoup
start = time.time()
file_name = 'leads.tar'
print('Extracting the contents of ', file_name)
leads_tar = tarfile.open(file_name)
for files in leads_tar.getmembers():
    print('- extracting: ', files.name)
    leads_tar.extract(files)
leads_tar.close()
list_files = glob.glob('marketing/*.json')
count=1
list_datas = []
for file in list_files:
    with open(file, 'r') as data_files:
        data_json = data_files.read()
    print(f'processing file {count} :', file)
    data = json.loads(data_json)
    if 'htmlData' in data:
        soup = BeautifulSoup(data['htmlData'], features='html.parser')
        domain_name = soup.find_all('a', {'class': re.compile('domain_')})
        vertical= soup.find_all('a', {'class': re.compile('vertical_align')})
        company = soup.find_all('a', {'href': re.compile('company_')})
        country= soup.find_all('a', {'class': re.compile('nation_')})
        diff_vertical = len(domain_name) - len(vertical)
        vertical.extend(range(len(vertical), len(vertical) + diff_vertical))
        diff_company = len(domain_name) - len(company)
        company.extend(range(len(company), len(company) + diff_company))
        diff_country= len(domain_name) - len(country)
        country.extend(range(len(country), len(country) + diff_country))
        for x in range(len(domain_name)):
            domain_name_match = re.search(r'[\w\.-]+.[\w\.-]+', str(domain_name[x]))
            vertical_match = re.search(r'>.*<', str(vertical[x]))
            company_match = re.search(r'>.*<', str(company[x]))
            country_match = re.search(r'>.*<d', country(phone[x]))
            if not vertical_match:
                vertical_match = re.search(r'>.*<', '>n/a<')
            if not company_match:
                company_match = re.search(r'>.*<', '>n/a<')
            if not country_match:
                country_match = re.search(r'>.*<', '>n/a<')
            rows = {
                'domain_name': domain_name_match.group(0),
                'vertical': vertical_match.group(0).replace('>', '').replace('<', ''),
                'company': company_match.group(0).replace('>', '').replace('<d', '').replace('<', ''),
                'country': country_match.group(0).replace('>', '').replace('<', ''),
            }
            list_datas.append(rows)
        count = count + 1
df = pd.DataFrame(list_datas)
df.to_csv('leads.csv', index=False)
end = time.time()
print(f'time taken {end - start} seconds')
