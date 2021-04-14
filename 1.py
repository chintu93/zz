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
list_emails = []
for file in list_files:
    with open(file, 'r') as data_files:
        data_json = data_files.read()
    print(f'processing file {count} :', file)
    data = json.loads(data_json)
    if 'htmlData' in data:
        soup = BeautifulSoup(data['htmlData'], features='html.parser')
        email = soup.find_all('a', {'class': re.compile('compose-mail-box')})
        name = soup.find_all('a', {'id': re.compile('emp_name_')})
        phone = soup.find_all('img',
                              {'class': re.compile('phone_img_responsive')})
        company = soup.find_all('a', {'href': re.compile('company_')})
        
        diff_phone = len(email) - len(phone)
        phone.extend(range(len(phone), len(phone) + diff_phone))
        diff_name = len(email) - len(name)
        name.extend(range(len(name), len(name) + diff_name))
        diff_company = len(email) - len(company)
        company.extend(range(len(company), len(company) + diff_company))
        for x in range(len(email)):
            email_match = re.search(r'[\w\.-]+@[\w\.-]+', str(email[x]))
            company_match = re.search(r'>.*<', str(company[x]))
            name_match = re.search(r'>.*<', str(name[x]))
            phone_match = re.search(r'>.*<d', str(phone[x]))
            if not phone_match:
                phone_match = re.search(r'>.*<', '>n/a<')
            if not name_match:
                name_match = re.search(r'>.*<', '>n/a<')
            if not company_match:
                company_match = re.search(r'>.*<', '>n/a<')
            rows = {
                'email': email_match.group(0),
                'name': name_match.group(0).replace('>', '').replace('<', ''),
                'phone': phone_match.group(0).replace('>', '').replace('<d', '').replace('<', ''),
                'company': company_match.group(0).replace('>', '').replace('<', ''),
            }
            list_emails.append(rows)
        count = count + 1
df = pd.DataFrame(list_emails)
df.to_csv('leads.csv', index=False)
end = time.time()
print(f'time taken {end - start} seconds')

