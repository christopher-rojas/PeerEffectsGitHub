"""
Additional functions used to collect GitHub Archive data and insert into mongodb.
"""

import logging
import urllib.request
from urllib.request import urlopen

def pretty_string(ugly_datetime):
    # Convert string in standard datetime format to correct format
    # for use in GitHub archive URL.
    # Input: Datetime in YMDH format.
    # Output: Datetime string in YMDH format (except 1 digit for hours 0-9)
    temp = ugly_datetime.strftime('%Y%m%d%H')
    if ugly_datetime.hour<10:
        pretty_datetime = '-'.join([temp[:4],temp[4:6],temp[6:8],temp[9:]])
    else:
        pretty_datetime = '-'.join([temp[:4],temp[4:6],temp[6:8],temp[8:]])
    return pretty_datetime
    
def download_file(current_datetime):
    # Download the file from Github Archive.
    # Input: Current datetime
    url = 'http://data.githubarchive.org/' + pretty_string(current_datetime) + '.json.gz'
    file_name = url.split('/')[-1]
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}
       
    req = urllib.request.Request(url, headers=hdr) 
    u = urlopen(req)
    logging.info(u.getcode())
    if u.getcode() == 200:
        f = open(file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        logging.info("Downloading: %s Bytes: %s" % (file_name, file_size))    
        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break
            file_size_dl += len(buffer)
            f.write(buffer)
        f.close()
    else:
        logging.info('Unable to download file:' + str(u.getcode()))
    
    