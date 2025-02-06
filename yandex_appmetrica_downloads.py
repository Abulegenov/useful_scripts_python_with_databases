import requests
import time
from datetime import date, timedelta


proxies = {'https':'your_proxy'}
column_dict = {
    'clicks':'application_id,publisher_name,publisher_id,tracker_name,tracking_id,click_timestamp,click_datetime,click_ipv6,'+
                    'click_url_parameters,click_id,click_user_agent,ios_ifa,android_id,google_aid,os_name,os_version,'+ 
                    'device_manufacturer,device_model,device_type,is_bot,country_iso_code,city',
    'installations':'application_id,publisher_name,publisher_id,tracker_name,tracking_id,click_timestamp,click_datetime,click_ipv6,'+
                    'click_url_parameters,click_id,click_user_agent,match_type,install_datetime,install_timestamp,install_receive_datetime,'+
                    'install_receive_timestamp,install_ipv6,is_reinstallation,ios_ifa,android_id,google_aid,profile_id,os_name,os_version,'+ 
                    'device_manufacturer,device_model,device_type,device_locale,app_version_name,app_package_name,connection_type,operator_name,mcc,mnc,country_iso_code,city,appmetrica_device_id',
    'events':'appmetrica_device_id,event_name,event_datetime,event_timestamp,operator_name,event_json,city,connection_type,app_version_name,device_locale,device_manufacturer,device_model,os_version',
}

def get_data(date_from, date_to, data_type):
    """Get the information about installations/clicks/events of your app"""
    date_from = str(date_from) + ' 00:00:00'
    date_to = str(date_to) + ' 23:59:59' 
    data_ext='csv'   
    payloads = {
        'application_id': int('your app id'),
        'date_since': date_from,
        'date_until': date_to,
        'date_dimension':'default',
        'use_utf8_bom':'true',
        'fields': column_dict[data_type],
        'oauth_token': 'your_auth_token',
        }
    base_url = (f'''https://api.appmetrica.yandex.ru/logs/v1/export/{data_type}.{data_ext}?''')
    new_payloads = []
    for key in payloads:
        new_payloads.append(str(key) + '=' + str(payloads[key]))
    payloads_str = '&'.join(new_payloads)
    req_url = base_url + payloads_str
    print("WAITING FOR URL...")
    print(req_url)
    res = requests.get(req_url,proxies=proxies,stream=True)
    while res.status_code != 200:
        time.sleep(3)
        res = requests.get(req_url,proxies=proxies,stream=True)
        print("Waiting for status code 200")
    filename = 'installations.csv'
    with open(filename, 'wb') as f:
        f.write(res.content)
    print("URL DOWNLOADED")
    print(filename)
    return filename


#example usage:
for key in column_dict.keys:
    to_date = date.today()
    from_date = to_date - timedelta(2)
    get_data(from_date, to_date, key)