import os
import json
import datetime, time

import pandas as pd


def list_files(dir):
    r = []
    subdirs = [x[0] for x in os.walk(dir)]
    for subdir in subdirs:
        files = os.walk(subdir).__next__()[2]
        if (len(files) > 0):
            for file in files:
                r.append(os.path.join(subdir, file))
    return r


directory = "C:\\Users\\jiri.pillar\\Downloads\\06\\06"
files = list_files(directory)
del files[0:6]
for i in files:
    print(i)

iotList = []
for i in files:
    with open(i) as f:
        for jsonObj in f:
            iotDict = json.loads(jsonObj)
            iotList.append(iotDict)




############################################################################################################

# import pandas as pd
#
#
# df = pd.read_json("C:\\Users\\jiri.pillar\\Downloads\\06\\18\\00\\49.json", lines=True)
# df = pd.concat([df1,df2])
#
#
# df = pd.DataFrame()
# df_con = pd.DataFrame()
# for i in files:
#     df = pd.read_json(i, lines=True)
#     df_con=pd.concat([df_con,df])
#
# ##takes one file and adds each line to the list
# iotList = []
# with open('C:\\Users\\jiri.pillar\\Downloads\\06\\18\\00\\49.json') as f:
#     for jsonObj in f:
#         iotDict = json.loads(jsonObj)
#         iotList.append(iotDict)


# convert-unix-timestamp-to-str-and-str-to-unix-timestamp-in-python
import datetime, time

def ts2string(ts, fmt="%Y-%m-%d %H:%M:%S.%f"):
    dt = datetime.datetime.fromtimestamp(ts)
    return dt.strftime(fmt)

def string2ts(string, fmt="%Y-%m-%d %H:%M:%S.%f"):
    dt = datetime.datetime.strptime(string, fmt)
    t_tuple = dt.timetuple()
    return int(time.mktime(t_tuple))

def test():
    ts = 1655452140000/ 1000.0
#    ts = iotList[0]['Body']['temperature'][0]['timestamp'] / 1000
    datetime_obj = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S.%f')
    print(datetime_obj)

    string = ts2string(ts)
    print(string)

    ts = string2ts(string)
    print(ts)


# python-replace-values-in-nested-dictionary
d = {'id': '10', 'datastreams': [{'current_value': '5'}, {'current_value': '4'}]}

for elem in d['datastreams']:      # for each elem in the list datastreams
    for k,v in elem.items():       # for key,val in the elem of the list
        if 'current_value' in k:   # if current_value is in the key
            elem[k] = int(v)       # Cast it to int
print(d)




# The following piece of code replaces (substrings of) values in a dictionary. It works for nested json structures and copes with json, list and string types. You can add other types if needed.

def dict_replace_value(d, old, new):
    x = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = dict_replace_value(v, old, new)
        elif isinstance(v, list):
            v = list_replace_value(v, old, new)
        elif isinstance(v, str):
            v = v.replace(old, new)
        x[k] = v
    return x


def list_replace_value(l, old, new):
    x = []
    for e in l:
        if isinstance(e, list):
            e = list_replace_value(e, old, new)
        elif isinstance(e, dict):
            e = dict_replace_value(e, old, new)
        elif isinstance(e, str):
            e = e.replace(old, new)
        x.append(e)
    return x

# See input and output below
b = dict_replace_value(a, 'string', 'something')




# Replaces INT values for temperature epoch timestamp with datetime STR
import datetime, time
for i, key in enumerate(iotList):
    for elem in iotList[i]['Body']['temperature']:      # for each elem in the list datastreams
        for k,v in elem.items():       # for key,val in the elem of the list
            if 'timestamp' in k:   # if current_value is in the key
                elem[k] = datetime.datetime.fromtimestamp(elem[k]/1000).strftime('%Y-%m-%d %H:%M:%S.%f')  # Cast it to datetime STR



# Splitting a list of dictionaries into several lists of dictionaries
import collections
result = collections.defaultdict(list)
for d in iotList:
    result[d['Body']['imsi']].append(d)
result_list = list(result.values())

# OR

grouping = {}
for d in iotList:
    if d['Body']['imsi'] not in grouping:
        grouping[d['Body']['imsi']] = []
    grouping[d['Body']['imsi']].append(d)
result = list(result.values())


for i, key in enumerate(result):
    device=json.dumps(result[i])





# GENERAL temperature gathering
devices_temperature=[]
for r, dev in enumerate(result):
    for i, key in enumerate(result[r]):
        devices_temperature.append(result[r][i]['Body']['temperature'])

devices_humidity=[]
for r, dev in enumerate(result):
    for i, key in enumerate(result[r]):
        devices_humidity.append(result[r][i]['Body']['humidity'])


#TODO adjust for device specifics
# TODO API = https://api.powerbi.com/beta/2b755fa1-23d1-48f3-98fc-6fdc1dc48d69/datasets/40468dfb-6511-43d9-b65b-33a68e9e539b/rows?key=QKb0gD3oCVkmK1WZa3PeKOM1QSoIJvc1GWY3qfjzVxSoXZ2yaeyJgOrnm2%2Be1Hq1ZoS5K9WwBdTCdAGiV46u%2FQ%3D%3D

device_901288002989991_temperature=[]
for i, key in enumerate(result_list[0]):
    device_901288002989991_temperature.append(result_list[0][i]['Body']['temperature'])

# TODO API = https://api.powerbi.com/beta/2b755fa1-23d1-48f3-98fc-6fdc1dc48d69/datasets/0ffcb66c-d97a-4fa4-beb9-3ae5c8d41440/rows?key=PvdAF9WSqPywOnZaj%2Fhj2vddgbJBCWNmhJN%2BQHhp44bkiRNCpCUzJGhjA3byciUpwZUqc%2BVDth%2BgQbeuPhPdew%3D%3D

device_901288002989997_temperature=[]
for i, key in enumerate(result_list[1]):
    device_901288002989997_temperature.append(result_list[1][i]['Body']['temperature'])



#TODO adjust for device specifics
# TODO API = https://api.powerbi.com/beta/2b755fa1-23d1-48f3-98fc-6fdc1dc48d69/datasets/62f4baa4-c7ba-4372-ac3c-687216b61dc3/rows?key=XWcR5KXE%2FO8R0%2FpYdd2qzh6YBm0PjwliuoRkTo2u8kKM%2FYelqPPUFpnxTk%2FhJbu4mknJcJ001Q3Y6cZCIRCayQ%3D%3D

device_901288002989991_humidity=[]
for i, key in enumerate(result_list[0]):
    device_901288002989991_humidity.append(result_list[0][i]['Body']['humidity'])

# TODO API = https://api.powerbi.com/beta/2b755fa1-23d1-48f3-98fc-6fdc1dc48d69/datasets/1fb13ce5-c47c-4b7c-9232-a56e87a110d6/rows?key=YYuwjHLKUvNki4qkkDleU7M%2F3%2Bgbgoo6m3r%2Brn93dhJ%2FK13X1KWEk2j1BjOqVRDsrBIh0ja581kApxWi0KofrA%3D%3D

device_901288002989997_humidity=[]
for i, key in enumerate(result_list[1]):
    device_901288002989997_humidity.append(result_list[1][i]['Body']['humidity'])

#TODO adjust for device specifics/general
#TODO add more devices

from itertools import chain
device_901288002989991_temperature=list(chain.from_iterable(device_901288002989991_temperature))
with open("device_901288002989991_temperature.json", "w") as outfile:
    json.dump(device_901288002989991_temperature, outfile)


###List the blobs in your container

from azure.storage.blob import ContainerClient

container = ContainerClient.from_connection_string(conn_str='DefaultEndpointsProtocol=https;AccountName=sttemperaturetestweu001;AccountKey=iCAkFc6T8XcqyP23FRaT8Nd9mjrgwVUsfxP1IpJa1CfDRnAm3SpDI68nrXFAFmd5sPk852KlNf42+AStRg8PYw==;EndpointSuffix=core.windows.net', container_name = 'ci-temperature-test-weu-001')
for blob in container.list_blobs():
    print(f'{blob.name} : {blob.last_modified}')


###Download a blob from your container

from azure.storage.blob import BlobClient

blob = BlobClient.from_connection_string(conn_str='DefaultEndpointsProtocol=https;AccountName=sttemperaturetestweu001;AccountKey=iCAkFc6T8XcqyP23FRaT8Nd9mjrgwVUsfxP1IpJa1CfDRnAm3SpDI68nrXFAFmd5sPk852KlNf42+AStRg8PYw==;EndpointSuffix=core.windows.net', container_name = 'ci-temperature-test-weu-001', blob_name="iot-temperature-test-weu-001/01/2022/07/01/11/49.json")
with open("./BlockDestination.txt", "wb") as my_blob:
    blob_data = blob.download_blob()
    blob_data.readinto(my_blob)
