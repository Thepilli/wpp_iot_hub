import os
import json
import datetime
import collections


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


def list_files(dir):
    r = []
    subdirs = [x[0] for x in os.walk(dir)]
    for subdir in subdirs:
        files = os.walk(subdir).__next__()[2]
        if (len(files) > 0):
            for file in files:
                r.append(os.path.join(subdir, file))
    return r


directory = "C:\\Users\\jiri.pillar\\Downloads\\2022"
files = list_files(directory)
del files[0:6]
print(files[-1])

iotList = []
for i in files:
    with open(i) as f:
        for jsonObj in f:
            iotDict = json.loads(jsonObj)
            iotList.append(iotDict)

for i, key in enumerate(iotList):
    for elem in iotList[i]['Body']['temperature']:  # for each elem in the list datastreams
        for k, v in elem.items():  # for key,val in the elem of the list
            if 'timestamp' in k:  # if current_value is in the key
                elem[k] = datetime.datetime.fromtimestamp(elem[k] / 1000).strftime(
                    '%Y-%m-%d %H:%M:%S.%f')  # Cast it to datetime STR

for i, key in enumerate(iotList):
    for elem in iotList[i]['Body']['humidity']:  # for each elem in the list datastreams
        for k, v in elem.items():  # for key,val in the elem of the list
            if 'timestamp' in k:  # if current_value is in the key
                elem[k] = datetime.datetime.fromtimestamp(elem[k] / 1000).strftime(
                    '%Y-%m-%d %H:%M:%S.%f')  # Cast it to datetime STR

result = collections.defaultdict(list)
for d in iotList:
    result[d['Body']['imsi']].append(d)
result_list = list(result.values())
combine = result_list[0] + result_list[1]
with open("input_iot_7_13.json", "w") as outfile:
    json.dump(combine, outfile)


########################################################################################################################


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


from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json

'DefaultEndpointsProtocol=https;AccountName=sttemperaturetestweu001;AccountKey=iCAkFc6T8XcqyP23FRaT8Nd9mjrgwVUsfxP1IpJa1CfDRnAm3SpDI68nrXFAFmd5sPk852KlNf42+AStRg8PYw==;EndpointSuffix=core.windows.net'

container = ContainerClient.from_connection_string(conn_str='DefaultEndpointsProtocol=https;'
                                                            'AccountName=sttemperaturetestweu001;'
                                                            'AccountKey=iCAkFc6T8XcqyP23FRaT8Nd9mjrgwVUsfxP1IpJa1CfDRnAm3SpDI68nrXFAFmd5sPk852KlNf42+AStRg8PYw==;'
                                                            'EndpointSuffix=core.windows.net',
                                                   container_name='ci-temperature-test-weu-001')

blob = BlobClient.from_connection_string(conn_str='DefaultEndpointsProtocol=https;'
                                                  'AccountName=sttemperaturetestweu001;'
                                                  'AccountKey=iCAkFc6T8XcqyP23FRaT8Nd9mjrgwVUsfxP1IpJa1CfDRnAm3SpDI68nrXFAFmd5sPk852KlNf42+AStRg8PYw==;'
                                                  'EndpointSuffix=core.windows.net',
                                         container_name='ci-temperature-test-weu-001',
                                         blob_name="iot-temperature-test-weu-001/01/2022/07/01/11/49.json")

