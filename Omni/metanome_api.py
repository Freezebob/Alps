import json
import pandas as pd
import requests
import re
NOT_WHITESPACE = re.compile(r'[^\s]')

#Per leggere piu' strutture JSON presenti nello stesso documento
def decode_stacked(document, pos=0, decoder=json.JSONDecoder()):
    while True:
        match = NOT_WHITESPACE.search(document, pos)
        if not match:
            return
        pos = match.start()

        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError:
            # do something sensible if there's some error
            raise
        yield obj


# Passando il nome di un algoritmo, ottengo il suo id
def alg_id(alg_name):
    url = 'http://127.0.0.1:8081/api/algorithms'
    r = requests.get(url)
    resp_json = json.loads(r.content)
    for alg in resp_json:
        if alg_name in alg["fileName"]:
            return alg["id"]


# Passando il nome di un algoritmo, ottengo il suo nome completo, incluso di estensione .jar
def alg_name_full(alg_name):
    url = 'http://127.0.0.1:8081/api/algorithms'
    r = requests.get(url)
    resp_json = json.loads(r.content)
    for alg in resp_json:
        if alg_name in alg["fileName"]:
            return alg["fileName"]


# Passando il nome dell'algoritmo ottengo la lista di tutti i parametri necessari per eseguirlo
def requirements(alg_name):
    n = alg_name_full(alg_name) #nome completo da usare nella prossima API
    url = 'http://127.0.0.1:8081/api/parameter/' + n
    print url
    r = requests.get(url)
    resp_json = json.loads(r.content)
    for item in resp_json:
        del item["fixNumberOfSettings"]
    return resp_json


# Passando il path di un ds ottengo le sue info,
# cioe' quello che  deov mettere in settingsper il type:
# "ConfigurationRequirementRelationalInput"
def info_dataset(ds_path):
    url = 'http://127.0.0.1:8081/api/file-inputs'
    r = requests.get(url)
    resp_json = json.loads(r.content)
    for ds in resp_json:
        if ds_path == ds["name"]:
            ds["separatorChar"] = ds.pop("separator")
            ds["header"] = ds.pop("hasHeader")
            #print ds
            for item in ["name", "comment"]:
                del ds[item]
            ds["type"] = "ConfigurationSettingFileInput"
            return ds
