import pandas as pd
import numpy as np
import json
import metanome_api
import copy
import re
import sys
import os
#Il body della richiesta

dtypes_dict = {'id': int,
 'country': str,
 'countryCode': str,
 'postcode': str,
 'urbanUnit': str,
 'urbanUnitCode': str,
 'lat': float,
 'lon': float,
 'revenueRange': str,
 'privateFinanceDate': str,
 'employees': str,
 'acronyms': str,
 'typeCategoryCode': str,
 'typeLabel': str,
 'typeKind': str,
 'isPublic': str,
 'leaders': str,
 'staff': str,
 'links': str,
 'privateOrgTypeId': float,
 'privateOrgTypeLabel': str,
 'activities': str,
 'alias': str,
 'relations': str,
 'badges': str,
 'children': str,
 'identifiers': str,
 'label': str,
 'creationYear': str,
 'commercialLabel': str,
 'address': str,
 'city': str,
 'citycode': str}

def omni(ds_path, result_name):
    print ds_path
    responses = []
    algs = {
        "SCDP": metanome_api.alg_id("SCDP"),
        "HyFD": alg_id("HyFD"),
        "HyUCC": alg_id("HyUCC"),
        "SPIDER": alg_id("SPIDER")
    }
    headers = {
        #"Host": "localhost:8081",
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "it-IT,it;q=0.8,en-US;q=0.5,en;q=0.3",
        #"Accept-Encoding": "gzip, deflate",
        "Referer": "http://localhost:8080/",
        "Content-Type":"application/json;charset=utf-8",
        #"Content-Length": "1926",
        "Origin": "http://localhost:8080",
        "Connection": "keep-alive",
        "Pragma":"no-cache",
        "Cache-Control":"no-cache"
    }
    url = 'http://127.0.0.1:8081/api/algorithm-execution/'
    for alg in ["SCDP", "HyFD", "HyUCC", "SPIDER"]: #
        requirements_list = requirements(alg)
        #print "requirements_list"
        #print requirements_list
        for item in requirements_list:
            if item["type"] == "ConfigurationRequirementRelationalInput":
                #print "R.Input"
                #print "PRIMA"
                #print item
                #print "INFO_DATASET"
                #print info_dataset(ds_path)
                item["settings"] = [info_dataset(ds_path)]
                #print "DOPO"
                #print item
            else:
                item["settings"] = [{"type": item["type"].replace("Requirement","Setting"),
                                     "value": item["defaultValues"][0]}]
        body = {
            "algorithmId": algs[alg],
            "executionIdentifier": result_name + "_" + alg + strftime("%Y-%m-%d_%H%M%S", gmtime()),
            ##
            # Boh non capisco perche' adesso devo mettere lower anche alg
            ##
            "requirements": requirements_list,
            "cacheResults":False,
            "writeResults":True,
            "countResults":False,
            "memory":""
        }
        #print json.dumps(body)
        r = requests.post(url, headers=headers, json=body)
        responses.append(json.loads(r.content))
    return responses


# Se voglio processare solo alcuni file per ora sposto tutti gli indesiderati nella cartella "limbo"
# Poi pensero' ad un modo piu' carino.
def exec_omni(mypath):
    #mypath = "/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/"
    onlyfiles = [f for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f))]
    print onlyfiles
    responses_list = []
    for csv in onlyfiles:
        name = re.sub(r'.*_', '', csv)
        name = name.split(".")[0].lower()
        #print mypath + csv
        responses_list.append(omni(mypath + csv, name))
    return responses_list


# Lista di tutti i file nella seguente cartella
def files_in_dir(mypath_results):
    #mypath = "/home/marco/Scrivania/dep/results/"
    onlyfiles = [f for f in os.listdir(mypath_results) if os.path.isfile(os.path.join(mypath_results, f))]
    return onlyfiles


# Leggo tutti i risultati presenti in mypath e li organizzo nel seguente modo
#   final_dep_results: contiene le dipendenze calcolate
#   stats: contiene i metadati su singola colonna
#   ds_names: contiene i nomi dei dataset analizzati
def read_all(mypath_results):
    final_dep_results = defaultdict(dict)
    stats = {}
    attr_read = False
    ds_names = []
    for f in onlyfiles:
        #print f.split("_")[0]
        ds_name = f.split("_")[0] # invece di usare per es. orcid_SPIDER_inds,uso solo orcid
        ds_name = re.sub('[(){}<>]', '', ds_name) # Le parentesi non piacciono ai dict
        if "stats" not in f:
            attributes, dependencies = read_dep(mypath_results + f)
            if ds_name not in ds_names:
                ds_names.append(ds_name)
            #print f.split("_")
            dep_type = f.split("_")[-1] # idem per il tipo di dipendenza
            final_dep_results[ds_name][dep_type] =  dependencies
        else:
            #stats[f.split("_")[0]] = read_stats(mypath + f)
            #print mypath + f
            stats[ds_name] = read_stats(mypath_results + f)
    return (stats, ds_names, final_dep_results)


def handle_deps(mypath, mypath_results):
    responses_list = exec_omni(mypath)
    onlyfiles = files_in_dir(mypath_results)
    stats, ds_names, final_dep_results = read_all(mypath_results)

if __name__ == "__main__":
    mypath = sys.argv[1]
    mypath_results = sys.argv[2]
    a, b, c = handle_deps(mypath, mypath_results)
