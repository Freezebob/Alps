#!/home/marco/anaconda2/bin/python
#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import json
import re
import sys
import os
import collections
import time
import requests
import pickle

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

import metanome_api
import deps_classe

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
        "HyFD": metanome_api.alg_id("HyFD"),
        "HyUCC": metanome_api.alg_id("HyUCC"),
        "SPIDER": metanome_api.alg_id("SPIDER")
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
        requirements_list = metanome_api.requirements(alg)
        #print "requirements_list"
        #print requirements_list
        for item in requirements_list:
            if item["type"] == "ConfigurationRequirementRelationalInput":
                #print "R.Input"
                #print "PRIMA"
                #print item
                #print "INFO_DATASET"
                #print info_dataset(ds_path)
                item["settings"] = [metanome_api.info_dataset(ds_path)]
                #print "DOPO"
                #print item
            else:
                item["settings"] = [{"type": item["type"].replace("Requirement","Setting"),
                                     "value": item["defaultValues"][0]}]
        body = {
            "algorithmId": algs[alg],
            "executionIdentifier": result_name + "_" + alg + time.strftime("%Y-%m-%d_%H%M%S", time.gmtime()),
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

# Controllo se posso inserire l'hs passato
def hs_check(hs, table, conn):
    sel = select([table]).where(table.c.string == str(hs))
    ris = conn.execute(sel)
    if not ris.rowcount:
        ins = table.insert().values(string=str(hs))
        conn.execute(ins)
        return "Inserted"
    else:
        print "già dentro"
        for i in ris:
            return i["idHand_sides"]


# Leggo tutti i risultati presenti in mypath e li organizzo nel seguente modo
#   final_dep_results: contiene le dipendenze calcolate
#   stats: contiene i metadati su singola colonna
#   ds_names: contiene i nomi dei dataset analizzati
def read_all(mypath_results):
    count_dict = collections.defaultdict(int)
    engine = sqlalchemy.create_engine('mysql://root:rootpasswordgiven@localhost/Alps')
    conn = engine.connect()
    meta = MetaData()
    Hand_sides = Table('Hand_sides', meta, autoload=True, autoload_with=engine)
    Dependencies = Table('Dependencies', meta, autoload=True, autoload_with=engine)
    Datasets = Table('Datasets', meta, autoload=True, autoload_with=engine)


    final_dep_results = collections.defaultdict(dict)
    stats = {}
    attr_read = False
    ds_names = []
    onlyfiles = files_in_dir(mypath_results)
    for f in onlyfiles:
        #print f.split("_")[0]
        ds_name = f.split("_")[0] # invece di usare per es. orcid_SPIDER_inds,uso solo orcid
        ds_name = re.sub('[(){}<>]', '', ds_name) # Le parentesi non piacciono ai dict
        if "stats" not in f:
            print f
            attributes, dependencies = deps_classe.read_dep(mypath_results + f)

            # Problema: i singoli insert sono LENTI. Provo ad eseguirne tanti assieme
            # for dep in dependencies:
            #     grbg = hs_check(dep.lhs, Hand_sides, conn) # Controllo se è già in Hand_sides
            #     grbg = hs_check(dep.rhs, Hand_sides, conn) # Controllo se è già in Hand_sides
            #
            #     idlhs = hs_check(dep.lhs, Hand_sides, conn) # Uso differente rispetto a prima, lo uso per recuperare l'id dell'hs
            #     idrhs = hs_check(dep.rhs, Hand_sides, conn) # Uso differente rispetto a prima, lo uso per recuperare l'id dell'hs
            #     if type(dep) is deps_classe.FD:
            #         type_d = "FD"
            #     elif type(dep) is deps_classe.IND:
            #         type_d = "IND"
            #     elif type(dep) is deps_classe.UCC:
            #         type_d = "UCC"
            #     elif type(dep) is deps_classe.ORD:
            #         type_d = "ORD"
            #     ins = Dependencies.insert().values(type=type_d, idLHS=idlhs, idRHS=idrhs)
            #     conn.execute(ins)

            # FUNZIONA ed è quello che uso. L'ho commentato solo perché voglio testare la seconda parte sulla tabella Dependencies senza rieseguire questo
            values = ""
            for dep in dependencies:
                if type(dep) is deps_classe.FD:
                    # print "FD"
                    # print "Values prima: {}".format(values)
                    if dep.lhs:
                        values += str(dep.lhs).replace('[', '("').replace(']', '")') + ", "
                    else:
                        values += "('NULL'), "
                    values += str(dep.rhs).replace('[', '("').replace(']', '")') + ", "
                    print "Values dopo {}".format(values)
            values = values[:-2]
                # print "INSERT IGNORE INTO Alps.Hand_sides (`string`) VALUES {};".format(values)
                # print f
                # print "INSERT IGNORE INTO Alps.Hand_sides (`string`) VALUES {};".format(values)
            if values:
                engine.execute("INSERT IGNORE INTO Alps.Hand_sides (`string`) VALUES {};".format(values))



            # Devo ripetere il ciclo dep purtroppo. Vediamo più avanti se c'è un'alternativa
            selects = ""
            for dep in dependencies:
                if type(dep) is deps_classe.FD:
                    if dep.lhs:
                        tmp_lhs = str(dep.lhs).replace('[', '').replace(']', '')
                    else:
                        tmp_lhs = "NULL"
                    tmp_rhs = str(dep.rhs).replace('[', '').replace(']', '')
                    selects += '("FD", (SELECT idHand_sides FROM Alps.Hand_sides WHERE `string` = "{}"), (SELECT idHand_sides FROM Alps.Hand_sides WHERE `string` = "{}")), '.format(tmp_lhs, tmp_rhs)
                    print "Selects dopo: {}".format(selects)
            selects = selects[:-2]
            print "\n\n\n\n"
            print "INSERT IGNORE INTO Alps.Dependencies (`type`, `idLHS`, `idRHS`) VALUES {};".format(selects)
            if selects:
                engine.execute("INSERT IGNORE INTO Alps.Dependencies (`type`, `idLHS`, `idRHS`) VALUES {};".format(selects))
	        # SELECT "FD", (SELECT idHand_sides FROM Alps.Hand_sides WHERE `string` = "'10'"), (SELECT idHand_sides FROM Alps.Hand_sides WHERE `string` = "'11'");



            # PROBLEMA: dato che in una singola insert vado a mettere molti valori non posso sapere quelle che flliranno perché già presenti
            # quindi i loro id andranno "sprecati", generando in certi casi dei buchi tra un id e il successivo.
            # Se eseguissi un insert alla volta potrei "riciclare" gliid non usati, ma così non so come fare.
            # Ho comunue a disposizione gli id che mi servono per inserire una tupla nella tabella Dependencies.
            # E invece no, questo discorso vale solo per le tuple non presenti. Per quelle già memorizzate devo comunque fare un select
            # values = ""
            # for dep in dependencies:
            #     if type(dep) is deps_classe.FD:
            #         count_dict["lhs"] = count_dict["rhs"] + 1 # rhs al primo ciclo varrà 0
            #         count_dict["rhs"] = count_dict["lhs"] + 1
            #         # print "FD"
            #         # print "Values prima: {}".format(values)
            #         if dep.lhs:
            #             el = str(dep.lhs).replace('[', '("').replace(']', '")') + ", "
            #             el = "(" + str(count_dict["lhs"]) + ", " + el[1:]
            #             # values += str(dep.lhs).replace('[', '("').replace(']', '")') + ", "
            #             values = el
            #         else:
            #             values += "(" + str(count_dict["lhs"]) + ", " + "'NULL'), "
            #
            #         el = str(dep.rhs).replace('[', '("').replace(']', '")') + ", "
            #         el = "(" + str(count_dict["rhs"]) + ", " + el[1:]
            #         # values += str(dep.rhs).replace('[', '("').replace(']', '")') + ", "
            #         values += el
            #         print "Values dopo {}".format(values)
            # values = values[:-2]
            # # print "INSERT IGNORE INTO Alps.Hand_sides (`string`) VALUES {};".format(values)
            # # print f
            # # print "INSERT IGNORE INTO Alps.Hand_sides (`string`) VALUES {};".format(values)
            # if values:
            #     engine.execute("INSERT IGNORE INTO Alps.Hand_sides (`idHand_sides`, `string`) VALUES {};".format(values))

            if ds_name not in ds_names:
                ds_names.append(ds_name)
            #print f.split("_")
            dep_type = f.split("_")[-1] # idem per il tipo di dipendenza
            final_dep_results[ds_name][dep_type] =  dependencies
        else:
            #stats[f.split("_")[0]] = read_stats(mypath + f)
            #print mypath + f
            stats[ds_name] = deps_classe.read_stats(mypath_results + f)
    return (stats, ds_names, final_dep_results)


def handle_deps(mypath, mypath_results):
    # responses_list = exec_omni(mypath) # Lo commento solo perché non voglio ricalcolare tutte le diepndenze
    stats, ds_names, final_dep_results = read_all(mypath_results)
    return stats, ds_names, final_dep_results

if __name__ == "__main__":
    mypath = sys.argv[1]
    mypath_results = sys.argv[2]
    stats, ds_names, final_dep_results = handle_deps(mypath, mypath_results)
    with open("objs.pkl", "w") as f:
        pickle.dump([stats, ds_names, final_dep_results], f)
