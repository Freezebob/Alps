#!/home/marco/anaconda2/bin/python
#-*- coding: utf-8 -*-
import copy as cp
import pickle
import collections
import sys, os
import tkinter
import pymongo as pm
import pprint
import re
import pandas as pd
import numpy as np
import multiprocessing as mp
import time
import operator
import ast

import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey


sys.path.append('/home/marco/github/Alps_1')
from Manipolation import gui
#from Omni.deps_classe import *
from Omni import deps_classe

attributes = {1: 'id',
 10: 'country',
 11: 'countryCode',
 12: 'postcode',
 13: 'urbanUnit',
 14: 'urbanUnitCode',
 15: 'lat',
 16: 'lon',
 17: 'revenueRange',
 18: 'privateFinanceDate',
 19: 'employees',
 2: 'acronyms',
 20: 'typeCategoryCode',
 21: 'typeLabel',
 22: 'typeKind',
 23: 'isPublic',
 24: 'leaders',
 25: 'staff',
 26: 'links',
 27: 'privateOrgTypeId',
 28: 'privateOrgTypeLabel',
 29: 'activities',
 3: 'alias',
 30: 'relations',
 31: 'badges',
 32: 'children',
 33: 'identifiers',
 4: 'label',
 5: 'creationYear',
 6: 'commercialLabel',
 7: 'address',
 8: 'city',
 9: 'citycode'}


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
# PRECISAZIONE: per dipendenza nulla intendo dip che ha attributi nulli in lhs o rhs

# Le variabili stats, ds_names, final_dep_results calcolate nel package Omni mi servono anche qui. Per non dover rieseguire tutto le tengo salvate con pickle
def load_results():
    stats = {}
    ds_names = []
    final_dep_results = collections.defaultdict(dict)
    sys.path.append('/home/marco/github/Alps_1/Omni')  # Devo capire bene questo problema con pickle
    with open("/home/marco/github/Alps_1/Omni/objs.pkl", "r") as f:
        stats, ds_names, final_dep_results = pickle.load(f)

        # Aggiunta dopo aver standardizzato i nomim dei files
        # for i in xrange(len(ds_names)):
        #     ds_names[i] = re.sub(r'.*_', '', ds_names[i]).split('.')[0].lower()
        # print ds_names
        return stats, ds_names, final_dep_results

# # Per l'interfaccia grafica
# def get_selection(ds_names, ds_list_box):
#     # print "ciao"
#     selected_options = [ds_names[int(item)] for item in  ds_list_box.curselection()]
#     return selected_options
#
#
# # Interfaccia grafica di prova. Da aggiustare le dimensioni
#
# def ds_list(ds_names):
#     main = Tk()
#     main.title('Test GUI')
#     main.geometry('400x400')
#
#     nb = ttk.Notebook(main)
#     nb.grid(row=1, column=0, columnspan=50, rowspan=49, sticky='NESW')
#
#     page1 = ttk.Frame(nb)
#     nb.add(page1, text='Dataset names')
#
#     ds_list_box = Listbox(page1)
#     ds_list_box.configure(selectmode=MULTIPLE, width=9, height=5)
#     ds_list_box.grid(row=0, column=0)
#
#     btnGet = Button(page1,text="Get Selection",command= lambda: get_selection(ds_names, ds_list_box))
#     btnGet.grid()
#
#     for name in ds_names:
#         ds_list_box.insert(END, name)
#
#     main.mainloop()


# Per tutte le dipendenze comuni
# TODO: per ora funziona solo con le FDs. FATTO anche per INDs e UCCs
def intersezione_ign(a, b, dep_type):
    ris = []
    if dep_type == "fds":
        for i in a:
            #print i
            #print type(i)
            for j in b:
                #print 'Confronto {} e {}'.format(i, j)
                if i == j and i not in ris:
                    #print "buono ==, metto dentro {}".format(i)
                    ris.append(i)
                    #break  #Se un dataset ha []->7 e un altro ha [1]->7, [4]->7 e [31]->7,
                    #l'unico FD in comune segnalato è il primo [1]->7. Quindi tolgo il break
                elif i <= j and j not in ris:  # tra [1]->[3] e [1,2]->[3] prendo [1,2] come coomune
                    #print "buono <=, metto dentro {}".format(j)
                    ris.append(j)
                    #break
                elif j <= i and i not in ris:
                    #print "buono >=, metto dentro {}".format(i)
                    ris.append(i)
                    #break

    elif dep_type == "inds" or dep_type == "uccs":
        for i in a:
            for j in b:
                #print 'Confronto {} e {}'.format(i, j)
                if i == j and i not in ris:
                    ris.append(i)
    return ris


def intersection_all_ign(dep_type, dep_results):
    dep_intersection = []
    first = True
    for name in ds_names:
        dep_list = dep_results[name][dep_type]
        if first:
            dep_intersection = dep_list
            first = False
        else:
            dep_intersection = intersezione_ign(dep_intersection, dep_list, dep_type)
    return dep_intersection


# intersection_some_ign("fds", final_dep_results_copy, selected_options)
def intersection_some_ign(dep_type, dep_results, datasets):
    dep_intersection = []
    first = True
    for name in datasets:
        # print name
        dep_list = dep_results[name][dep_type]
        if first:
            dep_intersection = dep_list
            first = False
        else:
            dep_intersection = intersezione_ign(dep_intersection, dep_list, dep_type)
    return dep_intersection
# 1 Si crea la lista dei file presenti nella cartella
# 2 Si leggono questi file e si crea una lista con i loro nomi semplificati
# 3 Si calcola l'intersezione tra tutti loro su una certa dipendenza, es IND



# Voglio escudere le dipendenze che considerano colonne vuote
# Adesso solo per le FD
# INUTILE, NON COMPAIONO MAI COME LHS, DEVO CONTTROLLARE IL RHS
# for name in ds_names:
#     print name
#     for i in final_dep_results[name]["fds"]:
#         print i
#         for el in i.lhs:
#             print el
#             print "Perc " + str(stats[name]["Percentage of Nulls"][attributes[el]])
#             print "-------"
#             if stats[name]["Percentage of Nulls"][attributes[el]] == 100:
#                 print i
#                 print "Ha {} nullo".format(el)
#                 final_dep_results[name]["fds"].remove(i)
#                 break


# Scrematura delle dipendenze, tolgo le nulle e quelle riconducibili ad una UCC
# (per lhs ho una UCC oppure un suo sovrainsieme)
def deps_screm(final_dep_results, ds_names):
    final_dep_results_copy = cp.deepcopy(final_dep_results)
    for name in ds_names:
        # print name
        for i in final_dep_results[name]["fds"]:
            # print i.rhs[0]
            # print "Perc " + str(stats[name]["Percentage of Nulls"][attributes[i.rhs[0]]])
            if stats[name]["Percentage of Nulls"][attributes[i.rhs[0]]] == 100:
                #print i
                #print "Ha {} nullo".format(i.rhs)
                final_dep_results_copy[name]["fds"].remove(i)  # Il remove creava dei problemi col for i, scombussolando l'ordine
                #print "-------"
            for j in final_dep_results[name]["uccs"]:
                # Ho cambiato la classe UCC
                # if set(i.lhs) == set(j.comb) or set(i.lhs) >= set(j.comb):   #Trovo lo stesso caso mille volte ['1'] e ['1'], ['1'] e ['1'] eccc
                    #print "lhs: {}, ucc: {}".format(i.lhs, j.comb)           #Penserò ad una soluzione
                    #print "-------"
                if set(i.lhs) == set(j.lhs) or set(i.lhs) >= set(j.lhs):
                    final_dep_results_copy[name]["fds"].remove(i)
    return final_dep_results_copy


# opened_csvs contiene tutti i csv pronti per essere analizzati
def csvs(csvpath, ds_names):
    opened_csvs = {}
    #csvpath = "/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/"
    onlyfiles = [f for f in os.listdir(csvpath) if os.path.isfile(os.path.join(csvpath, f))]
    for f in onlyfiles:
        # only_name = re.sub(r'.*_', '', f).split('.')[0]
        # only_name = re.sub('[(){}<>]', '', only_name)
        # print only_name.lower()
        # opened_csvs[only_name.lower()] = pd.read_csv('/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/' + f, sep=',', dtype=dtypes_dict)

        opened_csvs[re.sub('[(){}<>]', '', f)] = pd.read_csv('/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/' + f, sep=',', dtype=dtypes_dict)

    #     for i in opened_csvs[only_name.lower()].columns:
    #         print i
    #         opened_csvs[only_name.lower()][i] = opened_csvs[only_name.lower()][i].astype(str)
    # print "ds_names: {}".format(ds_names)
    # print "opened_csvs.keys(): {}".format(opened_csvs.keys())
    for i in ds_names:
        opened_csvs[i] = opened_csvs[i].fillna(-1)

    # Non una soluzione elegante, ma ogni tupla con almeno un elemento nullo non è considerata dal groupby
    # e mi rende difficile testare le dipendenze

    # Ricorda: le dipendenze considerano i valori null = null (come se avessero tutti lo stesso valore)
    # quindi ['1', '2'] ->3 non è vero se ho tra le tuple:
    #     1  2     3
    #     a  null  x
    #     a  null  y

    return opened_csvs


# Dipendenze esclusive per ogni dataset selezionato
def exclusive_d(selected_options, final_dep_results_copy):
    exclusive_deps = collections.defaultdict(dict)
    for ds in selected_options:
        tmp = []
        for i in final_dep_results_copy[ds]["fds"]:
            if i not in inter_fds:
                p = False
                for y in inter_fds:
                    # i = [1]->[3] e  y = [1,2]->[3]. In questo caso non posso considerare i come dip esclusiva
                    if i <= y:
                        p = True
                if p == False: # Solo quelle che arrivano con p=False sono davvero dip esclusive
                    tmp.append(i)
        exclusive_deps[ds]["fds"] = tmp
    return exclusive_deps


# Il mio groupby
def bing_bing_bong_new(a_list, ds, opened_csvs):
    # print "Biiiiing"
#     start = time.time()
    asd = "ciao"
    # creo un dizionario dove ogni chiave corrispsonde ad una riga del dataset, considerando solo gli attribubti di a_lilst, cioè lhs + rhs (che è l'ultimo elemento)
    dict_df = opened_csvs[ds][[attributes[i] for i in a_list]].to_dict("split")
    # print "dict_df: {}".format(dict_df)
    dict_df_data = dict_df["data"]  # righe vere e proprie solo con gli attributi desiderati
    # print "dict_df_data: {}".format(dict_df_data)

    result = collections.defaultdict(lambda: collections.defaultdict(int))

    for row in dict_df_data:
        result[str(row[:-1])][row[-1]] += 1
    # pprint.pprint(result)
    # print "\n\n\n\n\n\n\n\n"
    for key in result.keys():
        if len(result[key]) == 1:
            result.pop(key, None)
    # pprint.pprint(result)
    return result


def process_function(d_p, selected_options, exclusive_deps, stats, opened_csvs):
#     scre_dict = rec_dd()
    #deps_screm_nuovo_process = rec_dd()
    client_p = pm.MongoClient()
    db = client_p["Deps_db"]
    fds_collection = db["FDS"]
    seg_list = []
    for ds1 in selected_options:
            for ds2 in selected_options:
                if (ds1 != ds2 and d_p[ds2]):
                    tmp = []
                    l1 = d_p[ds2][0]
                    l2 = d_p[ds2][1]
                    for i in exclusive_deps[ds2]["fds"][l1:l2]:
                        # print "controllo {} di {} su {}".format(i, ds2, ds1)
                        if stats[ds1]["Percentage of Nulls"][attributes[i.rhs[0]]] != 100:
#                           provo su ds1 le dip esclusive di ds2,
#                            ma queste sono scremate sulla struttura di ds2. Quindi devo vedere se sono dip nulle per ds1
                            new_i = cp.deepcopy(i)
                            # print i
                            for x in i.lhs:
                                # print "esamino {}".format(x)
                                # print stats[ds1]["Percentage of Nulls"][attributes[x]]
                                # print "-----"
                                if stats[ds1]["Percentage of Nulls"][attributes[x]] == 100:
                                    new_i.lhs.remove(x)
                            # print new_i
                            if new_i not in tmp:
                                tmp.append(new_i)

                    # In tmp ho scremato ho le dip (scremate ed esclusive di ds1) scremate su ds2
                    t_dict = {
                            "test_ds": ds1,
                            "source_ds": ds2,
                            "dependencies": {}
                        }

                    for dep in tmp:
                        a_list = dep.lhs
                        a_list.append(dep.rhs[0])
                        if len(a_list) > 1:
                            result = bing_bing_bong_new(a_list, ds1,opened_csvs)
                            if result:
                                # cioè se il dict non è vuoto (lo è se controllo delle "mezze" ucc) devo spiegarlo meglio questo discorso
                                # Penso che il senso sia questo. Con screm ho tolto le dip relative alle ucc. Ma se in ds2 c'è una dipendenza
                                # che per ds1 era relativa ad una ucc (ma che ora è stata tolta dalla scrematura) allora il suo groupby non produrrà nulla
                                a = [[result.values()[i].keys(), result.values()[i].values()] for i in xrange(len(result))]
                                t_dict["dependencies"][str(a_list)] = [result.keys(), a]
                            else:
                                t_dict["dependencies"][str(a_list)] = []

                    for k in t_dict["dependencies"].keys():
                        t_dict["dependencies"]["dependencies."+str(k)] = t_dict["dependencies"].pop(k)  # la dicitura dependencies.str(k) serve per accedere
                                                                                                        # all'elemento con chiave k in dependencies. è unna cosa fattibile con pymongo


                    ex_string = 'fds_collection.update_one({{"test_ds": "{}", "source_ds": "{}"}}, {{"$set": {}}}, upsert=True)'.format(ds1, ds2, t_dict["dependencies"])
                    print "ex_string: {}".format(ex_string)
                    exec(ex_string)

def create_slides(n, n_slides):
    ris = [i for i in xrange(0, n, n_slides)]
    ris.append(n)
    return ris


def split(a, n):
    a = [i for i in xrange(a)]
    k, m = divmod(len(a), n)
    l = list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in xrange(n))
    for i in xrange(len(l)):
        if l[i]:
            l[i] = [l[i][0], l[i][len(l[i])-1] +1]
    return l


def attributes_names(x):
    x = ast.literal_eval(x)
    names = []
    for i in x:
        names.append(attributes[i])
    return names


def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.iteritems():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]


if __name__ == "__main__":
    # DEVO sostituire questa la lettura del file pickle con delle letture dal database creato
    stats, ds_names, final_dep_results = load_results()
#     print final_dep_results["organizations_ARAMIS.csv"].keys()
#     print final_dep_results.keys()
#     print stats.keys()
    #Senza fare scremature
    # stats, ds_names, final_dep_results_copy = load_results()


    # engine = sqlalchemy.create_engine('mysql://root:rootpasswordgiven@localhost/Alps_1')
    # conn = engine.connect()
    # ris = engine.execute("SELECT name FROM Alps_1.Datasets")
    # ds_names = []
    # for i in ris:
    #     ds_names.append(i[0]) # Visto che ogni elemento di ris è una tuplla con dim > 1 anche se contiene un solo elemento



    # QUESTA è una cosa impoortante da esaminare
    # Non avendo più problemi di memoria provo a non fare nessuna scrematura
    final_dep_results_copy = deps_screm(final_dep_results, ds_names)
#     for i in final_dep_results.keys():
#         for d in final_dep_results[i]["fds"]:
#             print d

    asd = "aaaaaaaaaaa"
    csvpath = "/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/"
    opened_csvs = csvs(csvpath, ds_names)
    root = tkinter.Tk()
    menu = gui.my_gui(root, ds_names)
    root.mainloop()
    selected_options = menu.selected_options

    inter_fds = intersection_some_ign("fds", final_dep_results_copy, selected_options)
    exclusive_deps = exclusive_d(selected_options, final_dep_results_copy)

    ds_slides = {}
    for ds in selected_options:
        # print ds
        if exclusive_deps[ds]:
            n = len(exclusive_deps[ds]["fds"])
        else:
            n = 0
        ds_slides[ds] = split(n, 4)
    # es
    # {'organizations_Alps_1v20.csv': [[0, 365], [365, 729], [729, 1093], [1093, 1457]],
    #  'organizations_Alps_1v20Dedup.csv': [[0, 211], [211, 422], [422, 633], [633, 844]]}
    processes = []
    dict_process = {}
    l_d_p = [] #list_dict_process
    # for i in xrange(len(slides)):
    for i in xrange(4):
        for ds in ds_slides.keys():
            dict_process[ds] = ds_slides[ds][i]
        l_d_p.append(cp.deepcopy(dict_process))
        # es di l_d_p
         # [{'organizations_Alps_1v20.csv': [0, 365], 'organizations_Alps_1v20Dedup.csv': [0, 211]},
         #           {'organizations_Alps_1v20.csv': [365, 729], 'organizations_Alps_1v20Dedup.csv': [211, 422]},
         #           {'organizations_Alps_1v20.csv': [729, 1093], 'organizations_Alps_1v20Dedup.csv': [422, 633]},
         #           {'organizations_Alps_1v20.csv': [1093, 1457], 'organizations_Alps_1v20Dedup.csv': [633, 844]}]
        #print l_d_p[i]
    for i in l_d_p:
#         print "slice per processo: {}".format(i)
        processes.append(mp.Process(target=process_function, args=(i, selected_options, exclusive_deps, stats, opened_csvs)))

    client = pm.MongoClient()
    db = client["Deps_db"]
    fds_collection = db["FDS"]
    fds_collection.create_index([("test_ds", pm.DESCENDING), ("source_ds", pm.DESCENDING)])
    start = time.time()
    for x in processes:
        x.start()
    # results = [output.get() for p in xrange(len(slides))]
    for x in processes:
        x.join()
    end = time.time()
    print(end-start)

    for option in selected_options:
        mongo_dict = fds_collection.find_one({"source_ds": option})
        sum_dict = collections.defaultdict(int)
    #     pprint.pprint(mongo_dict)
        if mongo_dict:
            for key in mongo_dict["dependencies"].keys():
                somma = 0
            #     print key
                if mongo_dict["dependencies"][key]:
                    for i in mongo_dict["dependencies"][key][1]:
                        somma += (sum(i[1]) - max(i[1]))
                    sum_dict[key] = somma
            sorted_x = sorted(sum_dict.items(), key=operator.itemgetter(1))

            # for key in deps_screm_nuovo["Alps_1v20dedup"]["Alps_1v20"]["fds"].keys():
            # Devo solo sistemare l'output. Non funziona se per una copia di ds non ci sono dipendenze esclusive (non nulle) da visualizzare
            for key in sorted_x:
                print "Dependency: {}".format(key[0])
                print "Names: {}\n".format(attributes_names(key[0]))
                for i, j in zip(mongo_dict["dependencies"][key[0]][0], mongo_dict["dependencies"][key[0]][1]):
                    print "\033[1m Key \033[0m: {}\n\033[1m Vals \033[0m: {}\n".format(i, j)
                print "----------------------"
            print asd
