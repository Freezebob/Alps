#!/home/marco/anaconda2/bin/python
#-*- coding: utf-8 -*-
#from tkinter import Tk, Label, Button, Listbox, MULTIPLE, END
import tkinter

class my_gui:
    def __init__(self, master, ds_names):
        self.master = master
        self.ds_names = ds_names
        master.title("A simple GUI")

        self.label = tkinter.Label(master, text="This is our first GUI!")
        self.label.pack()

        self.ds_list_box = tkinter.Listbox(master)
        self.ds_list_box.configure(selectmode=tkinter.MULTIPLE, width=50, height=20)
        self.ds_list_box.pack(side="left",fill="both", expand=True)

        self.ds_button = tkinter.Button(master,text="Get Selection",command= self.get_selection)
        self.ds_button.pack()
        for name in ds_names:
            self.ds_list_box.insert(tkinter.END, name)

        self.close_button = tkinter.Button(master, text="Close", command=master.quit)
        self.close_button.pack()

    def greet(self):
        print("Greetings!")

    def get_selection(self):
        self.selected_options = [self.ds_names[int(item)] for item in  self.ds_list_box.curselection()]


if __name__ == "__main__":
    root = tkinter.Tk()
    ds_names = ["Ciao", "ciao2", "ciao3"]
    menu = my_gui(root, ds_names)
    root.mainloop()
    print menu.selected_options

#
#
# #!/home/marco/anaconda2/bin/python
# #-*- coding: utf-8 -*-
# # In questo provo un approccio diverso e  cerco di fare il più possibile su mysql. Vediamo poi quel è il più veloce
#
# from __future__ import unicode_literals
# # Per gestire questo errore:
# #  UnicodeDecodeError: 'ascii' codec can't decode byte 0xe4 in position 10: ordinal not in range(128)
#
#
# import copy as cp
# import pickle
# import collections
# import sys, os
# import tkinter
# import pymongo as pm
# import pprint
# import re
# import pandas as pd
# import numpy as np
# import multiprocessing as mp
# import time
# import operator
# import ast
# import numbers
#
# import sqlalchemy
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.sql import select, and_
# from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, func, orm
# from sqlalchemy.orm import sessionmaker
#
#
# import gui
# sys.path.append('/home/marco/github/Alps_1')
# #from Omni.deps_classe import *
# import Omni.deps_classe
#
#
#
# # Globali
# engine = sqlalchemy.create_engine('mysql://root:rootpasswordgiven@localhost/Alps_1?charset=utf8') # , fast_executemany=True non funziona
# conn = engine.connect()
# meta = MetaData()
# org_ds = Table('organizations_alpsv20.csv', meta, autoload=True, autoload_with=engine)
# Session = sessionmaker(bind=engine)
# session = orm.Session(engine)
#
# attributes = {1: 'id',
#               10: 'country',
#               11: 'countryCode',
#               12: 'postcode',
#               13: 'urbanUnit',
#               14: 'urbanUnitCode',
#               15: 'lat',
#               16: 'lon',
#               17: 'revenueRange',
#               18: 'privateFinanceDate',
#               19: 'employees',
#               2: 'acronyms',
#               20: 'typeCategoryCode',
#               21: 'typeLabel',
#               22: 'typeKind',
#               23: 'isPublic',
#               24: 'leaders',
#               25: 'staff',
#               26: 'links',
#               27: 'privateOrgTypeId',
#               28: 'privateOrgTypeLabel',
#               29: 'activities',
#               3: 'alias',
#               30: 'relations',
#               31: 'badges',
#               32: 'children',
#               33: 'identifiers',
#               4: 'label',
#               5: 'creationYear',
#               6: 'commercialLabel',
#               7: 'address',
#               8: 'city',
#               9: 'citycode'}
#
# dtypes_dict = {'id': int,
#                'country': str,
#                'countryCode': str,
#                'postcode': str,
#                'urbanUnit': str,
#                'urbanUnitCode': str,
#                'lat': float,
#                'lon': float,
#                'revenueRange': str,
#                'privateFinanceDate': str,
#                'employees': str,
#                'acronyms': str,
#                'typeCategoryCode': str,
#                'typeLabel': str,
#                'typeKind': str,
#                'isPublic': str,
#                'leaders': str,
#                'staff': str,
#                'links': str,
#                'privateOrgTypeId': float,
#                'privateOrgTypeLabel': str,
#                'activities': str,
#                'alias': str,
#                'relations': str,
#                'badges': str,
#                'children': str,
#                'identifiers': str,
#                'label': str,
#                'creationYear': str,
#                'commercialLabel': str,
#                'address': str,
#                'city': str,
#                'citycode': str}
#
#
# # Le variabili stats, ds_names, final_dep_results calcolate nel package Omni mi servono anche qui. Per non dover rieseguire tutto le tengo salvate con pickle
# def load_results():
#     stats = {}
#     ds_names = []
#     final_dep_results = collections.defaultdict(dict)
#     sys.path.append('/home/marco/github/Alps_1/Omni')  # Devo capire bene questo problema con pickle
#     with open("/home/marco/github/Alps_1/Omni/objs.pkl", "r") as f:
#         stats, ds_names, final_dep_results = pickle.load(f)
#         # print ds_names
#         return stats, ds_names, final_dep_results
#
#
# # Per tutte le dipendenze comuni
# # TODO: per ora funziona solo con le FDs. FATTO anche per INDs e UCCs
# def intersezione_ign(a, b, dep_type):
#     ris = []
#     if dep_type == "fds":
#         for i in a:
#             # print i
#             # print type(i)
#             for j in b:
#                 #                 print 'Confronto {} e {}'.format(i, j)
#                 if i == j and i not in ris:
#                     # print "buono ==, metto dentro {}".format(i)
#                     ris.append(i)
#                     # break  #Se un dataset ha []->7 e un altro ha [1]->7, [4]->7 e [31]->7,
#                     # l'unico FD in comune segnalato è il primo [1]->7. Quindi tolgo il break
#                 elif i <= j and j not in ris:
#                     # print "buono <=, metto dentro {}".format(j)
#                     ris.append(j)
#                     # break
#                 elif j <= i and i not in ris:
#                     # print "buono >=, metto dentro {}".format(i)
#                     ris.append(i)
#                     # break
#
#     elif dep_type == "inds" or dep_type == "uccs":
#         for i in a:
#             for j in b:
#                 # print 'Confronto {} e {}'.format(i, j)
#                 if i == j and i not in ris:
#                     ris.append(i)
#     return ris
#
#
# def intersection_all_ign(dep_type, dep_results):
#     dep_intersection = []
#     first = True
#     for name in ds_names:
#         dep_list = dep_results[re.sub('[(){}<>]', '', name)][dep_type]
#         if first:
#             dep_intersection = dep_list
#             first = False
#         else:
#             dep_intersection = intersezione_ign(dep_intersection, dep_list, dep_type)
#     return dep_intersection
#
#
# # intersection_some_ign("fds", final_dep_results_copy, selected_options)
# # Visto che adesso faccio solo l'intersezione tra due dataset potrei usare direttamente intersection_ign e basta
# def intersection_some_ign(dep_type, dep_results, datasets):
#     dep_intersection = []
#     first = True
#     for name in datasets:
#         #         print "Intersezione, considero {}".format(name)
#         dep_list = dep_results[re.sub('[(){}<>]', '', name)][dep_type]
#         #         print "dep_results in intersection_some_ign: {}".format(dep_results)
#         if first:
#             dep_intersection = dep_list
#             first = False
#         else:
#             dep_intersection = intersezione_ign(dep_intersection, dep_list, dep_type)
#     return dep_intersection
#
#
# # 1 Si crea la lista dei file presenti nella cartella
# # 2 Si leggono questi file e si crea una lista con i loro nomi semplificati
# # 3 Si calcola l'intersezione tra tutti loro su una certa dipendenza, es IND
#
#
# # Voglio escudere le dipendenze che considerano colonne vuote
# # Adesso solo per le FD
# # INUTILE, NON COMPAIONO MAI COME LHS, DEVO CONTTROLLARE IL RHS
# # for name in ds_names:
# #     print name
# #     for i in final_dep_results[name]["fds"]:
# #         print i
# #         for el in i.lhs:
# #             print el
# #             print "Perc " + str(stats[name]["Percentage of Nulls"][attributes[el]])
# #             print "-------"
# #             if stats[name]["Percentage of Nulls"][attributes[el]] == 100:
# #                 print i
# #                 print "Ha {} nullo".format(el)
# #                 final_dep_results[name]["fds"].remove(i)
# #                 break
#
#
# # Scrematura delle dipendenze, tolgo le nulle e quelle riconducibili ad una UCC (per lhs ho una UCC oppure un suo sovrainsieme)
# # def deps_screm(deps, ds_name, stats):
# def deps_screm(deps, ds_name):
#     print "Scremo le dipendenze"
#     deps_copy = cp.deepcopy(deps)
#
#     for i in deps["fds"]:
#         # print i.rhs[0]
#         # print "Perc " + str(stats[name]["Percentage of Nulls"][attributes[i.rhs[0]]])
#         #         print "ds_name: {}".format(ds_name)
#         res = engine.execute("SELECT `Percentage of Nulls` FROM Alps_1.`stats_{}` WHERE `columnIdentifier` = '{}'".format(ds_name.split("_")[1], attributes[i.rhs[0]])).fetchone()[0]
#         # if stats[re.sub('[(){}<>]', '', ds_name)]["Percentage of Nulls"][attributes[i.rhs[0]]] == 100:
#         if res == 100:
#             # print i
#             # print "Ha {} nullo".format(i.rhs)
#             deps_copy["fds"].remove(i)  # Il remove creava dei problemi col for i, scombussolando l'ordine
#             # print "-------"
#         for j in deps["uccs"]:
#             # Ho cambiato la classe UCC
#             # if set(i.lhs) == set(j.comb) or set(i.lhs) >= set(j.comb):   #Trovo lo stesso caso mille volte ['1'] e ['1'], ['1'] e ['1'] eccc
#             # print "lhs: {}, ucc: {}".format(i.lhs, j.comb)           #Penserò ad una soluzione
#             # print "-------"
#             if set(i.lhs) == set(j.lhs) or set(i.lhs) >= set(j.lhs):
#                 deps_copy["fds"].remove(i)
#     #     print "Dipendenze dopo la scrematura: {}".format(deps_copy)
#     return deps_copy
#
#
# # opened_csvs contiene tutti i csv pronti per essere analizzati
# def csvs(csvpath, ds_names):
#     opened_csvs = {}
#     # csvpath = "/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/"
#     onlyfiles = [f for f in os.listdir(csvpath) if os.path.isfile(os.path.join(csvpath, f))]
#     for f in onlyfiles:
#         # only_name = re.sub(r'.*_', '', f).split('.')[0]
#         # only_name = re.sub('[(){}<>]', '', only_name)
#         # print only_name.lower()
#         opened_csvs[re.sub('[(){}<>]', '', f)] = pd.read_csv('/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/' + f,
#                                                      sep=',', dtype=dtypes_dict).fillna(-1)
#     #     for i in opened_csvs[only_name.lower()].columns:
#     #         print i
#     #         opened_csvs[only_name.lower()][i] = opened_csvs[only_name.lower()][i].astype(str)
#
#     # for i in ds_names:
#     #     opened_csvs[i] = opened_csvs[i].fillna(-1)
#
#     # Non una soluzione elegante, ma ogni tupla con almeno un elemento nullo non è considerata dal groupby
#     # e mi rende difficile testare le dipendenze
#
#     # Ricorda: le dipendenze considerano i valori null = null (come se avessero tutti lo stesso valore)
#     # quindi ['1', '2'] ->3 non è vero se ho tra le tuple:
#     #     1  2     3
#     #     a  null  x
#     #     a  null  y
#
#     return opened_csvs
#
#
# # Dipendenze esclusive per i due dataset selezionati (2 alla volta si fa)
# # names[0] -> source
# # names[1] -> test
# def exclusive_d(names, datasets_dict, final_dep_results, inter_fds):
#     exclusive_deps = collections.defaultdict(dict)
#     # for source_ds in names: # dataset_dict.keyes() non andava bene dato che riordina sempre gli elementi alfabeticamente
#     #     for ds in names:
#     #         if ds != source_ds:
#     #             test_ds = ds
#     source_ds = names[0]
#     test_ds = names[1]
#     inserts = ""
#     tmp = []
#     # Scorro le dipendenze del SOURCE_ds per trovare quelle esclusive (considerando l'altro ds)
#     for i in final_dep_results[re.sub('[(){}<>]', '', source_ds)]["fds"]:
#         if i not in inter_fds:
#             p = False
#             for y in inter_fds:
#                 # i = [1]->[3] e  y = [1,2]->[3]. In questo caso non posso considerare i come dip esclusiva
#                 if i <= y:
#                     p = True
#             if p == False:
#                 # Solo quelle che arrivano con p=False sono davvero dip esclusive,
#
#                 tmp.append(i)
#                 inserts += """("{}", "{}", (SELECT idDependencies FROM Alps_1.Dependencies
#                                         WHERE `type` = "{}"
#                                         AND idLHS = (SELECT idHand_sides FROM Alps_1.Hand_sides WHERE `string` = "{}")
#                                         AND idRHS = (SELECT idHand_sides FROM Alps_1.Hand_sides WHERE `string` = "{}"))), """.format(
#                     datasets_dict[test_ds], datasets_dict[source_ds], "FD", re.sub('[(){}\[\]<>]', '', str(i.lhs)),
#                     re.sub('[(){}\[\]<>]', '', str(i.rhs)))
#     exclusive_deps[re.sub('[(){}<>]', '', source_ds)]["fds"] = tmp
#     inserts = inserts[:-2]
#
#     if inserts:
#         #         print "INSERT IGNORE INTO Alps_1.Exclusive_deps (`datasets_test_id`, `datasets_source_id`, `dependencies_id`) VALUES {};".format(inserts)
#         engine.execute(
#             "INSERT IGNORE INTO Alps_1.Exclusive_deps (`datasets_test_id`, `datasets_source_id`, `dependencies_id`) VALUES {};".format(
#                 inserts))
#     return exclusive_deps
#
#
# # Il mio groupby
# # TODO: dovrei farne una versione sql
# def bing_bing_bong_new(a_list, ds, opened_csvs):
#     print "Biiiiing"
#     #     start = time.time()
#     asd = "ciao"
#     dict_df = opened_csvs[re.sub('[(){}<>]', '', ds)][[attributes[i] for i in a_list]].to_dict("split")
#     dict_df_data = dict_df["data"]
#
#     result = collections.defaultdict(lambda: collections.defaultdict(int))
#
#     for row in dict_df_data:
#         result[str(row[:-1])][row[-1]] += 1 # Prima chiave LHS, seconda chiave RHS, valore conteggio di quel RHS
#     for key in result.keys():
#         if len(result[key]) == 1:
#             result.pop(key, None)
#     return result
#
# # Il mio groupby
# # TODO: dovrei farne una versione sql
# def modified_bing_bing_bong_new(a_list, ds, opened_csvs):
#     print "Biiiiing"
#     a_list = [1] + a_list
#     print "a_list: {}".format(a_list)
#     #     start = time.time()
#     asd = "ciao"
#     result = collections.defaultdict(lambda: collections.defaultdict(int))
#     result_id_list = collections.defaultdict(lambda: collections.defaultdict(list))
#
#     projection_df = opened_csvs[re.sub('[(){}<>]', '', ds)][[attributes[i] for i in a_list]]
#     for row in projection_df.itertuples(index=False):
#         # result[str(row[1:-1])][row[-1]] += 1
#         # creo un secondo dict con le stesse chiavi e tiene la lista degli id delle righe. Posso far senza di result.
#         # Se conto gli id (row[0]) ottengo il count delle violazioni
#         result_id_list[str(row[1:-1])][str(row[-1])].append(row[0])
#     # for key in result.keys():
#     #     if len(result[key]) == 1:
#     #         result.pop(key, None)
#     for key in result_id_list.keys():
#         if len(result_id_list[key]) == 1:
#             result_id_list.pop(key, None)
#     return result_id_list
#
# def bing_bing_bong_sql_no_string(a_list, ds, engine_process):
#     id_viol = [] # Ci salvo gli id delle tuple violate
#     a_names = [attributes[i] for i in a_list]
#
#     # start = time.time()
#     # rr = session.query(org_ds.c.label, org_ds.c.address, org_ds.c.links).group_by(org_ds.c.label, org_ds.c.address,
#     #                                                                               org_ds.c.links).having(
#     #     func.count() > 1).all()
#     # end = time.time()
#     # print "r .query: {}".format(end - start)
#     start = time.time()
#     rr2 = select([org_ds.c[name] for name in a_names[:-1]]).\
#           group_by(*[org_ds.c[name] for name in a_names[:-1]]).\
#           having(func.count() > 1)
#     result = conn.execute(rr2)
#     end = time.time()
#     print "r .select: {}".format(end - start)
#
#     # Raggruppo per LHS e tengo solo quelli con count > 1
#     # start = time.time()
#     # a_names_string = ""
#     # for i in a_names[:-1]:  # L'ultimo elemento è l'RHS che non mi serve adesso
#     #     a_names_string += str(i) + ", "
#     # a_names_string = a_names_string[0:-2]
#     # r = engine_process.execute("""SELECT {} FROM Alps_1.`{}`
#     #                       GROUP BY {}
#     #                       HAVING COUNT(*) > 1;""".format(a_names_string, ds, a_names_string))
#     # end = time.time()
#     # print "r: {}".format(end - start)
#
#
#     # Ora devo controllare i relativi RHS delle tuplpe con LHS uguale (solo se gli RHS sono diversi allora ho  una violazione)
#
#     # Una volta che itero su tuples perde i suoi elementi e non posso fare un altro ciclo per prendere gli id
#     for i in result:
#         id_viol_tmp = []
#         diff_rhs = set()
#         dd = {attr: i[attr] for attr in i.keys()}
#         where_clauses = [org_ds.c[key] == value for (key, value) in dd.iteritems()]
#
#         rr3 = select([org_ds.c.id, org_ds.c[a_names[-1]]], and_(*where_clauses))
#         result2 = conn.execute(rr3)
#         for tup in result2:
#             # print tup
#             diff_rhs.add(tup[a_names[-1]])
#             id_viol_tmp.append((tup["id"]))
#         if len(diff_rhs) >= 2:
#             # print "rhs: {}".format(diff_rhs)
#             id_viol.append(id_viol_tmp)
#     return id_viol
#
# def bing_bing_bong_sql_string(a_list, ds, engine_process):
#     id_viol = []  # Ci salvo gli id delle tuple violate
#     a_names = [attributes[i] for i in a_list]
#
#     # start = time.time()
#     # rr = session.query(org_ds.c.label, org_ds.c.address, org_ds.c.links).group_by(org_ds.c.label, org_ds.c.address,
#     #                                                                               org_ds.c.links).having(
#     #     func.count() > 1).all()
#     # end = time.time()
#     # print "r .query: {}".format(end - start)
#     start = time.time()
#     rr2 = select([org_ds.c[name] for name in a_names[:-1]]). \
#         group_by(*[org_ds.c[name] for name in a_names[:-1]]). \
#         having(func.count() > 1)
#     result = conn.execute(rr2)
#     end = time.time()
#     print "r .select: {}".format(end - start)
#
#     # Raggruppo per LHS e tengo solo quelli con count > 1
#     # start = time.time()
#     # a_names_string = ""
#     # for i in a_names[:-1]:  # L'ultimo elemento è l'RHS che non mi serve adesso
#     #     a_names_string += str(i) + ", "
#     # a_names_string = a_names_string[0:-2]
#     # r = engine_process.execute("""SELECT {} FROM Alps_1.`{}`
#     #                       GROUP BY {}
#     #                       HAVING COUNT(*) > 1;""".format(a_names_string, ds, a_names_string))
#     # end = time.time()
#     # print "r: {}".format(end - start)
#
#     # Ora devo controllare i relativi RHS delle tuplpe con LHS uguale (solo se gli RHS sono diversi allora ho  una violazione)
#
#     # Una volta che itero su tuples perde i suoi elementi e non posso fare un altro ciclo per prendere gli id
#     for i in result:
#         aaa = dict(i)
#         equals = ""
#         id_viol_tmp = []
#         diff_rhs = set()  # appena ho 2 rhs diversi so che c'è una violazione. Mi basta questo. Eh no!!!
#
#         # start = time.time()
#         for attr in i.keys():
#             #         print attr
#             #         print type(i[attr])
#             str_i = str(i[attr]) if isinstance((i[attr]), numbers.Number) else i[attr] # Per engine.execute
#             # str_i = str(i.attr) if isinstance(i.attr, numbers.Number) else i.attr # Per session.query
#         #     equals += attr + (" IS NULL" if i[attr] is None else (' = "' + str_i.replace('"', '\\"') + '"')) + " AND "
#         # equals = equals[:-5] # IDEM
#             equals += attr + (" IS NULL" if i.attr is None else (' = "' + str_i.replace('"', '\\"') + '"')) + " AND "
#         equals = equals[:-5]
#         # end = time.time()
#         # print "Stringa: {}".format(end - start)
#
#         # print "Equals: {}".format(equals)
#         # print """query: SELECT DISTINCT {} FROM Alps_1.`{}`
#         #                 WHERE {}""".format(a_names[-1], ds, equals)
#         # start = time.time()
#
#         # start = time.time()
#         tuples = engine_process.execute("""SELECT {} FROM Alps_1.`{}`
#                                    WHERE {} """.format("id, " + a_names[-1], ds, equals))
#         # end = time.time()
#         # print "tuples: {}".format(end - start)
#
#         # start = time.time()
#         dd = {attr: i[attr] for attr in i.keys()}
#
#         # dd = {attr: None if i[attr] is None else i[attr] for attr in i.keys()} # Inutile, ottengo lo stesso ris senza if, cioè {'address': None, 'links': None ....}
#         # rr2 = session.query(org_ds.c.id).filter_by(**dd)
#         # end = time.time()
#         # print "tuples orm: {}".format(end - start)
#
#         # start = time.time()
#         for tupl in tuples:
#             diff_rhs.add(tupl[a_names[-1]])
#             id_viol_tmp.append((tupl["id"]))
#         if len(diff_rhs) >= 2:
#             id_viol.append(id_viol_tmp) # Se non ho rhs diversi cancello tutti gli id nella lista
#         # end = time.time()
#         # print "final loop: {}".format(end - start)
#     return id_viol
#
#         # end = time.time()
#         # print "query: {}".format(end - start)
#         # if tuples.rowcount != 1:
#         #     print "trovata eccezione"
#         #     for t in tuples:
#         #         print t
#         #     print "---"
#
#
#
# # def process_function(d_p, selected_options, exclusive_deps, stats, opened_csvs):
# def process_function(d_p, selected_options, exclusive_deps, opened_csvs):
#     #     scre_dict = rec_dd()
#     # deps_screm_nuovo_process = rec_dd()
#     client_p = pm.MongoClient()
#     db = client_p["Deps_db"]
#     fds_collection = db["FDS"]
#     seg_list = []
#     for ds1 in selected_options:
#         for ds2 in selected_options:
#             if (ds1 != ds2 and d_p[ds2]):
#                 tmp = []
#                 l1 = d_p[ds2][0]
#                 l2 = d_p[ds2][1]
#                 for i in exclusive_deps[ds2]["fds"][l1:l2]:
#                     # print "controllo {} di {} su {}".format(i, ds2, ds1)
#                     res = engine.execute("SELECT `Percentage of Nulls` FROM Alps_1.`stats_{}` WHERE `columnIdentifier` = '{}'".format(ds1.split("_")[1], attributes[i.rhs[0]])).fetchone()[0]
#
#                     # if stats[ds1]["Percentage of Nulls"][attributes[i.rhs[0]]] != 100:
#                     if res != 100:
#                         new_i = cp.deepcopy(i)
#                         for x in i.lhs:
#                             # print "esamino {}".format(x)
#                             # print stats[ds1]["Percentage of Nulls"][attributes[x]]
#                             # print "-----"
#                             res = engine.execute("SELECT `Percentage of Nulls` FROM Alps_1.`stats_{}` WHERE `columnIdentifier` = '{}'".format(ds1.split("_")[1], attributes[i.rhs[0]])).fetchone()[0]
#                             if res == 100:
#                             # if stats[ds1]["Percentage of Nulls"][attributes[x]] == 100:
#                                 new_i.lhs.remove(x)
#                         # print new_i
#                         if new_i not in tmp:
#                             tmp.append(new_i)
#
#                 # scre_dict[ds1][ds2] = np.array(tmp, dtype=FD) #à stesso discorso: provo su ds1 le dip esclusive di ds2,
#                 # ma queste sono scremate sulla struttura di ds1
#                 # for dep in tmp:
#                 # print "ds1: {}, ds2: {}. Dep: {}".format(ds1, ds2, dep)
#                 t_dict = {
#                     "test_ds": ds1,
#                     "source_ds": ds2,
#                     "dependencies": {},
#                     "count": 0
#                 }
#                 #                     print("t_dict: {}".format(t_dict))
#                 for dep in tmp:
#                     # tmp_dict = {}
#                     # print dep
#                     # a_list = [el for el in dep.lhs]
#                     # print a_list
#                     a_list = dep.lhs
#                     # print a_list
#                     a_list.append(dep.rhs[0])
#                     # print "a_list: {}".format(a_list)
#                     if len(a_list) > 1:
#                         # tmp_dict[str(a_list)] = bing_bing_bong(a_list, ds1)
#                         # deps_screm[ds1][ds2]["fds"] = tmp_dict
#                         result = bing_bing_bong_new(a_list, ds1, opened_csvs)
#                         if result:
#                             a = [[result.values()[i].keys(), result.values()[i].values()] for i in xrange(len(result))]
#                             #                                 deps_screm_nuovo_process[ds1][ds2]["fds"][str(a_list)] = np.array([np.array(result.keys()), a])
#                             t_dict["dependencies"][str(a_list)] = [result.keys(), a]
#                         else:
#                             t_dict["dependencies"][str(a_list)] = []
#
#                 for k in t_dict["dependencies"].keys():
#                     # t_dict["dependencies"]["dependencies."+k] = t_dict["dependencies"].pop(k)
#                     fds_collection.update_one(
#                         {"test_ds": ds1, "source_ds": ds2, "count": {"$lt": 200}},
#                         {
#                             "$set": {"dependencies." + k: t_dict["dependencies"][k]},
#                             "$inc": {"count": 1}
#                         },
#                         upsert=True
#                     )
#
#
# def create_slides(n, n_slides):
#     ris = [i for i in xrange(0, n, n_slides)]
#     ris.append(n)
#     return ris
#
#
# def split(a, n):
#     a = [i for i in xrange(a)]
#     k, m = divmod(len(a), n)
#     l = list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in xrange(n))
#     for i in xrange(len(l)):
#         if l[i]:
#             l[i] = [l[i][0], l[i][len(l[i]) - 1] + 1]
#     return l
#
#
# def attributes_names(x):
#     x = ast.literal_eval(x)
#     names = []
#     for i in x:
#         names.append(attributes[i])
#     return names
#
#
# def dict_merge(dct, merge_dct):
#     """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
#     updating only top-level keys, dict_merge recurses down into dicts nested
#     to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
#     ``dct``.
#     :param dct: dict onto which the merge is executed
#     :param merge_dct: dct merged into dct
#     :return: None
#     """
#     for k, v in merge_dct.iteritems():
#         if (k in dct and isinstance(dct[k], dict)
#                 and isinstance(merge_dct[k], collections.Mapping)):
#             dict_merge(dct[k], merge_dct[k])
#         else:
#             dct[k] = merge_dct[k]
#
#
# def load_ds_names_sql():
#     ris = engine.execute("SELECT name FROM Alps_1.Datasets")
#     ds_names = []
#     for i in ris:
#         ds_names.append(i[0])
#     return ds_names
#
#
# # Solo FD per ora
# def recreate_deps(deps_ids):
#     print "Ricreo le dipendenze"
#     deps = collections.defaultdict(list)
#     for db_id in deps_ids:
#         #         print "db_id: {}".format(db_id)
#         dep = engine.execute("SELECT * FROM Alps_1.Dependencies WHERE idDependencies = {}".format(db_id)).fetchone()
#
#         lhs = engine.execute("SELECT `string` FROM Alps_1.Hand_sides WHERE idHand_sides = {}".format(dep["idLHS"])).fetchone()[0]  # ottengo qualcosa diquesto tipo '10, 5, 4'
#         # print "lhs: {}".format(lhs)
#         # print "split ', ': {}".format(lhs.split(", "))
#         lhs = [int(i) if i != 'NULL' else '' for i in lhs.split(", ")]
#         # print "int lhs: {}".format(lhs)
#
#         rhs = engine.execute("SELECT `string` FROM Alps_1.Hand_sides WHERE idHand_sides = {}".format(dep["idRHS"])).fetchone()[0]
#         # print "rhs: {}".format(rhs)
#         # print "split ', ': {}".format(rhs.split(", "))
#         rhs = [int(i) if i != 'NULL' else '' for i in rhs.split(", ")]
#         # print "int rhs: {}".format(rhs)
#
#         #         print "lhs: {}, rhs: {}".format(lhs, rhs)
#         #         print "dep[type]: {}".format(dep["type"])
#         if dep["type"] == "FD":
#             deps["fds"].append(Omni.deps_classe.FD(lhs, rhs))
#         elif dep["type"] == "UCC":
#             deps["uccs"].append(Omni.deps_classe.UCC(lhs))
#     #     print "deps: {}".format(deps)
#     return deps
#
#
# def flatten_list(l):
#     flat_list = []
#     for sublist in l:
#         if type(sublist) == list:
#             for item in sublist:
#                 flat_list.append(item)
#         else:
#             flat_list.append(sublist)
#     return flat_list
#
#
# def process_function_sql(d_p, selected_options, deps_cleaned_dict):
#     # es di d_p: {'organizations_Alps_1v20.csv': [0, 365], 'organizations_Alps_1v20Dedup.csv': [0, 211]}
#
#     # Ogni processo deve avere il proprio engine
#     engine_process = sqlalchemy.create_engine('mysql://root:rootpasswordgiven@localhost/Alps_1?charset=utf8')
#     conn = engine_process.connect()
#     for ds in selected_options:
#         if d_p[ds]:
#             l1 = d_p[ds][0]
#             l2 = d_p[ds][1]
#             for dep in deps_cleaned_dict[ds]["fds"][l1:l2]:
#                 print "DEP: {}".format(dep)
#                 # SBAGLILATO così vado a modificare l'elemneto dep aggiungendo a lhs l'rhs. es da [1,2]->[3] vado a [1,2,3]->[3] perché sto usando append che è una funzione
#                 # python si basa su puntatori agli oggetti, c'era spiegata questa cosa su stackoverflow, da trovare (il problema è l'aver usato append)
#                 # a_list = dep.lhs
#                 # a_list.append(dep.rhs[0])
#                 a_list = dep.lhs + dep.rhs
#                 if len(a_list) > 1:
#                     # devo fare il groupby
#                     # result = modified_bing_bing_bong_new(a_list, ds1, opened_csvs)
#                     result = bing_bing_bong_sql_no_string(a_list, ds1, engine_process)
#
#                     if result:
#                         tuple_list = []
#
#                         # Istruzioni per gestire l'output di new_bing_bing_bong
#                         # nested_list = [result.values()[i].values() for i in xrange(len(result))]
#                         # flat_list = flatten_list(flatten_list(nested_list))
#
#                         flat_list = flatten_list(flatten_list(result))
#
#                         tmp_lhs = str(dep.lhs).replace('[', '').replace(']', '')
#                         tmp_rhs = str(dep.rhs).replace('[', '').replace(']', '')
#                         ds_id = engine_process.execute('SELECT idDataset FROM Alps_1.Datasets WHERE `name` = "{}"'.format(ds)).fetchone()[0]
#                         # print """SELECT idDependencies FROM Alps_1.Dependencies WHERE `type` = "{}"
#                         #                     AND idLHS = (SELECT idHand_sides FROM Alps_1.Hand_sides WHERE `string` = "{}")
#                         #                     AND idRHS = (SELECT idHand_sides FROM Alps_1.Hand_sides WHERE `string` = "{}")""".format("FD", tmp_lhs, tmp_rhs)
#                         dep_id = engine_process.execute("""SELECT idDependencies FROM Alps_1.Dependencies WHERE `type` = "{}"
#                                             AND idLHS = (SELECT idHand_sides FROM Alps_1.Hand_sides WHERE `string` = "{}")
#                                             AND idRHS = (SELECT idHand_sides FROM Alps_1.Hand_sides WHERE `string` = "{}")""".format("FD", tmp_lhs, tmp_rhs)).fetchone()[0]
#                         selects_violations = ''
#                         for val in flat_list:
#                             selects_violations += "({}, {}, {}), ".format(ds_id, dep_id, val)
#                         selects_violations = selects_violations[:-2]
#
#                         engine_process.execute("INSERT IGNORE INTO Alps_1.Violations (`datasets_id`, `dependencies_id`, `n_tuple`) VALUES {};".format(selects_violations))
#
#
#                         # Vecchio output, ora voglio solo alcune cose da inserire nella tabella Violations:
#                             # id dataset
#                             # id della dipendenza
#                             # id delle tuple che violano
#                         # a = [[result.values()[i].keys(), result.values()[i].values()] for i in xrange(len(result))]
#                         # print "LHSs: " + str(result.keys())
#                         # print "RHSs: " + str(a)
#                         # print "------"
#                         # print "\n\n"
#
# # def insert_exclusive_deps(selected_options, stats):
# # Funzionante, ma l'ho scomposta per farla girare in multiprocessing
# def insert_exclusive_deps(selected_options):
#
#     # ds1: source_ds
#     # ds2: test_ds
#     inter_fds_dict = dict()
#     # Ho tolto il dict stats, il prossimo sarebbe di togliere final_dep_results, ma per ora è troppo comodo
#     # Penso d i tenerlo. Alla fine è fatto di dep costruite facendo interrogazioni sul DB per le loro componenti
#     final_dep_results = collections.defaultdict(dict)
#     for ds1 in selected_options:
#         for ds2 in selected_options:
#             if (ds1 != ds2):
#                 print "Confronto source:{} con  test:{}".format(ds1, ds2)
#                 # dipendenze esclusive di ds1 che non sono in ds2
#                 ds1_id = engine.execute("SELECT idDataset FROM Alps_1.Datasets where `name` = '{}'".format(ds1)).fetchone()[0]
#                 if "fds" not in final_dep_results[re.sub('[(){}<>]', '',
#                                                          ds1)].keys():  # controllo su fds, ma poi rimepio anche le uccs (e ini futuro le altre)
#                     # Dato che confronto tutti i ds selezionati. Un certo ds sarà confrontato con
#                     # tutti gli altri e mi basta riempire la sua parte di dict una volta sola.
#                     # Quando ci sarà "fds" come chiave vorrà dire che è stato riempito e non serve rifarlo.
#                     print "Non ho ancora inserito {} in final_dep_results".format(ds1)
#
#                     #                     print "ds1_id: {}".format(ds1_id)
#                     ds1_deps = engine.execute("""SELECT dependencies_idDependencies
#                                       FROM Alps_1.Datasets_Dependencies
#                                       WHERE datasets_idDataset = {}""".format(ds1_id))
#                     ds1_deps = [r[0] for r in ds1_deps]
#                     #                     print "ds1_deps: {}".format(ds1_deps)
#                     deps = recreate_deps(ds1_deps)
#                     final_dep_results[re.sub('[(){}<>]', '', ds1)]["fds"] = deps_screm(deps, ds1)["fds"]
#                     final_dep_results[re.sub('[(){}<>]', '', ds1)]["uccs"] = deps["uccs"]
#                     # pulilzia
#                 ds2_id = engine.execute("SELECT idDataset FROM Alps_1.Datasets where `name` = '{}'".format(ds2)).fetchone()[0]
#                 if "fds" not in final_dep_results[re.sub('[(){}<>]', '', ds2)].keys():
#                     print "Non ho ancora inserito {} in final_dep_results".format(ds2)
#                     ds2_deps = engine.execute("""SELECT dependencies_idDependencies
#                                       FROM Alps_1.Datasets_Dependencies
#                                       WHERE datasets_idDataset = {}""".format(ds2_id))
#                     ds2_deps = [r[0] for r in ds2_deps]
#                     deps = recreate_deps(ds2_deps)
#                     final_dep_results[re.sub('[(){}<>]', '', ds2)]["fds"] = deps_screm(deps, ds2)["fds"]
#                     final_dep_results[re.sub('[(){}<>]', '', ds2)]["uccs"] = deps["uccs"]
#                     # A questo punto ho le liste di oggetti FD ( e UCC)
#                     # Pulizia
#                 #                 print "final_dep_results: {}".format(final_dep_results)
#
#                 # Se ho source: A e test: B calcolo inter_AB, ma quando avrò source:B e test:A l'inter_BA = inter_AB, ma ormai
#                 # è stato cancellato. Tengo gli inter salvati
#                 names = [ds1, ds2]
#                 names.sort()
#                 names_string = '_'.join([i for i in names])
#                 print "names_string: {}".format(names_string)
#                 if names_string not in inter_fds_dict.keys():
#                     # Lo faccio per riempire inter_fds che contiente le intersezioni tra coppie di dataset. Una volta
#                     # che ho calcolato l'intersezione tra A e B, quando avrò Be A non dovrò mideps_scrca rifarlo.
#                     # Per questo faccio il sort e il join dei nome come chiave di inter_fds_dict
#                     print "Non ce l'ho ancora nel dict"
#                     inter_fds_dict[names_string] = intersection_some_ign("fds", final_dep_results, [ds1, ds2])
#                 #                 print "inter_fds: {}".format(inter_fds)
#                 exclusive_deps = exclusive_d([ds1, ds2], {ds1: ds1_id, ds2: ds2_id}, final_dep_results,
#                                              inter_fds_dict[names_string])
#
#                 # inter_fds tra i due ds
#                 # exclusive_deps sui due ds
#     # devo metterli dentro il doppio ciclo for, cambiando qualcosa
#     # faccio il check delle diepndnenze di un ds dopo aver fatto recreate_deps e poi lo innserisco
#     # in final_dep_results che quindi sarà già scremato.
#     # Dopo averlo fatto anche per il ds2 ho le loro dipendenze pulite ed applico (dentro al ciclo) inter_fds ed exclusive_deps
#     # queste dip esclusive le metterò dentro al dataset ( avendo sia ds1 che ds2, l'id delle dip lo recupero, ce li ho già in ds1/2_deps comunque, ma dovrei accoppiarli)
#     # (potrei creare un dict con gli id e le rispettive dip, così le ho già comode da usare)
#
#     # Roba inutile credo
#     # final_dep_results_copy = deps_screm(final_dep_results, selected_options)
#     # inter_fds = intersection_some_ign("fds", final_dep_results_copy, selected_options)
#     # exclusive_deps = exclusive_d(selected_options, final_dep_results_copy, inter_fds)
#     #
#     #
#     #             selects = engine.execute("""SELECT dependencies_idDependencies
#     #                               FROM Alps_1.Datasets_Dependencies
#     #                               WHERE datasets_idDataset = {}
#     #                               AND dependencies_idDependencies not in
#     #         									(SELECT dependencies_idDependencies FROM Alps_1.Datasets_Dependencies
#     #                                              WHERE datasets_idDataset = {});""".format(ds1_id, ds2_id))
#     #
#     #
#     #             values = ""
#     #             for dep in selects:
#     #                 values += "({}, {}, {}), ".format(ds2_id, ds1_id, dep[0])
#     #             values = values[:-2]
#     #             engine.execute("INSERT IGNORE INTO Alps_1.Exclusive_deps (`datasets_test_id`, `datasets_source_id`, `dependencies_id`) VALUES {};".format(values))
#
#
# # def insert_violations(selected_options, stats, opened_csvs):
# def insert_violations(selected_options, opened_csvs):
#
#     for ds1 in selected_options:
#         violated_deps = []
#         ds1_id = engine.execute("SELECT idDataset FROM Alps_1.Datasets where `name` = '{}'".format(ds1)).fetchone()[0]
#         deps = engine.execute("SELECT dependencies_id FROM Alps_1.Exclusive_deps WHERE datasets_test_id = {}".format(ds1_id))
#         # Adesso ho gli id delle dep violate da ds1
#         deps_recreated = recreate_deps([r[0] for r in deps])
#         deps_cleaned = []  # deps_recreated contiene tutte le dip non possedute da ds1, ma se il motivo sono
#         #  degli attributi nulli, allora le tolgo, non mi interessano
#         for i in deps_recreated["fds"]:  #
#             res = engine.execute("SELECT `Percentage of Nulls` FROM Alps_1.`stats_{}` WHERE `columnIdentifier` = '{}'".format(ds1.split("_")[1], attributes[i.rhs[0]])).fetchone()[0]
#             # if stats[re.sub('[(){}<>]', '', ds1)]["Percentage of Nulls"][attributes[i.rhs[0]]] != 100:
#             if res != 100:
#                 # provo su ds1 le dip esclusive di ds2,
#                 # ma queste sono scremate sulla struttura di ds2. Quindi devo vedere se sono dip nulle per ds1
#                 new_i = cp.deepcopy(i)
#                 # print i
#                 for x in i.lhs:
#                     # print "esamino {}".format(x)
#                     # print stats[ds1]["Percentage of Nulls"][attributes[x]]
#                     # print "-----"
#                     res = engine.execute("SELECT `Percentage of Nulls` FROM Alps_1.`stats_{}` WHERE `columnIdentifier` = '{}'".format(ds1.split("_")[1], attributes[i.rhs[0]])).fetchone()[0]
#                     # if stats[re.sub('[(){}<>]', '', ds1)]["Percentage of Nulls"][attributes[x]] == 100:
#                     if res == 100:
#                         # Le lhs possono contenere attributi che per ds1 sono nulli. Li tolgo dato che sono inutilil
#                         # Perché possono essere nulli? Nel ds in cui la dip valeva era tutto ok, ma ora la sto provando
#                         # su un altro dataset.
#                         new_i.lhs.remove(x)
#                 # print new_i
#                 if new_i not in deps_cleaned:
#                     deps_cleaned.append(new_i)
#                 # Ho ricreato le dipendenze per cui ds1 ha delle violazioni e le ho appena scremate
#         for dep in deps_cleaned:
#             print "DEP: {}".format(dep)
#             a_list = dep.lhs
#             a_list.append(dep.rhs[0])
#             if len(a_list) > 1:
#                 #devo fare il groupby
#                 result = bing_bing_bong_new(a_list, ds1, opened_csvs)
#                 if result:
#                     a = [[result.values()[i].keys(), result.values()[i].values()] for i in xrange(len(result))]
#                     print "LHSs: " + str(result.keys())
#                     print "RHSs: " + str(a)
#                     print "------"
#             print "\n\n"
#
#         # for dep in deps:
#         #     # Ho la lista degli attributi che compongono l'LHS
#         #
#         #     dep_type = engine.execute("SELECT `type` FROM Alps_1.Dependencies WHERE idDependencies = {}".format(dep[0])).fetchone()[0]
#         #     if dep_type == "FD":
#         #         print "dipendenza violate da: {}".format(ds1)
#         #         print "dep_type: {}".format(dep_type)
#         #         lhs_list = engine.execute("""SELECT `string` FROM Alps_1.Hand_sides
#         #                                        WHERE idHand_sides = (SELECT idLHS FROM Alps_1.Dependencies
#         #                                                             WHERE idDependencies = {})""".format(dep[0])).fetchone()[0]
#         #         rhs_list = engine.execute("""SELECT `string` FROM Alps_1.Hand_sides
#         #                                        WHERE idHand_sides = (SELECT idRHS FROM Alps_1.Dependencies
#         #                                                             WHERE idDependencies = {})""".format(dep[0])).fetchone()[0]
#         #
#         #         print "lhs_list: {}".format(lhs_list)
#         #         print "rhs_list: {}".format(rhs_list)
#         #         a_list = lhs_list + rhs_list  # Credo non serva comunque
#
#
# # Lista di tutti i file nella seguente cartella
# def files_in_dir(mypath_results):
#     # mypath = "/home/marco/Scrivania/dep/results/"
#     onlyfiles = [f for f in os.listdir(mypath_results) if os.path.isfile(os.path.join(mypath_results, f))]
#     return onlyfiles
#
# # Inutile, le carico nell'Omni intanto
# def load_stats(stats_path):
#     onlyfiles = files_in_dir(stats_path)
#     for file in onlyfiles:
#         if file.split("_")[-1] == "stats":
#             file_path = stats_path + file
#             stats = pd.read_csv(file_path)
#             stats.to_sql(file.split("_")[0] + "_stats", con=engine, if_exists="replace", index=False)
#
#
# # TODO una funzione per la pulizia nei nomi delle parentesi, dato che se devo usarli con i dict danno errore
# if __name__ == "__main__":
#     ds_names = load_ds_names_sql()
#     print ds_names
#     # stats, a, b = load_results()  # Lo tengo solo perché non sono riuscito ancora ad inserire le stats nel db
#     # print "ds_names: {}".format(ds_names)
#     csvpath = "/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/"
#     opened_csvs = csvs(csvpath, ds_names) # Non più necessario
#
#     start = time.time()
#     porca_troia = bing_bing_bong_sql_no_string([4,7,26,14], "organizations_alpsv20.csv", engine)
#     end = time.time()
#     print "sql no string: {}".format(end - start)
#
#     start = time.time()
#     porca_puttana_vacca_troia = bing_bing_bong_sql_string([4, 7, 26, 14], "organizations_alpsv20.csv", engine)
#     end = time.time()
#     print "sql no string: {}".format(end - start)
#
#     start = time.time()
#     rris = modified_bing_bing_bong_new([4,7,26,14], "organizations_alpsv20.csv", opened_csvs)
#     end = time.time()
#     print "Without sql {}".format(end - start)
#
#     root = tkinter.Tk()
#     menu = gui.my_gui(root, ds_names)
#     root.mainloop()
#     selected_options = menu.selected_options
#     # insert_exclusive_deps(selected_options, stats)
#     insert_exclusive_deps(selected_options)
#     # insert_violations(selected_options, stats, opened_csvs)
#
#     deps_cleaned_dict = collections.defaultdict(dict)
#     deps_cleaned_slices = collections.defaultdict(dict)
#
#     for ds1 in selected_options:
#         violated_deps = []
#         ds1_id = engine.execute("SELECT idDataset FROM Alps_1.Datasets where `name` = '{}'".format(ds1)).fetchone()[0]
#         deps = engine.execute("SELECT dependencies_id FROM Alps_1.Exclusive_deps WHERE datasets_test_id = {}".format(ds1_id))
#         # Adesso ho gli id delle dep violate da ds1
#         deps_recreated = recreate_deps([r[0] for r in deps])
#         deps_cleaned_tmp = []  # deps_recreated contiene tutte le dip non possedute da ds1, ma se il motivo sono
#         #  degli attributi nulli, allora le tolgo, non mi interessano
#         for i in deps_recreated["fds"]:  #
#             if (i.lhs[-1] == i.rhs[0]):
#                 print "A\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\nA\n"
#             if (i.lhs == [8, 4, 26, 13]):
#                 print "ciao"
#             res = engine.execute("SELECT `Percentage of Nulls` FROM Alps_1.`stats_{}` WHERE `columnIdentifier` = '{}'".format(ds1.split("_")[1], attributes[i.rhs[0]])).fetchone()[0]
#             # if stats[re.sub('[(){}<>]', '', ds1)]["Percentage of Nulls"][attributes[i.rhs[0]]] != 100:
#             if res != 100:
#                 # provo su ds1 le dip esclusive di ds2,
#                 # ma queste sono scremate sulla struttura di ds2. Quindi devo vedere se sono dip nulle per ds1
#                 new_i = cp.deepcopy(i)
#                 # print i
#                 for x in i.lhs:
#                     # print "esamino {}".format(x)
#                     # print stats[ds1]["Percentage of Nulls"][attributes[x]]
#                     # print "-----"
#                     res = engine.execute("SELECT `Percentage of Nulls` FROM Alps_1.`stats_{}` WHERE `columnIdentifier` = '{}'".format(ds1.split("_")[1], attributes[i.rhs[0]])).fetchone()[0]
#                     # if stats[re.sub('[(){}<>]', '', ds1)]["Percentage of Nulls"][attributes[x]] == 100:
#                     if res == 100:
#                         # Le lhs possono contenere attributi che per ds1 sono nulli. Li tolgo dato che sono inutilil
#                         # Perché possono essere nulli? Nel ds in cui la dip valeva era tutto ok, ma ora la sto provando
#                         # su un altro dataset.
#                         new_i.lhs.remove(x)
#                 # print new_i
#                 if new_i not in deps_cleaned_tmp:
#                     deps_cleaned_tmp.append(new_i)
#                 # Ho ricreato le dipendenze per cui ds1 ha delle violazioni e le ho appena scremate
#         deps_cleaned_dict[ds1]["fds"] = deps_cleaned_tmp
#         if deps_cleaned_tmp:
#             n = len(deps_cleaned_tmp)
#         else:
#             n = 0
#         deps_cleaned_slices[ds1]["fds"] = split(n, 4)
#         # es
#         # {'organizations_Alps_1v20.csv': [[0, 365], [365, 729], [729, 1093], [1093, 1457]],
#         #  'organizations_Alps_1v20Dedup.csv': [[0, 211], [211, 422], [422, 633], [633, 844]]}
#
#     processes = []
#     dict_process = {}
#     l_d_p = []
#     for i in xrange(4):
#         for ds in selected_options:
#             dict_process[ds] = deps_cleaned_slices[ds]["fds"][i]
#         l_d_p.append(cp.deepcopy(dict_process))
#         # es di l_d_p
#         # [{'organizations_Alps_1v20.csv': [0, 365], 'organizations_Alps_1v20Dedup.csv': [0, 211]},
#         #           {'organizations_Alps_1v20.csv': [365, 729], 'organizations_Alps_1v20Dedup.csv': [211, 422]},
#         #           {'organizations_Alps_1v20.csv': [729, 1093], 'organizations_Alps_1v20Dedup.csv': [422, 633]},
#         #           {'organizations_Alps_1v20.csv': [1093, 1457], 'organizations_Alps_1v20Dedup.csv': [633, 844]}]
#     for i in l_d_p:
#         print "Ad un processo passo: {}".format(i)
#         processes.append(mp.Process(target=process_function_sql, args=(i, selected_options, deps_cleaned_dict)))
#     start = time.time()
#     for x in processes:
#         x.start()
#     # results = [output.get() for p in xrange(len(slides))]
#     for x in processes:
#         x.join()
#     end = time.time()
#     print(end - start)
#
#     # insert_violations(selected_options, opened_csvs)
