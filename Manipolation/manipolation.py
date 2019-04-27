#!/home/marco/anaconda2/bin/python
#-*- coding: utf-8 -*-
import copy as cp
from tkinter import *
from tkinter import ttk
import pickle
import collections
import sys, os
sys.path.append('/home/marco/github/Alps')
from Omni.deps_classe import *

# Le variabili stats, ds_names, final_dep_results calcolate nel package Omni mi servono anche qui. Per non dover rieseguire tutto le tengo salvate con pickle
def load_results():
    stats = {}
    ds_names = []
    final_dep_results = collections.defaultdict(dict)
    sys.path.append('/home/marco/github/Alps/Omni')  # Devo capire bene questo problema con pickle
    with open("/home/marco/github/Alps/Omni/objs.pkl", "r") as f:
        stats, ds_names, final_dep_results = pickle.load(f)
        # print ds_names
        return stats, ds_names, final_dep_results

# Per l'interfaccia grafica
def get_selection(ds_names, ds_list_box):
    # print "ciao"
    selected_options = [ds_names[int(item)] for item in  ds_list_box.curselection()]
    return selected_options


# Interfaccia grafica di prova. Da aggiustare le dimensioni

def ds_list(ds_names):
    main = Tk()
    main.title('Test GUI')
    main.geometry('400x400')

    nb = ttk.Notebook(main)
    nb.grid(row=1, column=0, columnspan=50, rowspan=49, sticky='NESW')

    page1 = ttk.Frame(nb)
    nb.add(page1, text='Dataset names')

    ds_list_box = Listbox(page1)
    ds_list_box.configure(selectmode=MULTIPLE, width=9, height=5)
    ds_list_box.grid(row=0, column=0)

    btnGet = Button(page1,text="Get Selection",command= lambda: get_selection(ds_names, ds_list_box))
    btnGet.grid()

    for name in ds_names:
        ds_list_box.insert(END, name)

    main.mainloop()

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
                elif i <= j and j not in ris:
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


def intersection_some_ign(dep_type, dep_results, datasets):
    dep_intersection = []
    first = True
    for name in datasets:
        print name
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


# Scrematura delle dipendenze, tolgo le nulle e quelle riconducibili ad una UCC (per lhs ho una UCC oppure un suo sovrainsieme)
def deps_screm(final_dep_results, ds_names):
    final_dep_results_copy = cp.deepcopy(final_dep_results)
    for name in ds_names:
        print name
        for i in final_dep_results[name]["fds"]:
            #print i.rhs[0]
            #print "Perc " + str(stats[name]["Percentage of Nulls"][attributes[i.rhs[0]]])
            if stats[name]["Percentage of Nulls"][attributes[i.rhs[0]]] == 100:
                #print i
                #print "Ha {} nullo".format(i.rhs)
                final_dep_results_copy[name]["fds"].remove(i)  # Il remove creava dei problemi col for i, scombussolando l'ordine
                #print "-------"
            for j in final_dep_results[name]["uccs"]:
                if set(i.lhs) == set(j.comb) or set(i.lhs) >= set(j.comb):   #Trovo lo stesso caso mille volte ['1'] e ['1'], ['1'] e ['1'] eccc
                    #print "lhs: {}, ucc: {}".format(i.lhs, j.comb)           #Penserò ad una soluzione
                    #print "-------"
                    final_dep_results_copy[name]["fds"].remove(i)
    return final_dep_results_copy


# opened_csvs contiene tutti i csv pronti per essere analizzati
def csvs(csvpath, ds_names):
    opened_csvs = {}
    #csvpath = "/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/"
    onlyfiles = [f for f in listdir(csvpath) if isfile(join(csvpath, f))]
    for f in onlyfiles:
        only_name = re.sub(r'.*_', '', f).split('.')[0]
        only_name = re.sub('[(){}<>]', '', only_name)
        print only_name.lower()
        opened_csvs[only_name.lower()] = pd.read_csv('/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/' + f, sep=',', dtype=dtypes_dict)
    #     for i in opened_csvs[only_name.lower()].columns:
    #         print i
    #         opened_csvs[only_name.lower()][i] = opened_csvs[only_name.lower()][i].astype(str)

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
def exclusive_deps(selected_options, final_dep_results_copy):
    exclusive_deps = collections.defaultdict(dict)
    for ds in selected_options:
        tmp = []
        for i in final_dep_results_copy[ds]["fds"]:
            if i not in inter_fds:
                p = False
                for y in inter_fds:
                    if i <= y:
                        p = True
                if p == False:
                    tmp.append(i)
        exclusive_deps[ds]["fds"] = tmp
    return exclusive_deps


# Il mio groupby
def bing_bing_bong_new(a_list, ds):
#     start = time.time()
    dict_df = opened_csvs[ds][[attributes[i] for i in a_list]].to_dict("split")
    dict_df_data = dict_df["data"]

    result = collections.defaultdict(lambda: collections.defaultdict(int))

    for row in dict_df_data:
        result[str(row[:-1])][row[-1]] += 1
    for key in result.keys():
        if len(result[key]) == 1:
            result.pop(key, None)
    return result


def process_function(lim1, lim2, lim1_d, lim2_d):
#     scre_dict = rec_dd()
    #deps_screm_nuovo_process = rec_dd()
    client_p = pm.MongoClient()
    db = client_p["Deps_db"]
    fds_collection = db["FDS"]
    seg_list = []
    for ds1 in selected_options:
            for ds2 in selected_options:
                if ds1 != ds2:
                    tmp = []
                    if ds2 == "alpsv20dedup":
                        l1 = lim1_d
                        l2 = lim2_d
                    else:
                        l1 = lim1
                        l2 = lim2
                    for i in exclusive_deps[ds2]["fds"][l1:l2]:
                        #print "controllo {} di {} su {}".format(i, ds2, ds1)
                        if stats[ds1]["Percentage of Nulls"][attributes[i.rhs[0]]] != 100:
                            new_i = deepcopy(i)
                            for x in i.lhs:
                                #print "esamino {}".format(x)
                                #print stats[ds1]["Percentage of Nulls"][attributes[x]]
                                #print "-----"
                                if stats[ds1]["Percentage of Nulls"][attributes[x]] == 100:
                                    new_i.lhs.remove(x)
                            #print new_i
                            if new_i not in tmp:
                                tmp.append(new_i)

                    #scre_dict[ds1][ds2] = np.array(tmp, dtype=FD) #à stesso discorso: provo su ds1 le dip esclusive di ds2,
                                              #ma queste sono scremate sulla struttura di ds1
                    #for dep in tmp:
                        #print "ds1: {}, ds2: {}. Dep: {}".format(ds1, ds2, dep)
                    t_dict = {
                            "test_ds": ds1,
                            "source_ds": ds2,
                            "dependencies": {},
                            "count": 0
                        }
#                     print("t_dict: {}".format(t_dict))
                    for dep in tmp:
                        #tmp_dict = {}
                        #print dep
                        #a_list = [el for el in dep.lhs]
                        #print a_list
                        a_list = dep.lhs
                        #print a_list
                        a_list.append(dep.rhs[0])
                        #print "a_list: {}".format(a_list)
                        if len(a_list) > 1:
                            #tmp_dict[str(a_list)] = bing_bing_bong(a_list, ds1)
                            #deps_screm[ds1][ds2]["fds"] = tmp_dict
                            result = bing_bing_bong_new(a_list, ds1)
                            if result:
                                a = [[result.values()[i].keys(), result.values()[i].values()] for i in xrange(len(result))]
#                                 deps_screm_nuovo_process[ds1][ds2]["fds"][str(a_list)] = np.array([np.array(result.keys()), a])
                                t_dict["dependencies"][str(a_list)] = [result.keys(), a]
                            else:
                                t_dict["dependencies"][str(a_list)] = []

                    for k in t_dict["dependencies"].keys():
                        #t_dict["dependencies"]["dependencies."+k] = t_dict["dependencies"].pop(k)
                        fds_collection.update_one(
                            {"test_ds": ds1, "source_ds": ds2, "count": {"$lt": 200}},
                            {
                                "$set": {"dependencies."+k: t_dict["dependencies"][k]},
                                "$inc": {"count": 1}
                            },
                            upsert=True
                        )


def create_slides(n, n_slides):
    ris = [i for i in xrange(0, n, n_slides)]
    ris.append(n)
    return ris


r = create_slides(len(exclusive_deps["alpsv20dedup"]["fds"]), (len(exclusive_deps["alpsv20dedup"]["fds"]) / 4) + 1)
slides_d = [[i,j] for i, j in zip(r[:-1], r[1:])]


r = create_slides(len(exclusive_deps["alpsv20"]["fds"]), (len(exclusive_deps["alpsv20"]["fds"]) / 4) + 1)
slides = [[i,j] for i, j in zip(r[:-1], r[1:])]


processes = []
# for i in xrange(len(slides)):
for i in xrange(len(slides)):
    processes.append(mp.Process(target=thread_function, args=(slides[i][0], slides[i][1], slides_d[i][0], slides_d[i][1])))


start = time.time()
for x in processes:
    x.start()
# results = [output.get() for p in xrange(len(slides))]
for x in processes:
    x.join()
end = time.time()
print(end-start)


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


for col in xrange(len(results[0])):
    tot = rec_dd()
    for row in results:
        dict_merge(tot, row[col])
    fds_collection.insert_one(tot)


if __name__ == "__main__":
    stats, ds_names, final_dep_results = load_results()
    final_dep_results_copy = deps_screm(final_dep_results, ds_names)
    csvpath = "/home/marco/Scrivania/dep/backend/WEB-INF/classes/inputData/"
    opened_csvs = csvs(csvpath, ds_names)
    ds_list(ds_names)
