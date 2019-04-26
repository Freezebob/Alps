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
