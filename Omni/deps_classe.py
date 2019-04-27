#!/home/marco/anaconda2/bin/python
#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import re
import metanome_api

# Classi per le varie dipendenze

class DEP(object):
    def __init__(self, lhs, rhs):
        if lhs == ['']:
            lhs = []
        self.lhs = lhs
        self.rhs = rhs

#     def __init__(self, x):
#         self.lhs = x.lhs
#         self.rhs = x.rhs

    def __hash__(self):
        return hash((tuple(self.lhs), tuple(self.rhs)))  # Passo dalle liste alle tuple per renderle hashabili
                                                         # Pare funzioni

    def __eq__(self, other):
        #print "uso eq!!!!1122333"
        return ((set(self.lhs), set(self.rhs)) == (set(other.lhs), set(other.rhs)))


#         return ((set(self.lhs), set(self.rhs)) == (set(other.lhs), set(other.rhs)) or
#                 (
#                   (set(self.lhs) <= set(other.lhs) and set(self.rhs) == set(other.rhs))
#                   or
#                   (set(other.lhs) <= set(slef.lhs) and set(self.rhs) == set(other.rhs))
#                 )
#                )
    def __le__(self, other): #Se self.lhs è un sottinsieme di other.rhs
        #print "uso le"
        return ((set(self.lhs) <= set(other.lhs)) and (set(self.rhs) == set(other.rhs)))

    def __ge__(self, other): #Se other.lhs è un sottinsieme di self.rhs
        #print "uso ge"
        return ((set(other.lhs) <= set(slef.lhs)) and (set(self.rhs) == set(other.rhs)))

    def __ne__(self, other):
        return not self.__eq__(other)

class FD(DEP):
    def __str__(self):
        return str(self.lhs) + " -> " + str(self.rhs)

class IND(DEP):
    def __str__(self):
        return str(self.lhs) + " ⊆ " + str(self.rhs)

class ORD(DEP):
    def __init__(self, lhs, rhs, order_type, comp):
        DEP.__init__(self, lhs, rhs)
        self.order_type = order_type
        self.comp = comp
    def __str__(self):
        return str(self.lhs) + " ~> " + str(self.rhs) + str(self.order_type) + str(self.comp)

class UCC(object): # UCC ha un solo attributo, quindi non lo faccio d erivare da DEP
    def __init__(self, comb):
        self.comb = comb
    def __hash__(self):
        return hash(tuple(self.comb))
    def __eq__(self, other):
        return set(self.comb) == set(other.comb)
    def __ne__(self, other):
        return not self.__eq__(other)
    def __str__(self):
        return str(self.comb)


# Pensiero di qualche funzione per rendere il codice più elegante, IN LAVORAZIONE
# PER ORA VA BENE COSì

# Lettura delle dipendenze

def create_dep(lhs, rhs, dep_type):
    if dep_type == "fds":
        return FD(lhs, rhs)
    elif dep_type == "inds":
        return IND(lhs, rhs)

def read_dep(file_name): # Il caso stats è a parte
    dep_type = re.sub(r'.*_', '', file_name)
    if dep_type == "uccs": # Caso UCC
        return read_uccs(file_name)
    columns = {}
    deps = []
    results = False
    f = open(file_name, "r")
    if dep_type == "fds": # Caso FD
        symbol = "->"
    elif dep_type == "inds": # Caso IND
        symbol = "[="

    for line in f:
        if line[0:2] == "1.":
            columns[line.split()[1]] = (line.split()[0][2:])
        elif line == "# RESULTS\n":
            results = True
        elif results == True:
            both = line.rstrip("\n").split(symbol)
            lhs = both[0].split(",") # Lo split mi crea una lista. Il problema è che l'ordine dei suoi elementi così conta, ma in verità un lhs [1,2] è uguale ad un [2,1]
            rhs = both[1].split(",")
            deps.append(create_dep(lhs, rhs, dep_type))
    return columns, deps

def read_stats(file_name):
    #with open('ris', 'r') as myfile:
    with open(file_name, 'r') as myfile:
        data=myfile.read().replace('\n', '')
    ris = pd.DataFrame()
    for obj in metanome_api.decode_stacked(data):
        tmp = pd.DataFrame.from_dict(obj["statisticMap"])
        tmp["columnIdentifier"] = obj["columnCombination"]["columnIdentifiers"][0]["columnIdentifier"]
        #print tmp
        #ris = ris.append(pd.DataFrame.from_dict(obj["statisticMap"]))
        ris = ris.append(tmp)
    ris = ris[ris.index == "value"]
    ris.index = ris["columnIdentifier"]
    return ris

def read_uccs(file_name):
    f = open(file_name, "r")
    columns = {}
    UCCs = []
    results = False
    for line in f:
        if line[0:2] == "1.":
            columns[(line.split()[0][2:])] = line.split()[1]
        elif line == "# RESULTS\n":
            results = True
        elif results == True:
            #print UCC(line.rstrip("\n").split(","))
            UCCs.append(UCC(line.rstrip("\n").split(",")))
            #print UCCs[0]
    return columns, UCCs
