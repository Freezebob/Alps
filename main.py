#!/home/marco/anaconda2/bin/python
#-*- coding: utf-8 -*-

import os, shutil, argparse
from Omni
# Parsing delgi argomenti da linea di commando
parser = argparse.ArgumentParser(description="Tool di analisi metadati di dataset.")
parser.add_argument("directory",help="Pathname della directory contenente i dataset da analizzare")
parser.add_argument("-f","--files",dest="files_list",metavar="NN",type=string,nargs="*",default=[],help='Lista dei file da esaminare')
parser.add_argument("-d","--deps",dest="deps_list",metavar="N",type=string,nargs="*",default=["STATS","FDS","UCCS","INDS"],help='Lista delle dipendenze da calcolare (default: ["STATS","FDS","UCCS","INDS"]')
# parser.add_argument("-v","--verbose",action="store_true",default=False,help="Mostra nel dettaglio l'avanzamento dell'elaborazione")
args = parser.parse_args()
directory = args.directory
files_list = args.files_list
deps_list = args.deps_list
