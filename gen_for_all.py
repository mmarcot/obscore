import sys
from argparse import ArgumentParser

descr = """Permet de générer une requête SQL contenue dans un fichier pour toute une liste de tables"""

parser = ArgumentParser(description=descr)
parser.add_argument('req.sql', help="Le fichier contenant la requete à dupliquer")
parser.add_argument('liste_tables.txt', help="Le fichier contenant la liste des tables")
parser.add_argument('f_out.sql', help="Le fichier SQL de sortie")
parser.parse_args()

# ouverture des fichiers :
f_req = open(sys.argv[1], "r")
f_ls_tab = open(sys.argv[2], "r")
f_out = open(sys.argv[3], "w")

# extraction des données :
str_req = f_req.read()
liste_tab = f_ls_tab.readlines()

# on ecrit dans le fichier out :
cpt = 0
for t in liste_tab :
  f_out.write(str_req.format(table=t[:-1]))
  f_out.write("\n")
  cpt+=1

# on ferme les fichiers ouverts :
f_req.close()
f_ls_tab.close()
f_out.close()

print(cpt, "requêtes générées !")
