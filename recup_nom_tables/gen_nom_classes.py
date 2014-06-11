import sys
from argparse import ArgumentParser

descr = """Permet de générer le nom des classes Saada grace aux noms de table"""

parser = ArgumentParser(description=descr)
parser.add_argument('liste_collec.txt', help="Le fichier contenant la liste des collections saada")
parser.add_argument('f_out.sql', help="Le fichier SQL de sortie")
parser.parse_args()

# ouverture des fichiers :
f_ls_collec = open(sys.argv[1], "r")
f_out = open(sys.argv[2], "w")

# extraction des données :
liste_tab = f_ls_collec.readlines()

# on ecrit dans le fichier out :
cpt = 0
for t in liste_tab :
    f_out.write("img" + t[:-7])
    f_out.write("\n")
    cpt+=1

# on ferme les fichiers ouverts :
f_ls_collec.close()
f_out.close()

print(cpt, "lignes générées !")
