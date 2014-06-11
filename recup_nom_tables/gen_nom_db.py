import sys, os
from argparse import ArgumentParser

descr = """Permet de recupérer tous les noms de collection d'une BDD Saada """

parser = ArgumentParser(description=descr)
parser.add_argument('DB_name', help="Nom de la BDD dans Saada")
parser.add_argument('-o', "--output", help="Nom du fichier de sortie, \"out.txt\" par défaut")
parser.add_argument('-l', "--repository", help="Lien vers le repository Saada si différent de ~/saada_workspace/repository")
args = vars(parser.parse_args())

# nom du fichier de sortie en fonction des arguments passés :
if args['output'] :
  name_f_out = args['output']
else :
  name_f_out = "out.txt"

# path du repository en fonction des arguments passés :
if args["repository"] :
  path_repo = args["repository"]
else :
  path_repo = os.path.join(os.path.expanduser("~/saada_workspace/repository/" + sys.argv[1])) 

f_out = open(name_f_out, "w")
lines = os.listdir(path_repo)

liste_conf = ["config", "dms", "embeddeddb" , "indexrelations", "logs", "tmp",
    "voreports"]

for l in lines :
  if l in liste_conf :
    continue
  string = l[:-1] + "_image\n"
  f_out.write(string.lower())

f_out.close()

