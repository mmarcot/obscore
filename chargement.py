# Script python qui centralise l'ensemble des requêtes SQL permettant
# de charger la table obscore

import psycopg2
import datetime
import os.path


def logMe(ch, display=True):
    """Fonction qui permet d'écrire dans le fichier de log et 
    d'afficher le message ou non"""
    if display :
        print(ch)
    fl = open("log.txt", "a")
    str_time = str(datetime.datetime.now())
    fl.write("--------------------------------------------------------------------\n")
    fl.write(str_time + " ==> " + ch + "\n")
    fl.close()
    
    
def epurer(p_str):
    """Fonction qui enlève les caractères non voulus"""
    p_str.replace("\n", " ")
    p_str.replace("\t", " ")
    p_str.replace("\r", " ")
    
    return p_str
    
    
    


try :
    conn = psycopg2.connect("dbname='VizieR' user='postgres' host='localhost' password='reverser'")
    logMe("Connection établie")
except :
    logMe("Erreur lors de la connection à la BDD")
    exit(1)

cur = conn.cursor()

# on vide la BDD :
cur.execute("DELETE FROM obscore;")


# ############## 1_insert.sql :
f = open(os.path.join("sql", "1_insert.sql"))
liste_req = epurer(f.read()).split(";")
for req in liste_req[:-1] :
    cur.execute(req)
    logMe(cur.query.decode("UTF-8"))
f.close()


# ############## 2_obs_collection :
f = open(os.path.join("sql", "2_obs_collection.sql"))
cur.execute(f.read())
f.close()


# ############## 3_dataproduct_type :
f = open(os.path.join("sql", "3_dataproduct_type.sql"))
liste_req = epurer(f.read()).split(";")
for req in liste_req[:-1] :
    cur.execute(req)
    logMe(cur.query.decode("UTF-8"), False)
f.close()





# validation du travail :
conn.commit()
logMe("Commit", False)





























