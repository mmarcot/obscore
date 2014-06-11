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
    p_str = p_str.replace("\n", " ")
    p_str = p_str.replace("\t", " ")
    p_str = p_str.replace("\r", " ")
    
    return p_str


def formatPourVizier(ch):
    """Fonction qui formatte le nom des catalogues de BDD => VizieR"""
    ch = ch.replace("_", "/")
    ch = ch.replace("AA", "A+A")
    
    return ch


def formatPourBDD(ch):
    """Fonction qui formatte le nom des catalogues de VizieR => BDD"""
    ch = ch.replace("/", "_")
    ch = ch.replace("A+A", "AA")
    
    return ch  


def enleverExtension(ch):
    """Fonction qui permet d'enlever l'extension d'une nom de fichier FITS"""
    l = ch.lower().split('.')
    ind = -1
    try :
        ind = l.index("fit")
    except ValueError :
        try :
            ind = l.index("fits")
        except ValueError :
            pass
        
    if ind != -1 :
        ch = ""
        for e in l :
            if e == "fit" or e == "fits" :
                break
            ch = ch + "." + e
            
        return ch[1:]
    else :
        return ch
        
        
    
    


try :
    conn = psycopg2.connect("dbname='VizieR' user='postgres' host='localhost' password='reverser'")
    logMe("Connection établie")
except :
    logMe("Erreur lors de la connection à la BDD")
    exit(1)

cur = conn.cursor()

# choisir les étapes à executer :
exec_all = False
exec_vider = False
exec_insert = False
exec_obs_collection = False
exec_dataproduct_type = False
exec_obs_id = False
exec_obs_publisher_did = True


# on vide la BDD :
if exec_vider or exec_all :
    cur.execute("DELETE FROM obscore;")


# ############## 1_insert.sql ############## :
if exec_insert or exec_all :
    f = open(os.path.join("sql", "1_insert.sql"))
    liste_req = epurer(f.read()).split(";")
    for req in liste_req[:-1] :
        logMe(req, False)
        cur.execute(req)
    f.close()
    conn.commit()
    logMe("Commit insert [OK]")


# ############## 2_obs_collection ############## :

if exec_obs_collection or exec_all :
    f = open(os.path.join("sql", "2_obs_collection.sql"))
    content = f.read()
    logMe(content, False)
    cur.execute(content)
    f.close()
    conn.commit()
    logMe("Commit obs_collection [OK]")


# ############## 3_dataproduct_type ############## :

if exec_dataproduct_type or exec_all :
    # construction de la liste des catalogues contenant des cubes :
    cur.execute("""
        select name_class
        from saada_metaclass_image
        where name_origin LIKE 'NAXIS_'
        group by name_class
        having count(*) > 2;
        """)
    liste_cube_tmp = cur.fetchall()
    liste_cube = []
    for i in range(len(liste_cube_tmp)) :
        liste_cube.append(formatPourVizier(liste_cube_tmp[i][0])[3:])
        
    # construction de la liste de tous les catalogues :
    cur.execute("select distinct obs_collection from obscore;")
    liste_all_tmp = cur.fetchall()
    liste_all = []
    for i in range(len(liste_all_tmp)) :
        liste_all.append(liste_all_tmp[i][0])
    
    # pour chaque catalogue on met si il contient des cubes ou
    # des images :
    for e in liste_all :
        if e in liste_cube :
            req = """
                update obscore
                set dataproduct_type = 'cube'
                where obs_collection LIKE '{}'
                 """.format(e)
            logMe(req, False)
            cur.execute(req)
        else :
            req = """
                update obscore
                set dataproduct_type = 'image'
                where obs_collection LIKE '{}'
                 """.format(e)
            logMe(req, False)
            cur.execute(req)
    
    conn.commit()
    logMe("Commit dataproduct_type [OK]")



# ############## 4_obs_id ############## :
if exec_obs_id or exec_all :
    cur.execute("""
        select oidsaada, filename
        from saada_loaded_file
        """)
    res = cur.fetchall()
    
    for tu in res :
        cur.execute("update obscore set obs_id='" + enleverExtension(tu[1]) + 
                    "' where obscore.oidsaada=" + str(tu[0]) + ";")
    
    conn.commit()
    logMe("Commit obs_id [OK]")


# ############## 5_obs_publisher_did ############## :
if exec_obs_publisher_did or exec_all :
    cur.execute("""
        update obscore
        set obs_publisher_did = 'ivo://cds/vizier/' || obs_id;
        """)
    conn.commit()
    logMe("Commit obs_publisher_did [OK]")




logMe("Table obscore correctement chargée")






























