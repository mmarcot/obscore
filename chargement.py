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
    
    
    

if __name__ == "__main__":
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
        logMe(req, False)
        cur.execute(req)
    f.close()
    conn.commit()
    logMe("Commit insert [OK]")
    
    
    # ############## 2_obs_collection :
    f = open(os.path.join("sql", "2_obs_collection.sql"))
    content = f.read()
    logMe(content, False)
    cur.execute(content)
    f.close()
    conn.commit()
    logMe("Commit obs_collection [OK]")
    
    
    # ############## 3_dataproduct_type :
    cur.execute("""
        select name_class
        from saada_metaclass_image
        where name_origin LIKE 'NAXIS_'
        group by name_class
        having count(*) > 2;
        """)
    liste_cube_tmp = cur.fetchall()
    
    # construction de la liste de cube :
    liste_cube = []
    for i in range(len(liste_cube_tmp)) :
        liste_cube.append(formatPourVizier(liste_cube_tmp[i][0])[3:])
        
    # construction de la liste de tous les catalogues :
    cur.execute("select distinct obs_collection from obscore;")
    liste_all = cur.fetchall()
    
    for e in liste_all :
        e_viz = formatPourVizier(e[0])
        
        if e_viz in liste_cube :
            req = """
                update obscore
                set dataproduct_type = 'cube'
                where obs_collection LIKE '{}'
                 """.format(e_viz)
            logMe(req, False)
            cur.execute(req)
        else :
            req = """
                update obscore
                set dataproduct_type = 'image'
                where obs_collection LIKE '{}'
                 """.format(e_viz)
            logMe(req, False)
            cur.execute(req)
    
    
    conn.commit()
    logMe("Commit dataproduct_type [OK]")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    














