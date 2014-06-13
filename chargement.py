# Script python qui centralise l'ensemble des requêtes SQL permettant
# de charger la table obscore

import psycopg2
import datetime
import dateutil.parser
import os.path
from math import sqrt, pow


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
    
    
def getListeClassesAvecLaColonne(curseur, nom_colonne):
    """Fonction qui à partir d'un nom de colonne renvoie l'ensemble des
    tables qui contiennent une telle colonne"""
    curseur.execute("""
        select name_coll 
        from saada_metaclass_image 
        where name_attr like '{}'
        """.format(nom_colonne))
    res = curseur.fetchall();
    ls_name_class = []
    for e in res :
        ls_name_class.append(e[0].lower())
        
    return ls_name_class


def getContenuColonne(curseur, table, nom_colonne):
    """Fonction qui récupère l'oidsaada + le contenu d'une colonne et qui 
    le retourne sous forme de liste de tuples
    [(oidsaada, contenu), (oidsaada, contenu), ... (oidsaada, contenu)]"""
     
    curseur.execute("""
        select oidsaada, {}
        from {};""".format(nom_colonne, table))
    return curseur.fetchall()


def calculerJours(p_date):
    """Fonction qui calcul le nombre de jours entre la date passée en paramêtre et la 
    date repère
    Le paramètre doit etre de la classe datetime
    """
    date_repere = datetime.datetime(1858,11,17) # MJD : 17 nov 1858 à 0h00 
    duree = p_date - date_repere
    
    return (duree.days + (duree.seconds / (60*60*24)))
    

        
        


    

# connexion à la BDD :
try :
    conn = psycopg2.connect("dbname='VizieR' user='postgres' host='localhost' password='reverser'")
    logMe("Connection à la BDD établie")
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
exec_obs_publisher_did = False
exec_obs_creator_name = False
exec_t_min = False
exec_t_exposure_time = False
exec_access_estsize = False
exec_s_resolution = True


# chargement de la liste des collections (.._image) et des classes (imgj_aa_ ..) :
f_class = open("liste_classes_db.txt")
f_collec = open("liste_collec_db.txt")
liste_classes = f_class.readlines()
liste_collec = f_collec.readlines()
for i in range(len(liste_classes)) :
    liste_classes[i] = liste_classes[i].strip()
for i in range(len(liste_collec)) :
    liste_collec[i] = liste_collec[i].strip()
f_class.close()
f_collec.close()



# on vide la BDD :
if exec_vider or exec_all :
    cur.execute("DELETE FROM obscore;")
    conn.commit()
    logMe("Commit vidage BDD [OK]")



# ############## 1_insert ############## :

if exec_insert or exec_all :
    req = """
        insert into obscore(oidsaada, sky_pixel_csa, s_ra, s_dec, access_url, s_region, s_fov, access_format)
            select oidsaada, sky_pixel_csa, pos_ra_csa, pos_dec_csa, product_url_csa, shape_csa, (size_alpha_csa+size_delta_csa)/2, 'image/fits' 
            from {table};
        """
    for collec_image in liste_collec :
        logMe(req.format(table=collec_image), False)
        cur.execute(req.format(table=collec_image))
    conn.commit()
    logMe("Commit insert [OK]")



# ############## 2_obs_collection ############## :

if exec_obs_collection or exec_all :
    req = """
        update obscore
        set obs_collection = replace(replace((select collection
        from saada_loaded_file
        where obscore.oidsaada = saada_loaded_file.oidsaada),
        '_', '/'), 'AA', 'A+A');
        """
    logMe(req, False)
    cur.execute(req)
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
    res = []
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
    
    
    
# ############## 6_obs_creator_name ############## :

if exec_obs_creator_name or exec_all :

    # on cherche la liste des classes Saada contenant la colonne x :
    ls_name_class_ori = getListeClassesAvecLaColonne(cur, "_origin")
    ls_name_class_crea = getListeClassesAvecLaColonne(cur, "_creator")
    
    # on cherche le contenu de ces colonnes :
    res = []
    for classe in liste_classes :
        if classe[3:] in ls_name_class_ori :
            res += getContenuColonne(cur, classe, "_origin")
    
        if classe[3:] in ls_name_class_crea :
            res += getContenuColonne(cur, classe, "_creator")
    
    # puis on l'insert dans notre table obscore :
    for tu in res :
        cur.execute("update obscore set obs_creator_name='" + str(tu[1]) + 
                    "' where obscore.oidsaada=" + str(tu[0]) + ";")
    
    conn.commit()
    logMe("Commit obs_creator_name [OK]")



# ################# 7_t_min ################# :

# TODO regrouper les '_date' par catalogue pour savoir si c'est du JJ/MM/YY ou MM/JJ/YY

if exec_t_min or exec_all :
    ls_date = getListeClassesAvecLaColonne(cur, "_date")
    ls_mjd = getListeClassesAvecLaColonne(cur, "_mjd")
    ls_jd = getListeClassesAvecLaColonne(cur, "_jd")
    
    # on cherche le contenu de ces colonnes :
    res_date, res_mjd, res_jd = [], [], []
    for classe in liste_classes :
        if classe[3:] in ls_date :
            res_date += getContenuColonne(cur, classe, "_date")
        if classe[3:] in ls_mjd :
            res_mjd += getContenuColonne(cur, classe, "_mjd")
        if classe[3:] in ls_jd :
            res_jd += getContenuColonne(cur, classe, "_jd")
            
    # insertion dans la table obscore :
    for tu in res_date :
        if tu[1] :
            date_formate = dateutil.parser.parse(tu[1])
            cur.execute("update obscore set t_min ='" + str(calculerJours(date_formate)) +
                "' where obscore.oidsaada=" + str(tu[0]) + ";")
    for tu in res_mjd :
        if tu[1] :
            cur.execute("update obscore set t_min ='" + str(tu[1]) +
                "' where obscore.oidsaada=" + str(tu[0]) + ";")
    for tu in res_jd :
        if tu[1] :
            mjd = float(tu[1]) - 2400000.5
            cur.execute("update obscore set t_min ='" + str(mjd) +
                "' where obscore.oidsaada=" + str(tu[0]) + ";")
    conn.commit()
    logMe("Commit t_min [OK]")
    
    
            
# ################# 8_t_exptime ################# :

if exec_t_exposure_time or exec_all :
    ls_expo = getListeClassesAvecLaColonne(cur, "_exposure")
    
    res = []
    for classe in liste_classes :
        if classe[3:] in ls_expo :
            res += getContenuColonne(cur, classe, "_exposure")
            
    # puis on l'insert dans notre table obscore :
    for tu in res :
        if tu[1] :
            cur.execute("update obscore set t_exptime='" + str(tu[1]) + 
                        "' where obscore.oidsaada=" + str(tu[0]) + ";")
    
    conn.commit()
    logMe("Commit t_exptime [OK]")

    
    
# ################# 9_access_estsize ################# :

if exec_access_estsize or exec_all :    
    
    # on recupère l'oidsaada + l'access url de toutes le lignes de la table obscore :
    res = []
    cur.execute("""
        select oidsaada, access_url
        from obscore
        """)
    res = cur.fetchall()
    
    # puis on insert ligne par ligne dans obscore :
    for tu in res :
        if tu[1] :
            cur.execute("update obscore set access_estsize='" + str(os.path.getsize(tu[1])) +
                        "' where obscore.oidsaada=" + str(tu[0]))

    conn.commit()
    logMe("Commit access_estsize [OK]")



# ################# 10_s_resolution ################# :

if exec_s_resolution or exec_all :
    
    # on charge la liste 'res' avec l'ensemble des coordonnées de
    # chaque image :
    res = []
    for collec in liste_collec :
        cur.execute("""
            select oidsaada, cd1_1_csa, cd1_2_csa, cd2_1_csa, cd2_2_csa
            from {}""".format(collec))
        res += cur.fetchall()
        
    # on calcul le champs s_resolution :
    for tu in res :
        cd1_1 = float(tu[1])
        cd1_2 = float(tu[2])
        cd2_1 = float(tu[3])
        cd2_2 = float(tu[4])
        
        s_res = sqrt(pow(cd1_1,2)+pow(cd1_2,2)) + sqrt(pow(cd2_1,2)+pow(cd2_2,2))
        
        # .. et on l'insert dans la table obscore :
        cur.execute("""
            update obscore
            set s_resolution = {}
            where obscore.oidsaada = {}
            """.format(s_res, tu[0]))

    conn.commit()
    logMe("Commit s_resolution [OK]")



logMe("Table obscore correctement chargée")






























