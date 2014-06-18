
import psycopg2


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


def getContenuColonne(curseur, table, nom_colonne1, nom_colonne2, nom_colonne3, nom_colonne4 ):
    """Fonction qui récupère l'oidsaada + le contenu d'une colonne et qui 
    le retourne sous forme de liste de tuples
    [(oidsaada, contenu), (oidsaada, contenu), ... (oidsaada, contenu)]"""
     
    curseur.execute("""
        select oidsaada, {}, {}, {}, {}
        from {};""".format(nom_colonne1,nom_colonne2,nom_colonne3,nom_colonne4, table))
    return curseur.fetchall()




# connexion à la BDD :
try :
    conn = psycopg2.connect("dbname='VizieR' user='postgres' host='localhost' password='reverser'")
    print("Connection à la BDD établie")
except :
    print("Erreur lors de la connection à la BDD")
    exit(1)
cur = conn.cursor()



liste_classes = getListeClassesAvecLaColonne(cur, "_cdelt3")


# on cherche la liste des classes Saada contenant la colonne x :
ls_restfreq = getListeClassesAvecLaColonne(cur, "_restfreq")
ls_restfrq = getListeClassesAvecLaColonne(cur, "_restfrq")
ls_freq = getListeClassesAvecLaColonne(cur, "_freq")
ls_frequ = getListeClassesAvecLaColonne(cur, "_frequ")
ls_freque = getListeClassesAvecLaColonne(cur, "_freque")
ls_frequen = getListeClassesAvecLaColonne(cur, "_frequen")
ls_frequenc = getListeClassesAvecLaColonne(cur, "_frequenc")

# on cherche le contenu de ces colonnes :
res = []
for classe in liste_classes :
    classe = classe.lower()
    if classe in ls_restfreq :
        res += getContenuColonne(cur, "img"+classe, "_cdelt3", "_crval3", "_ctype3", "_restfreq")
    if classe in ls_restfrq :
        res += getContenuColonne(cur, "img"+classe, "_cdelt3", "_crval3", "_ctype3", "_restfrq")
    if classe in ls_freq :
        res += getContenuColonne(cur, "img"+classe, "_cdelt3", "_crval3", "_ctype3", "_freq")
    if classe in ls_frequ :
        res += getContenuColonne(cur, "img"+classe, "_cdelt3", "_crval3", "_ctype3", "_frequ")
    if classe in ls_freque :
        res += getContenuColonne(cur, "img"+classe, "_cdelt3", "_crval3", "_ctype3", "_freque")
    if classe in ls_frequen :
        res += getContenuColonne(cur, "img"+classe, "_cdelt3", "_crval3", "_ctype3", "_frequen")
    if classe in ls_frequenc :
        res += getContenuColonne(cur, "img"+classe, "_cdelt3", "_crval3", "_ctype3", "_frequenc")
        
# puis on l'insert dans notre table obscore :
for tu in res :
    concat = str(tu[1]) + ' # ' + str(tu[2]) + ' # ' + str(tu[3]) + ' # ' + str(tu[4])
    cur.execute("update obscore set frequency='" + concat + 
                "' where obscore.oidsaada=" + str(tu[0]) + ";")


conn.commit()
print("OK")

