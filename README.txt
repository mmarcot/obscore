##########################################################################
##                                                                      ##
##                   CHARGEMENT TABLE OBSCORE                           ##
##                          script chargment.py                         ##
##########################################################################


Le script fonctionne UNIQUEMENT si : 
  - une collection = un classe Saada
  - la classe à le meme nom que la collection excepté qu'elle est précédée des 3 lettres 'img'
  - psycopg2 et python-dateutil sont correctement installés


Le script chargement.py à besoin pour charger la table obscore :

- du fichier nommé 'liste_classes_db.txt' contenant le listes de toutes les classes de Saada (séparé par des retours à la ligne)
  ex : imgj_aa_551_a2

- du fichier nommé 'liste_collec_db.txt' contenant la liste de toutes les tables Saada finissant par _image (séparé par des retours à la ligne)
  ex : j_aa_551_a2_image
