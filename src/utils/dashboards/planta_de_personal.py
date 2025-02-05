from time import time
from os import path


def etl(algo, warehouse):
    path_file = path.join(warehouse, f"{round(time())}.txt")
    with open(path_file) as file:
        for i in range(10000):
            file.write(f"{i}\n")


"""

dimBussines : data from gn_empre

bi_emple
nm_icimp
nm_conce
nm_icimf
nm_ctaco
nm_gacon
nm_entid
SO_AREAS
gn_empre
nm_contr
gn_ccost
SO_CODIA
bi_cargo
dm_diasn
nm_incap
nm_ausen
nm_mause
nm_autol
nm_gacco
NM_DISEV
nm_acumu
NM_PROPA
nm_bepro
SL_REQPE
RL_MOTSO

"""
