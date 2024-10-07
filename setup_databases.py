import bw2data as bd
import bw2io as bi
from premise import *
import wurst.searching as ws
# 0. virtual environment: enbios, sparks, premise (within enbios)
# 0bis. cut-off and apos.

bd.projects.set_current('calliope_enbios')
bi.bw2setup()

SPOLDS_CUTOFF = r"C:\bw2\Ben_course\ecoinvent 3.9_cutoff_ecoSpold02\datasets"
SPOLDS_APOS = r""

# Ecoinvent v3.9.1 cutoff and apos
if 'cutoff391' not in bd.databases:
    ei = bi.SingleOutputEcospold2Importer(SPOLDS_CUTOFF, "cutoff391", use_mp=False)
    ei.apply_strategies()
    ei.write_database()
if 'apos391' not in bd.databases:
    ei = bi.SingleOutputEcospold2Importer(SPOLDS_APOS, "apos391", use_mp=False)
    ei.apply_strategies()
    ei.write_database()

# premise, without updates (only imported inventories)
# TODO: check if we need updates
ndb = NewDatabase(
    scenarios=[
        {"model": "image", "pathway": "SSP2-RCP19", "year": 2020},
    ],
    source_db="cutoff391",
    source_version="3.9.1",
    key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo='
)
ndb.write_db_to_brightway(name='premise_base')

