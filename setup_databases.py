import bw2io as bi
from premise import *
from tasks import *
import bw2data as bd

# setup_databases
bd.projects.set_current('calliope_enbios_bw2')
bi.bw2setup()

SPOLDS_CUTOFF = r"C:\ecoinvent_data\3.9.1\cutoff\datasets"
SPOLDS_APOS = r"C:\ecoinvent_data\3.9.1\apos\datasets"

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
ndb = NewDatabase(
    scenarios=[
        {"model": "image", "pathway": "SSP2-RCP19", "year": 2020},
    ],
    source_db="cutoff391",
    source_version="3.9.1",
    key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo='
)
ndb.write_db_to_brightway(name='premise_base')

# set the background
# TODO:
#  1. cement: assume CCS
#  2. Biomass:
#  3. steel and iron: look for inventories
#  4. plastics:
#  5. ammonia and methanol
#  6. transport
#  7. electricity
#  8. heat
#  9. rails only electric (Europe)


# 1. set the foreground
# 1.1 update inventories
# TODO: maybe database arrangement changes to have only a single database
chp_waste_update(db_waste_name='apos391', db_original_name='cutoff391',
                       locations=consts.LOCATION_EQUIVALENCE.values())
biofuel_to_methanol_update(db_methanol_name='premise_base')
trucks_update(db_truck_name='premise_base')
passenger_car_update(db_passenger_name='premise_base')
gas_to_liquid_update(db_cobalt_name='cutoff391', db_gas_to_liquid_name='premise_base')

# 1.2 create fleets
solar_pv_fleet(db_solar_name='premise_base')


