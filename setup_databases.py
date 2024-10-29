import sys

import bw2io as bi
from premise import *
from tasks import *
import bw2data as bd
import pandas as pd

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
        {"model": "image", "pathway": "SSP2-RCP19", "year": 2050},
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
                 locations=['CH'])
biofuel_to_methanol_update(db_methanol_name='premise_base')
trucks_and_bus_update(db_truck_name='premise_base')
passenger_car_update(db_passenger_name='premise_base')
gas_to_liquid_update(db_cobalt_name='cutoff391', db_gas_to_liquid_name='premise_base')
biofuel_to_methane_infrastructure(db_syn_gas_name='cutoff391')
hp_update(db_hp_name='cutoff391')
hydro_run_of_river_update(db_hydro_name='cutoff391')
for location in ['FR', 'DE']:
    hydro_reservoir_update(location=location, db_hydro_name='cutoff391')
airborne_wind_lci(bd_airborne_name='cutoff391')

# 1.2 create fleets
solar_pv_fleet(db_solar_name='premise_base')
hydrogen_from_electrolysis_market(db_hydrogen_name='premise_base',
                                  soec_share=0.5, aec_share=0.3, pem_share=0.2)  # TODO: propose relevant fleets
batteries_fleet(db_batteries_name='premise_base', scenario='tc', technology_share=None)
# wind fleets created in Germany
wind_onshore_fleet(db_wind_name='cutoff391', location='DE', fleet_turbines_definition={'turbine_1': [
        {
            'power': 4.0, 'manufacturer': "Vestas", 'rotor_diameter': 100, 'hub_height': 120,
            'commissioning_year': 2030,
            'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
        }, 0.5],
        'turbine_2': [
            {
                'power': 6.0, 'manufacturer': 'Vestas', 'rotor_diameter': 120, 'hub_height': 140,
                'commissioning_year': 2030,
                'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
            },
            0.5]})
wind_offshore_fleet(db_wind_name='cutoff391', location='DE', fleet_turbines_definition={'turbine_1': [
        {
            'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'gravity', 'floating_platform': None, 'sea_depth': 5, 'distance_to_shore': 30
        }, 0.05],  # based on the SG 14-222 DD
        'turbine_2': [
        {
            'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'gravity', 'floating_platform': None, 'sea_depth': 5, 'distance_to_shore': 30
        }, 0.05],  # based on the V164-10MW
        'turbine_3': [
        {
            'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'monopile', 'floating_platform': None, 'sea_depth': 30, 'distance_to_shore': 30
        }, 0.2],
        'turbine_4': [
        {
            'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'monopile', 'floating_platform': None, 'sea_depth': 30, 'distance_to_shore': 30
        }, 0.2],
        'turbine_5': [
        {
            'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'tripod', 'floating_platform': None, 'sea_depth': 45, 'distance_to_shore': 30
        }, 0.1],
        'turbine_6': [
        {
            'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'tripod', 'floating_platform': None, 'sea_depth': 45, 'distance_to_shore': 30
        }, 0.1],
        'turbine_7': [
        {
            'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'floating', 'floating_platform': 'spar_buoy_steel', 'sea_depth': 45,
            'distance_to_shore': 30
        }, 0.15],
        'turbine_8': [
        {
            'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'floating', 'floating_platform': 'spar_buoy_steel', 'sea_depth': 45,
            'distance_to_shore': 30
        }, 0.15]
        })


# 1.3 delete infrastructure and leave all activities ready in 'additional_acts'
delete_infrastructure_main(file_path=r'C:\Users\mique\OneDrive - UAB\PhD_ICTA_Miquel\research stay Delft\technology_mapping_clean.xlsx')
