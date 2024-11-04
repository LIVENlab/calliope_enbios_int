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
if 'original_cutoff391' not in bd.databases:
    ei = bi.SingleOutputEcospold2Importer(SPOLDS_CUTOFF, "original_cutoff391", use_mp=False)
    ei.apply_strategies()
    ei.write_database()
if 'apos391' not in bd.databases:
    ei = bi.SingleOutputEcospold2Importer(SPOLDS_APOS, "apos391", use_mp=False)
    ei.apply_strategies()
    ei.write_database()
# create a copy of cutoff391
if "cutoff391" not in bd.databases:
    bd.Database('original_cutoff391').copy("cutoff391")

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
# TODO: Ecoinvent will NEVER be used. We use premise_base or additional_acts ALWAYS. Additional_acts activities should never have cutoff391 as initial database. We can leave 'Ecoinvent' in 'technology_mapping_clean', but we should add a line in the code to transform that into 'premise_base'
# TODO: database handling should be as follows:
# 1. Import cutoff and apos
# 2. Import premise_original
# 3. Create a copy of premise_original named premise_base (so we can also do analysis WITHOUT background changes using premise_original)
# -> infrastructure analysis possible. Without background changes
# 4. Apply background changes to premise_base
# -> infrastructure analysis possible. With background changes
# 5. Apply foreground changes
# -> O&M analysis possible. WITHOUT infrastructure, but with all inputs.
# 6. Apply avoid_double accounting
# -> O&M analysis possible. WITHOUT infrastructure, AND WITHOUT carrier inputs.



# 1. set the background
# 1.1. Unlink carrier activities
def avoid_double_accounting():
    """
    We use the polluter pays principle to avoid double accounting. There are two possible sources of double accounting.
    Let's break them down using electricity production as an example:
    1. Calliope calculates the demand for electricity in Europe. We will calculate the impacts of the technologies that
    produce electricity to satisfy this demand. Thus, when calculating the impacts of other technologies within the
    electricity system (e.g., electricity used in electrolysers for hydrogen production), we should not count the
    impacts of this electricity again. (delete internal links)
    2. Calliope assumes electricity is produced with certain technologies. However, in Ecoinvent, other technologies
    might be producing electricity in the background (e.g., coal is not used in Calliope, but is in the background of
    Ecoinvent). Thus, they should not be accounted either. (delete links from shifted demand from Ecoinvent to Calliope)
    """
    # 1.1.1 Electricity
    unlink_electricity()
    # 1.1.2 Heat
    unlink_heat()
    # 1.1.3 CO2
    unlink_co2()
    # 1.1.4 Hydrogen
    unlink_hydrogen()
    # 1.1.5 Waste
    # In cutoff it comes without any environmental burdens, so there is no need to apply any unlinks
    # 1.1.6 Biomass
    unlink_biomass()
    # 1.1.7 Methane
    unlink_methane()
    # 1.1.8 Methanol
    unlink_methanol()
    # 1.1.9 Kerosene
    unlink_kerosene()
    # 1.1.10 Diesel
    unlink_diesel()



# 1.2 substitute background activities
# 1.2.1 make European freight trains 100% electric
train_update()

# TODO:
#  1. cement: assume CCS
#  2. Biomass:
#  3. steel and iron: look for inventories
#  4. plastics:
#  5. ammonia and urea
#  5bis. methanol
#  6. transport
#  7. electricity
#  8. heat


# 2. set the foreground
# 2.1 update inventories
# TODO: maybe database arrangement changes to have only a single database
update_methanol_facility()
chp_waste_update(db_waste_name='apos391', db_original_name='cutoff391',
                 locations=['CH'])
biofuel_to_methanol_update(db_methanol_name='premise_base')
trucks_and_bus_update(db_truck_name='premise_base')
passenger_car_and_scooter_update(db_passenger_name='premise_base')
gas_to_liquid_update(db_cobalt_name='cutoff391', db_gas_to_liquid_name='premise_base')
biofuel_to_methane_infrastructure(db_syn_gas_name='cutoff391')
hp_update(db_hp_name='cutoff391')
hydro_run_of_river_update(db_hydro_name='cutoff391')
for location in ['FR', 'DE']:
    hydro_reservoir_update(location=location, db_hydro_name='cutoff391')
airborne_wind_lci(bd_airborne_name='cutoff391')

# 2.2 create fleets
solar_pv_fleet(db_solar_name='premise_base')
hydrogen_from_electrolysis_market(db_hydrogen_name='premise_base',
                                  soec_share=0.5, aec_share=0.3, pem_share=0.2)  # TODO: propose relevant fleets
batteries_fleet(db_batteries_name='premise_base', scenario='tc', technology_share=None)
# wind fleets created in Germany
wind_onshore_fleet(db_wind_name='cutoff391', location='DE', fleet_turbines_definition={'turbine_1': [
    {
        'power': 4.0, 'manufacturer': "Vestas", 'rotor_diameter': 125, 'hub_height': 100,
        'commissioning_year': 2030,
        'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
    }, 0.333],
    'turbine_2': [
        {
            'power': 6.0, 'manufacturer': 'Vestas', 'rotor_diameter': 145, 'hub_height': 120,
            'commissioning_year': 2030,
            'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
        },
        0.333],
    'turbine_3': [
        {
            'power': 8.0, 'manufacturer': 'Vestas', 'rotor_diameter': 160, 'hub_height': 145,
            'commissioning_year': 2030,
            'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
        },
        0.333]}
                   )
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

# 2.3 delete infrastructure and leave all activities ready in 'additional_acts'
delete_infrastructure_main(
    file_path=r'C:\Users\mique\OneDrive - UAB\PhD_ICTA_Miquel\research stay Delft\technology_mapping_clean.xlsx')
