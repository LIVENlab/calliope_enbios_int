import bw2io as bi
from premise import *
from tasks import *
import bw2data as bd
from consts import *


# TODO:
#  2. Plot background changes with current electricity mix and with only wind
#  3. Put database flow at test in a new project
#  4. Do infrastructure analysis at all steps
#  5. Carefully document everything
#  6. Think about implementation of material indicators (contrast NEW TOOL with my own functions).

def install_and_update_databases():
    """
    Databases:
    1. 'original_cutoff391'
    2. 'apos391'
    3. 'premise_original'
    4. 'premise_base': will contain the updated background and foreground. Infrastructure deletion,
    and fuel input deletion (double accounting avoided).
    5. 'additional_acts':
    6. Auxiliary databases:
        - 'premise_auxiliary_for_infrastructure' will be the base for 'infrastructure (with European steel and concrete)'
    7. Final databases (all of them come WITH background changes):
        - 'infrastructure (with European steel and concrete)': infrastructure activities WITH European markets for
          steel and concrete.
    # TODO: maybe after all the treatments, we can create a database 'O&M biosphere' with all the corresponding activities. Same for 'O&M technosphere'. In this way, we would have all needed activities well classified in three separated databases.
    """
    # setup_databases
    bd.projects.set_current(PROJECT_NAME)
    bi.bw2setup()

    # Ecoinvent v3.9.1 cutoff and apos
    if 'original_cutoff391' not in bd.databases:
        ei = bi.SingleOutputEcospold2Importer(SPOLDS_CUTOFF, "original_cutoff391", use_mp=False)
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
    ndb.write_db_to_brightway(name='premise_original')

    # premise with cement update
    ndb = NewDatabase(
        scenarios=[
            {"model": "image", "pathway": "SSP2-RCP19", "year": 2020},
        ],
        source_db="cutoff391",
        source_version="3.9.1",
        key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo='
    )
    ndb.update('cement')
    ndb.write_db_to_brightway(name='premise_cement')

    # create premise_original copy named 'premise_base)
    bd.Database('premise_original').copy(name="premise_base")

    # background changes
    update_background()

    # create a copy for each of the databases that we will have in the project.
    premise_base_auxiliary()

    # foreground changes
    update_foreground()

    # 'infrastructure (with European steel and concrete)' operating.
    update_cement_iron_foreground()

    # O&M activities in premise_base and additional_acts do not have infrastructure inputs after running this function.
    # Moreover, now we have activities (with ', biosphere' and ', technosphere' at the end of the name indicated in the
    # mapping file) in additional_acts.
    delete_infrastructure_main(
        file_path=r'C:\Users\mique\OneDrive - UAB\PhD_ICTA_Miquel\research stay Delft\technology_mapping_clean.xlsx')

    # avoid double accounting
    # TODO: applied ONLY to those databases used in ENBIOS!
    avoid_double_accounting()


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


def update_background():
    # 1.2 substitute background activities
    # 1.2.1. cement
    cement_update()
    # 1.2.2 make European freight trains 100% electric
    train_update()
    # 1.2.3. biomass
    # TODO: waiting for Calliope assumptions
    # 1.2.4. steel
    steel_update()
    # 1.2.5. plastics
    # NOTE: aromatics follow today's synthetic route due to lack of data. Olefins produced from methanol, produced
    # from hydrogen and CO2 (DAC). Assumptions on recycling and improved circular economies could not be matched.
    plastics_update()
    # 1.2.6. methanol
    methanol_update()
    # 1.2.7. ammonia
    ammonia_update()
    # 1.2.8 transport (NOTE: it takes 15-20 min)
    transport_update()


def update_foreground():
    # 2. set the foreground
    # 2.1 update inventories
    update_methanol_facility()
    chp_waste_update(db_waste_name='apos391', db_original_name='premise_base',
                     locations=['CH'])
    biofuel_to_methanol_update(db_methanol_name='premise_base')
    trucks_and_bus_update(db_truck_name='premise_base')
    passenger_car_and_scooter_update(db_passenger_name='premise_base')
    gas_to_liquid_update(db_cobalt_name='premise_base', db_gas_to_liquid_name='premise_base')
    biofuel_to_methane_infrastructure(db_syn_gas_name='premise_base')
    hp_update(db_hp_name='premise_base')
    hydro_run_of_river_update(db_hydro_name='premise_base')
    hydro_reservoir_update(location='ES', db_hydro_name='premise_base')
    airborne_wind_lci(bd_airborne_name='premise_base')
    # 2.2 create fleets
    create_fleets()
    # TODO: think if I should aggregate infrastructure in the different tiers in one single activity for
    #  kerosene, diesel, and methanol.


def create_fleets():
    # 2.2 create fleets
    solar_pv_fleet(db_solar_name='premise_base')
    hydrogen_from_electrolysis_market(db_hydrogen_name='premise_base',
                                      soec_share=0.5, aec_share=0.3, pem_share=0.2)  # TODO: propose relevant fleets
    batteries_fleet(db_batteries_name='premise_base', scenario='tc', technology_share=None)
    # wind fleets created in Germany
    wind_onshore_fleet(db_wind_name='original_cutoff391', location='DE', fleet_turbines_definition={'turbine_1': [
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
    wind_offshore_fleet(db_wind_name='original_cutoff391', location='DE', fleet_turbines_definition={'turbine_1': [
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
