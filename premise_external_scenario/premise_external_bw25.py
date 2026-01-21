from premise import NewDatabase
from datapackage import Package
import bw2data as bd
import bw2calc as bc
import pickle
import wurst.searching as ws
import bw2io as bi
from WindTrace import WindTrace_onshore, WindTrace_offshore
from functions import create_additional_acts_db
import sys
from typing import Dict, List, Union, Any
import config_parameters
import pandas as pd
import matplotlib.pyplot as plt
import math
import copy
import matplotlib.colors as mcolors
import re

def import_ei_12():
    bi.import_ecoinvent_release(
        version="3.12",
        system_model="cutoff",
        username="USERNAME",
        password="PASS",
        lci=True,
        lcia=True,
        # optional:
        biosphere_name='ecoinvent-3.12-biosphere',         # Default name like "ecoinvent-3.10-biosphere"
    )
def create_custom_database(output_database_name: str,
                           year: int,
                           external_scenario_json_path = r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\datapackage.json",
                           bw25_project_name: str = 'bw25_matrix'):
    bd.projects.set_current(bw25_project_name)
    pkg = Package(external_scenario_json_path)

    external_scenario = [
        {"scenario": "Business As Usual", "data": pkg},
    ]
    ndb = NewDatabase(
        scenarios=[
            {
                "model": "image",
                "pathway": "SSP2-L",
                "year": year,
                "external scenarios": external_scenario,
            }
        ],
        source_db="cutoff391",   # change to what you actually use
        source_version="3.9.1",
        key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
    )
    ndb.update("external")

    pickle_path = ndb.scenarios[0]['database filepath']
    try:
        with open(pickle_path, "rb") as f:
            data = pickle.load(f)
    except:
        print("There is no pickled database")

    # delete "World" markets
    data = [a for a in data if a['location'] != 'World']

    # european production routes for steel
    print('Creating European production routes for steel')
    regionalise_steel_production(data=data)

    # electricity mixes substitutions
    print('Starting electricity mix substitutions')

    voltages = ["high", "medium", "low"]

    all_voltages = [
        a for a in data
        if any(
            a["name"] == f"market for electricity, {v} voltage (new)"
            and a["reference product"] == f"electricity, {v} voltage (new)"
            for v in voltages
        )
    ]

    acts_to_be_replaced = []
    for act in all_voltages:
        act_to_be_replaced = [a for a in data if a['name'] == act['name'][:-6] and a['reference product'] == act['reference product'][:-6] and a['location'] == act['location']]
        acts_to_be_replaced.append(act_to_be_replaced[0])

    # TODO: create a function
    counter = 0
    for replaceable_act in acts_to_be_replaced:
        replacing_act = all_voltages[counter]
        for act in data:
            for exchange in ws.technosphere(act):
                if exchange['product'] == replaceable_act['reference product'] and exchange['name'] == replaceable_act['name'] and exchange['unit'] == replaceable_act['unit'] and exchange['location'] == replaceable_act['location']:
                    if not exchange['name'] == act['name']:
                        exchange['product'] = replacing_act['reference product']
                        exchange['name'] = replacing_act['name']
        counter += 1

    # natural gas substitutions
    print('Starting natural gas substitutions')
    locations = [
        "AT", "BE", "CH", "CZ", "DE", "DK", "ES", "FI", "FR", "GB", "GR", "HU", "IE", "IT",
        "NL", "NO", "PL", "RO", "SE", "SK", "RoE"
    ]
    acts_to_be_replaced = [a for a in data if a['name'] == 'market for natural gas, high pressure' and a['location'] in locations]
    for replaceable_act in acts_to_be_replaced:
        for act in data:
            for exchange in ws.technosphere(act):
                if (exchange['product'] == replaceable_act['reference product'] and
                        exchange['name'] == replaceable_act['name'] and
                        exchange['unit'] == replaceable_act['unit'] and
                        exchange['location'] == replaceable_act['location']):
                    if not exchange['name'] == act['name']:
                        exchange['product'] = 'methane, high pressure (new)'
                        exchange['name'] = 'market for methane, high pressure (new)'
    locations = [
        "DE", "GB", "NL", "NO", "RO"
    ]
    acts_to_be_replaced = [a for a in data if a['name'] == 'petroleum and gas production, offshore' and a['reference product'] == 'natural gas, high pressure' and a['location'] in locations]
    for replaceable_act in acts_to_be_replaced:
        for act in data:
            for exchange in ws.technosphere(act):
                if (exchange['product'] == replaceable_act['reference product'] and
                        exchange['name'] == replaceable_act['name'] and
                        exchange['unit'] == replaceable_act['unit'] and
                        exchange['location'] == replaceable_act['location']):
                    if not exchange['name'] == act['name']:
                        exchange['product'] = 'methane, high pressure (new)'
                        exchange['name'] = 'market for methane, high pressure (new)'

    locations = [
        "NL", "RO", "GB", "DE"
    ]
    acts_to_be_replaced = [a for a in data if a['name'] == 'petroleum and gas production, onshore' and a['reference product'] == 'natural gas, high pressure' and a['location'] in locations]

    for replaceable_act in acts_to_be_replaced:
        for act in data:
            for exchange in ws.technosphere(act):
                if (exchange['product'] == replaceable_act['reference product'] and
                        exchange['name'] == replaceable_act['name'] and
                        exchange['unit'] == replaceable_act['unit'] and
                        exchange['location'] == replaceable_act['location']):
                    if not exchange['name'] == act['name']:
                        exchange['product'] = 'methane, high pressure (new)'
                        exchange['name'] = 'market for methane, high pressure (new)'


    # biomass substitution
    print('Starting biomass substitutions')
    # 1. Caluclate LHV of new market:
    new_biomass_act = [a for a in data if a['name'] == 'market for biomass, used as fuel (new)'][0]
    forest_act = False
    chips_act = False
    for ex in ws.technosphere(new_biomass_act):
        if ex['name'] == 'supply of forest residue':
            forest_heat = ex['amount'] * 19
            forest_act = True
        elif ex['name'] == 'market for wood chips, wet, measured as dry mass':
            chips_heat = ex['amount'] * 8.7
            chips_act = True
    if not forest_act:
        forest_heat = 0
    if not chips_act:
        chips_heat = 0
    new_biomass_act_lhv = forest_heat + chips_heat
    # 2. replace acts
    locations = ['CH', 'Europe without Switzerland']
    acts_to_be_replaced_wet = [a for a in data if a['name'] == 'market for wood chips, wet, measured as dry mass' and a['location'] in locations]
    acts_to_be_replaced_dry = [a for a in data if a['name'] == 'market for wood chips, dry, measured as dry mass' and a['location'] == 'RER']
    acts_to_be_replaced_pellet = [a for a in data if a['name'] == 'market for wood pellet, measured as dry mass' and a['location'] == 'RER']
    acts_to_be_replaced = acts_to_be_replaced_wet + acts_to_be_replaced_dry + acts_to_be_replaced_pellet

    indirect_energy_acts = ['ethanol production', 'synthetic natural gas', 'biomethane production', 'syngas', 'hydrogen production']
    energy_data = [
        a for a in data
        if (
            ('heat' in a['reference product'] and 'megajoule' in a['unit'])
            or ('electricity, high voltage' in a['reference product'] and 'kilowatt hour' in a['unit'])
            or any(name in a['name'] for name in indirect_energy_acts)
        )
    ]
    for replaceable_act in acts_to_be_replaced:
        for act in energy_data:  # only substitute heat uses of the new biomass market
            for exchange in ws.technosphere(act):
                if (exchange['product'] == replaceable_act['reference product'] and
                        exchange['name'] == replaceable_act['name'] and
                        exchange['unit'] == replaceable_act['unit']):
                    if replaceable_act['name'] == 'market for wood chips, wet, measured as dry mass':
                        exchange['product'] = 'biomass, used as fuel (new)'
                        exchange['name'] = 'market for biomass, used as fuel (new)'
                        exchange['location'] = 'Europe without Switzerland'
                        exchange['amount'] = exchange['amount'] * 8.7 / new_biomass_act_lhv
                    elif replaceable_act['name'] == 'market for wood chips, dry, measured as dry mass':
                        exchange['product'] = 'biomass, used as fuel (new)'
                        exchange['name'] = 'market for biomass, used as fuel (new)'
                        exchange['location'] = 'Europe without Switzerland'
                        exchange['amount'] = exchange['amount'] * 19 / new_biomass_act_lhv
                    elif replaceable_act['name'] == 'market for wood pellet, measured as dry mass':
                        exchange['product'] = 'biomass, used as fuel (new)'
                        exchange['name'] = 'market for biomass, used as fuel (new)'
                        exchange['location'] = 'Europe without Switzerland'
                        exchange['amount'] = exchange['amount'] * 17 / new_biomass_act_lhv


    # coal and lignite substitutions
    print('Starting coal and lignite substitutions')
    # 1. Caluclate LHV of new lignite market:
    new_lignite_act = [a for a in data if a['name'] == 'market for lignite, for energy uses (new)'][0]
    lignite_is_present = False
    charcoal_is_present = False
    for ex in ws.technosphere(new_lignite_act):
        if ex['name'] == 'market for lignite':
            lignite_heat = ex['amount'] * 11
            lignite_is_present = True
        if ex['product'] == 'charcoal':
            charcoal_heat = ex['amount'] * 30
            charcoal_is_present = True
    if not lignite_is_present:
        lignite_heat = 0
    if not charcoal_is_present:
        charcoal_heat = 0
    new_lignite_act_lhv = lignite_heat + charcoal_heat
    # 2. replace acts
    coal_act = [a for a in data if a['name'] == 'market for hard coal' and a['reference product'] == 'hard coal'
                           and a['location'] == 'Europe, without Russia and Turkey'][0]
    lignite_act = [a for a in data if a['name'] == 'market for lignite' and a['reference product'] == 'lignite'
                           and a['location'] == 'RER'][0]
    acts_to_be_replaced = [coal_act, lignite_act]
    energy_data = [
        a for a in data
        if (
            ('heat' in a['reference product'] and ('heat' in a['name'] or 'electricity' in a['name']))
            or ('electricity' in a['reference product'] and ('heat' in a['name'] or 'electricity' in a['name']))
            )

    ]
    for replaceable_act in acts_to_be_replaced:
        for act in energy_data:
            for exchange in ws.technosphere(act):
                if (exchange['product'] == replaceable_act['reference product'] and
                            exchange['name'] == replaceable_act['name'] and
                            exchange['unit'] == replaceable_act['unit'] and
                            exchange['location'] == replaceable_act['location']):
                    if replaceable_act['name'] == 'market for hard coal':
                        exchange['product'] = 'coal (new)'
                        exchange['name'] = 'market for coal, for energy uses (new)'
                        exchange['location'] = 'Europe, without Russia and Turkey'
                    elif replaceable_act['name'] == 'market for lignite':
                        exchange['product'] = 'lignite (new)'
                        exchange['name'] = 'market for lignite, for energy uses (new)'
                        exchange['location'] = 'RER'
                        exchange['amount'] = exchange['amount'] * 11 / new_lignite_act_lhv

    # Fix syngas (only European inputs)
    print('Starting syngas substitutions')
    syngas_act = [a for a in data if a['name'] == 'syngas production, from natural gas' and a['location'] == 'RER'
                  and a['reference product'] == 'syngas, from natural gas'][0]
    input_amounts = []
    for ex in ws.technosphere(syngas_act):
        input_amounts.append(ex['amount'])
    inputs_sum = sum(input_amounts)
    new_exchanges = [ex for ex in syngas_act['exchanges'] if ex['type'] != 'technosphere']
    syngas_act['exchanges'] = new_exchanges
    syngas_act['exchanges'].append({'amount': inputs_sum,
                               'comment': 'It should be low pressure, but we prefer location representativeness',
                               'location': 'Europe without Switzerland',
                               'name': 'market group for natural gas, high pressure',
                               'product': 'natural gas, high pressure',
                               'reference product': 'natural gas, high pressure',
                               'type': 'technosphere',
                               'uncertainty type': 0,
                               'unit': 'cubic meter'})


    with open(pickle_path, "wb") as f:
        pickle.dump(data, f)

    ndb.write_db_to_brightway(output_database_name)


def regionalise_steel_production(data):
    act_names_to_be_replaced = [
    "iron pellet production",
    "pig iron production, blast furnace, with carbon capture and storage",
    "steel production, blast furnace-basic oxygen furnace, with carbon capture and storage, low-alloyed",
    "pig iron production, top gas recycling-blast furnace",
    "steel production, blast furnace-basic oxygen furnace, with top gas recycling, low-alloyed",
    "pig iron production, blast furnace, with top gas recycling, with carbon capture and storage",
    "steel production, blast furnace-basic oxygen furnace, with top gas recycling, with carbon capture and storage, low-alloyed",
    "pig iron production, by electrowinning",
    "steel production, electrowinning-electric arc furnace, low-alloyed",
    "pig iron production, hydrogen-based direct reduction iron",
    "preheating of iron ore pellets",
    "preheating of hydrogen",
    "steel production, hydrogen-based direct reduction iron-electric arc furnace, low-alloyed",
    "pig iron production, with natural gas-based direct reduction",
    "steel production, natural gas-based direct reduction iron-electric arc furnace, low-alloyed",
    "steel production, natural gas-based direct reduction iron-electric arc furnace, with carbon capture and storage, low-alloyed",
    "pig iron production, with natural gas-based direct reduction, with carbon capture and storage",
    "steel production, blast furnace-basic oxygen furnace, unalloyed",
    "steel production, blast furnace-basic oxygen furnace, with carbon capture and storage, unalloyed",
        "steel production, blast furnace-basic oxygen furnace, with top gas recycling, unalloyed",
        "steel production, blast furnace-basic oxygen furnace, with top gas recycling, with carbon capture and storage, unalloyed",
        "steel production, natural gas-based direct reduction iron-electric arc furnace, unalloyed",
        "steel production, natural gas-based direct reduction iron-electric arc furnace, with carbon capture and storage, unalloyed",
        "steel production, hydrogen-based direct reduction iron-electric arc furnace, unalloyed",
        "steel production, electrowinning-electric arc furnace, unalloyed",
        "carbon dioxide, captured at steel production plant using direct reduction iron, using vacuum pressure swing adsorption"  # NOTE: we assume the CO2 used is the one captured at the steel production plant
]
    acts_to_be_replaced = [a for a in data if a['name'] in act_names_to_be_replaced][2:]
    for act in acts_to_be_replaced:
        new_act = copy.deepcopy(act)
        new_act['location'] = 'RER'
        for prod_ex in ws.production(new_act):
            prod_ex['location'] = 'RER'
        data.append(new_act)

        electricity_map = {
            "high": {
                "names": ["market for electricity, high voltage", "market group for electricity, high voltage"],
                "product": "electricity, high voltage",
            },
            "medium": {
                "names": ["market for electricity, medium voltage", "market group for electricity, medium voltage"],
                "product": "electricity, medium voltage",
            },
            "low": {
                "names": ["market for electricity, low voltage", "market group for electricity, low voltage"],
                "product": "electricity, low voltage",
            },
        }
        for ex in ws.technosphere(new_act):
            ### SUBSTITUTE INPUTS ###
            # electricity
            for level, cfg in electricity_map.items():
                if ex["name"] in cfg["names"]:
                    ex["name"] = f"market group for electricity, {level} voltage"
                    ex["product"] = cfg["product"]
                    ex["location"] = "RER"
                    break

            # natural gas
            if ex["name"] in ['market group for natural gas, high pressure', 'market for natural gas, high pressure']:
                ex['name'] = "market group for natural gas, high pressure"
                ex['product'] = "natural gas, high pressure"
                ex['location'] = "Europe without Switzerland"

            # heat
            elif (
                    any(s in ex["name"] for s in [
                        "heat production,",
                        "market for heat, district or industrial",
                        "market group for heat, district or industrial",
                    ])
                    and "heat, district or industrial" in ex["product"]
            ):
                ex['name'] = "market group for heat, district or industrial, natural gas"
                ex['product'] = "heat, district or industrial, natural gas"
                ex['location'] = "RER"

            elif ( any(s in ex["name"] for s in [
                        'heat production,', 'market heat, central or small-scale',
                        'market group for heat, central or small-scale'
                    ])
                 and "heat, central or small-scale" in ex["product"]):
                ex['name'] = "market group for heat, central or small-scale, natural gas"
                ex['product'] = "heat, central or small-scale, natural gas"
                ex['location'] = "RER"


            ### RELINK ###
            if ex['name'] in act_names_to_be_replaced:
                ex['location'] = 'RER'
            elif ex['name'] == 'market for iron pellet':
                ex['name'] = "iron pellet production"
                ex['location'] = 'RER'


###################
# APPLY WINDTRACE #
###################
def _test_onshore_wind_turbine_existance(fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                                         location: str, new_db_name: str):
    expected_keys = {'power', 'manufacturer', 'rotor_diameter', 'hub_height', 'commissioning_year',
                     'generator_type', 'recycled_share_steel', 'lifetime', 'eol_scenario'}
    park_names = []
    for turbine, info in fleet_turbines_definition.items():
        turbine_parameters = info[0]
        park_name = f'{turbine}_{turbine_parameters["power"]}_{location}'
        park_names.append(park_name)
        if turbine_parameters.keys() != expected_keys:
            raise ValueError(f'The keys introduced {turbine_parameters.keys()} do not match '
                             f'the expected keys {expected_keys}')
    try:
        # Check if lengths match, meaning no duplicates
        if len(park_names) == len(list(set(park_names))):
            print("No duplicates found in park names")
        else:
            print("Park name duplicates found. Change the park names you introduce")
        for act_name in park_names:
            act = [a for a in bd.Database(new_db_name) if a["name"] == act_name]
            if not act:
                print('Your onshore park turbines have not been created before')
                return False
        print('Your onshore park turbines are already present in the database. They will be used by default')
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit()

def _test_offshore_wind_turbine_existence(fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                                          location: str, new_db_name: str):
    expected_keys = {'power', 'manufacturer', 'rotor_diameter', 'hub_height', 'commissioning_year',
                     'generator_type', 'recycled_share_steel', 'lifetime', 'eol_scenario', 'offshore_type',
                     'floating_platform', 'sea_depth', 'distance_to_shore'}
    park_names = []
    for turbine, info in fleet_turbines_definition.items():
        turbine_parameters = info[0]
        if turbine_parameters['offshore_type'] == 'floating':
            park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["floating_platform"]}_{location}'
        else:
            park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["offshore_type"]}_{location}'
        park_names.append(park_name)
        if turbine_parameters.keys() != expected_keys:
            raise ValueError(f'The keys introduced {turbine_parameters.keys()} do not match '
                             f'the expected keys {expected_keys}')
    try:
        # Check if lengths match, meaning no duplicates
        if len(park_names) == len(list(set(park_names))):
            print("No duplicates found in park names")
        else:
            print("Park name duplicates found. Change the park names you introduce")
        for act_name in park_names:
            act = [a for a in bd.Database(new_db_name) if a["name"] == f'{act_name}_offshore_turbine']
            if not act:
                print('Your offshore park turbines have not been created before')
                return False
        print('Your offshore park turbines are already present in the database. They will be used by default')
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit()


def _onshore_turbine_fleet_exists(location_new_wind_act: str):
    try:
        bd.Database('additional_acts').get(f'electricity production, onshore wind fleet, {location_new_wind_act}')
        return True
    except:
        return False


def _offshore_turbine_fleet_exists(location_new_wind_act: str):
    try:
        bd.Database('additional_acts').get(f'electricity production, offshore wind fleet, {location_new_wind_act}')
        return True
    except:
        return False


def apply_windtrace_onshore(db_wind_name: str, location: str,
                       fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                       biosphere3: bd.Database = bd.Database('biosphere3')):
    print('Creating onshore wind fleet')
    create_additional_acts_db()

    turbine_fleet_exists = _onshore_turbine_fleet_exists(location)
    if turbine_fleet_exists:
        act = bd.Database('additional_acts').get(f'electricity production, onshore wind fleet, {location}')
        act.delete()

    # create individual turbines
    for turbine, info in fleet_turbines_definition.items():
        turbine_parameters = info[0]
        park_name = f'{turbine}_{turbine_parameters["power"]}_{location}'
        WindTrace_onshore.lci_wind_turbine(
            new_db=bd.Database('additional_acts'), cutoff391=bd.Database(db_wind_name),
            park_name=park_name, park_power=turbine_parameters['power'], number_of_turbines=1,
            park_location=location, park_coordinates=(51.181, 13.655),
            manufacturer=turbine_parameters['manufacturer'], rotor_diameter=turbine_parameters['rotor_diameter'],
            turbine_power=turbine_parameters['power'], hub_height=turbine_parameters['hub_height'],
            commissioning_year=turbine_parameters['commissioning_year'],
            generator_type=turbine_parameters['generator_type'],
            recycled_share_steel=turbine_parameters['recycled_share_steel'],
            lifetime=turbine_parameters['lifetime'], eol_scenario=turbine_parameters['eol_scenario'],
            biosphere3=biosphere3
        )

    # create wind fleet activity (per 1 kWh)
    fleet_activity = bd.Database('additional_acts').new_activity(
        name=f'electricity production, onshore wind fleet, {location}',
        code=f'electricity production, onshore wind fleet, {location}',
        unit='kilowatt hour',
        location=location
    )
    fleet_activity['reference product'] = 'electricity, high voltage'
    fleet_activity.save()
    new_ex = fleet_activity.new_exchange(input=fleet_activity.key, type='production', amount=1)
    new_ex.save()
    # add inputs
    for turbine, info in fleet_turbines_definition.items():
        share = info[1]
        turbine_parameters = info[0]
        park_name = f'{turbine}_{turbine_parameters["power"]}_{location}'
        turbine_activity = bd.Database('additional_acts').get(park_name + '_turbine_kwh')
        # to fleet activity (infrastructure)
        new_ex = fleet_activity.new_exchange(input=turbine_activity, type='technosphere',
                                             amount=share)
        new_ex.save()

    return fleet_activity

def substitute_windtrace_onshore(ecoinvent_database_name: str,
                                 database_windtrace_should_substitute: str,
                                 location_new_wind_act: str,
                                 fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                                 biosphere3: bd.Database = bd.Database('biosphere3'),
                                 european_locations_only: bool = True,
                                 ):
    bd.projects.set_current(config_parameters.PROJECT_NAME)
    create_additional_acts_db()
    turbines_exist = _test_onshore_wind_turbine_existance(fleet_turbines_definition=fleet_turbines_definition,
                                                          location=location_new_wind_act, new_db_name='additional_acts')

    if not turbines_exist:
        new_onshore_act = apply_windtrace_onshore(db_wind_name=ecoinvent_database_name, location=location_new_wind_act,
                                                  fleet_turbines_definition=fleet_turbines_definition, biosphere3=biosphere3
                                                  )
    else:
        new_onshore_act = bd.Database('additional_acts').get(f'electricity production, onshore wind fleet, {location_new_wind_act}')

    european_locations = ['ES', 'BG', 'SE', 'AT', 'MK', 'MD', 'HR', 'XK', 'LU', 'GR', 'IS', 'BA', 'EE', 'SK',
                          'ME', 'LT', 'SI', 'IE', 'BE', 'RS', 'RO', 'NL', 'UA', 'PL', 'FR', 'GB', 'NO', 'CZ',
                          'MT', 'DK', 'IT', 'LV', 'DE', 'PT', 'FI', 'BY', 'GI', 'AL', 'HU', 'CH']
    onshore_wind_names = ['electricity production, wind, 1-3MW turbine, onshore',
                          'electricity production, wind, <1MW turbine, onshore',
                          'electricity production, wind, >3MW turbine, onshore']
    if european_locations_only:
        wind_acts_to_substitute = [a for a in bd.Database(database_windtrace_should_substitute) if a['name'] in onshore_wind_names
                               and a['location'] in european_locations]
    else:
        wind_acts_to_substitute = [a for a in bd.Database(database_windtrace_should_substitute) if a['name'] in onshore_wind_names]

    for act in wind_acts_to_substitute:
        for ex in act.consumers():
            ex.input = new_onshore_act
            ex.save()


def apply_windtrace_offshore(db_wind_name: str, location: str,
                             fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]]
                            ):
    print('Creating offshore wind fleet')
    create_additional_acts_db()

    turbine_fleet_exists = _offshore_turbine_fleet_exists(location)
    if turbine_fleet_exists:
        act = bd.Database('additional_acts').get(f'electricity production, offshore wind fleet, {location}')
        act.delete()

    # create individual turbines
    for turbine, info in fleet_turbines_definition.items():
        turbine_parameters = info[0]
        if turbine_parameters['offshore_type'] == 'floating':
            park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["floating_platform"]}_{location}'
        else:
            park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["offshore_type"]}_{location}'
        WindTrace_offshore.lci_offshore_turbine(
            new_db=bd.Database('additional_acts'), cutoff391=bd.Database(db_wind_name),
            biosphere3=bd.Database('biosphere3'),
            park_name=park_name, park_power=turbine_parameters['power'], number_of_turbines=1,
            park_location=location, park_coordinates=(51.181, 13.655),
            manufacturer=turbine_parameters['manufacturer'], rotor_diameter=turbine_parameters['rotor_diameter'],
            turbine_power=turbine_parameters['power'], hub_height=turbine_parameters['hub_height'],
            commissioning_year=turbine_parameters['commissioning_year'],
            generator_type=turbine_parameters['generator_type'],
            recycled_share_steel=turbine_parameters['recycled_share_steel'],
            lifetime=turbine_parameters['lifetime'], scenario=turbine_parameters['eol_scenario'],
            sea_depth=turbine_parameters['sea_depth'], distance_to_shore=turbine_parameters['distance_to_shore'],
            offshore_type=turbine_parameters['offshore_type'],
            floating_platform=turbine_parameters['floating_platform']
        )

    # create wind fleet activity (per 1 kWh)
    fleet_activity = bd.Database('additional_acts').new_activity(
            name=f'electricity production, offshore wind fleet, {location}',
            code=f'electricity production, offshore wind fleet, {location}',
            unit='kilowatt hour',
            location=location
        )
    fleet_activity['reference product'] = 'electricity, high voltage'
    fleet_activity.save()
    new_ex = fleet_activity.new_exchange(input=fleet_activity.key, type='production', amount=1)
    new_ex.save()
    # add inputs
    for turbine, info in fleet_turbines_definition.items():
        share = info[1]
        turbine_parameters = info[0]
        if turbine_parameters['offshore_type'] == 'floating':
            park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["floating_platform"]}_{location}'
        else:
            park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["offshore_type"]}_{location}'
        turbine_activity = bd.Database('additional_acts').get(f'{park_name}_offshore_turbine_kwh')
        # to fleet activity (infrastructure)
        new_ex = fleet_activity.new_exchange(input=turbine_activity, type='technosphere',
                                                 amount=share)
        new_ex.save()

    return fleet_activity


def substitute_windtrace_offshore(ecoinvent_database_name: str,
                                 database_windtrace_should_substitute: str,
                                 location_new_wind_act: str,
                                 fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                                 european_locations_only: bool = True):
    bd.projects.set_current(config_parameters.PROJECT_NAME)
    create_additional_acts_db()
    turbines_exist = _test_offshore_wind_turbine_existence(fleet_turbines_definition=fleet_turbines_definition,
                                                          location=location_new_wind_act, new_db_name='additional_acts')
    if not turbines_exist:
        new_offshore_act = apply_windtrace_offshore(db_wind_name=ecoinvent_database_name, location=location_new_wind_act,
                                              fleet_turbines_definition=fleet_turbines_definition,
                                              )
    else:
        new_offshore_act = bd.Database('additional_acts').get(
            f'electricity production, offshore wind fleet, {location_new_wind_act}')
    european_locations = ['ES', 'BG', 'SE', 'AT', 'MK', 'MD', 'HR', 'XK', 'LU', 'GR', 'IS', 'BA', 'EE', 'SK',
                          'ME', 'LT', 'SI', 'IE', 'BE', 'RS', 'RO', 'NL', 'UA', 'PL', 'FR', 'GB', 'NO', 'CZ',
                          'MT', 'DK', 'IT', 'LV', 'DE', 'PT', 'FI', 'BY', 'GI', 'AL', 'HU', 'CH']
    if european_locations_only:
        wind_acts_to_substitute = [a for a in bd.Database(database_windtrace_should_substitute) if
                                   a['name'] == 'electricity production, wind, 1-3MW turbine, offshore'
                                   and a['location'] in european_locations]
    else:
        wind_acts_to_substitute = [a for a in bd.Database(database_windtrace_should_substitute) if
                                   a['name'] == 'electricity production, wind, 1-3MW turbine, offshore']

    for act in wind_acts_to_substitute:
        for ex in act.consumers():
            ex.input = new_offshore_act
            ex.save()

    # TODO: VERY IMPORTANT REFLECTION! inputs for steel are from cutoff391, not custom_XXXX. This means the steel used to produce the wind turbine
    #  is made of the steel from cutoff391. This is actually a good idea! Should I consider NOT substituting exchanges that are used to create infrastructure???

##########################
# CREATE SCENARIO VALUES #
##########################

def demand_array(new_db_name: str, analysis_act, amount: float,
                 lcia_method: tuple = ('EF v3.1', 'climate change', 'global warming potential (GWP100)')):
    new_acts = [a for a in bd.Database(new_db_name) if '(new)' in a['name']]
    new_acts_set = set(new_acts)
    my_functional_unit, data_objs, _ = bd.prepare_lca_inputs(
        {analysis_act: amount}, method=lcia_method
    )
    lca_obj = bc.LCA(demand=my_functional_unit, data_objs=data_objs)
    lca_obj.lci()
    supply_array = lca_obj.supply_array
    products = {'Biomass (MJ)': 0.0,
                'Hydrogen (kg)': 0.0,
                'Methanol (kg)': 0.0,
                'Kerosene (kg)': 0.0,
                'Diesel (kg)': 0.0,
                'Liquefied petroleum gas (kg)': 0.0,
                'Methane (m3)': 0.0,
                'Carbon dioxide (kg)': 0.0,
                'Lubricating oil (kg)': 0.0,
                'Electricity, high voltage (kWh)': 0.0,
                'Electricity, low voltage (kWh)': 0.0,
                'Electricity, medium voltage (kWh)': 0.0,
                'Heat, central (MJ)': 0.0,
                'Heat, district (MJ)': 0.0,
                'Coal (kg)': 0.0,
                'Lignite (kg)': 0.0}
    for product_id, row_index in lca_obj.dicts.product.items():
        product = bd.get_activity(product_id)
        amt = supply_array[row_index]

        if amt == 0:
            continue

        if product not in new_acts_set:
            continue

        name = product['name']

        if 'market for biomass' in name:
            products['Biomass (MJ)'] += amt

        elif 'market for hydrogen' in name:
            products['Hydrogen (kg)'] += amt

        elif 'market for methanol' in name:
            products['Methanol (kg)'] += amt

        elif 'market for kerosene' in name:
            products['Kerosene (kg)'] += amt

        elif 'market for diesel' in name:
            products['Diesel (kg)'] += amt

        elif 'market for liquefied petroleum gas' in name:
            products['Liquefied petroleum gas (kg)'] += amt

        elif 'market for methane' in name:
            products['Methane (m3)'] += amt

        elif 'market for carbon dioxide' in name:
            products['Carbon dioxide (kg)'] += amt

        elif 'market for lubricating oil' in name:
            products['Lubricating oil (kg)'] += amt

        elif 'market for electricity, high voltage' in name:
            products['Electricity, high voltage (kWh)'] += amt

        elif 'market for electricity, low voltage' in name:
            products['Electricity, low voltage (kWh)'] += amt

        elif 'market for electricity, medium voltage' in name:
            products['Electricity, medium voltage (kWh)'] += amt

        elif 'market for heat, central' in name:
            products['Heat, central (MJ)'] += amt

        elif 'market for heat, district' in name:
            products['Heat, district (MJ)'] += amt

        elif 'market for coal' in name:
            products['Coal (kg)'] += amt

        elif 'market for lignite' in name:
            products['Lignite (kg)'] += amt

    return products


def pes_demand(biosphere_db_name: str, analysis_act, amount: float,
                 lcia_method: tuple = ('EF v3.1', 'climate change', 'global warming potential (GWP100)')):
    coal = [a for a in bd.Database(biosphere_db_name) if 'Coal,' in a['name'] and a['categories'][0] == 'natural resource']
    uranium = [a for a in bd.Database(biosphere_db_name) if a['name'] == 'Uranium' and a['categories'][0] == 'natural resource']
    gas = [a for a in bd.Database(biosphere_db_name) if 'Gas,' in a['name'] and a['categories'][0] == 'natural resource']
    water = [a for a in bd.Database(biosphere_db_name) if 'Water,' in a['name'] and a['categories'][0] == 'natural resource']
    oil = [a for a in bd.Database(biosphere_db_name) if 'Oil, crude' in a['name'] and a['categories'][0] == 'natural resource']
    # Missing Waste TODO?
    biomass_energy = [a for a in bd.Database(biosphere_db_name) if 'Energy, gross' in a['name'] and a['categories'][0] == 'natural resource']
    land = [a for a in bd.Database(biosphere_db_name) if 'Transformation, from' in a['name'] and a['categories'][0] == 'natural resource']

    coal_set = set(coal)
    uranium_set = set(uranium)
    gas_set = set(gas)
    water_set = set(water)
    oil_set = set(oil)
    land_set = set(land)
    biomass_energy_set = set(biomass_energy)

    my_functional_unit, data_objs, _ = bd.prepare_lca_inputs(
        {analysis_act: amount}, method=lcia_method
    )
    lca_obj = bc.LCA(demand=my_functional_unit, data_objs=data_objs)
    lca_obj.lci()
    inventory_array = lca_obj.inventory.sum(axis=1)
    raw_materials = {
        "Coal (kg)": 0.0,
        "Uranium (kg)": 0.0,
        "Gas (m3)": 0.0,
        "Water (m3)": 0.0,
        "Oil (kg)": 0.0,
        "Land (m2)": 0.0,
        "Biomass (MJ)": 0.0,
    }
    for flow_id, index in lca_obj.dicts.biosphere.items():
        flow = bd.get_node(id=flow_id)
        value = inventory_array[index].item()
        if flow in coal_set:
            raw_materials["Coal (kg)"] += value
        elif flow in uranium_set:
            raw_materials["Uranium (kg)"] += value
        elif flow in gas_set:
            raw_materials["Gas (m3)"] += value
        elif flow in water_set:
            raw_materials["Water (m3)"] += value
        elif flow in oil_set:
            raw_materials["Oil (kg)"] += value
        elif flow in land_set:
            raw_materials["Land (m2)"] += value
        elif flow in biomass_energy_set:
            raw_materials["Biomass (MJ)"] += value

    return raw_materials

def create_scenario_values(new_db_name: str, csv_file):
    # find new acts
    new_acts = [a for a in bd.Database(new_db_name) if ['(new)' in a['name']]]

    pass


def plot_input_data(
    path,
    save_path,
    year="2050",
    region=None,
    region_agg="mean",   # "mean" or "sum" when region is None
    cols=4,
    figsize_per_row=5,
):
    df = pd.read_csv(path)
    if "variables" not in df.columns:
        raise ValueError("Expected a 'variables' column in the CSV.")
    if year not in df.columns:
        raise ValueError(f"Expected a '{year}' column in the CSV.")
    split_cols = df["variables"].astype(str).str.split("|", expand=True)
    if split_cols.shape[1] < 3:
        raise ValueError("Expected variables formatted like 'Share|Carrier|Route'.")
    df["carriers"] = split_cols[1]
    df["routes"] = split_cols[2]
    df["routes"] = df["routes"].apply(lambda x: "Imports" if "Import" in str(x) else x)
    keep_cols = ["region", "carriers", "routes", year]
    missing = [c for c in keep_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    df = df[keep_cols].copy()
    if region is not None:
        df_plot = df[df["region"] == region].copy()
        if df_plot.empty:
            available = sorted(df["region"].unique())
            raise ValueError(
                f"Region '{region}' not found. Available regions include: {available[:10]} ..."
            )
        df_plot = df_plot.groupby(["carriers", "routes"], as_index=False)[year].sum()
    else:
        per_region = df.groupby(["region", "carriers", "routes"], as_index=False)[year].sum()
        if region_agg == "mean":
            df_plot = per_region.groupby(["carriers", "routes"], as_index=False)[year].mean()
        elif region_agg == "sum":
            df_plot = per_region.groupby(["carriers", "routes"], as_index=False)[year].sum()
        else:
            raise ValueError("region_agg must be 'mean' or 'sum'.")
    unique_carriers = sorted(df_plot["carriers"].unique())
    all_routes = sorted(df_plot["routes"].unique())
    cmap = plt.get_cmap("tab20")
    route_color_map = {route: cmap(i % 20) for i, route in enumerate(all_routes)}
    rows = math.ceil(len(unique_carriers) / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(30, figsize_per_row * rows))
    axes = axes.flatten()
    for i, carrier in enumerate(unique_carriers):
        ax = axes[i]
        carrier_df = df_plot[df_plot["carriers"] == carrier]
        bottom = 0.0
        for _, r in carrier_df.iterrows():
            route = r["routes"]
            share = float(r[year])
            ax.bar(
                carrier,
                share,
                bottom=bottom,
                color=route_color_map[route],
                label=route,
                edgecolor="black",
                linewidth=0.5,
            )
            bottom += share
        ax.set_title(carrier, fontsize=24, fontweight="bold")
        ax.set_ylabel("Share", fontsize=18)
        ax.set_xticks([])
        ax.tick_params(axis="y", labelsize=18)
        ax.set_ylim(0, 1.05)
        ax.grid(axis="y", linestyle=":", linewidth=0.6, alpha=0.6)
        # -------- Per-snippet legend (upper-right, vertical) --------
        handles, labels = ax.get_legend_handles_labels()
        uniq = dict(zip(labels, handles))  # deduplicate
        if carrier == 'ElectricityHV':
            legend_fontsize = 11
        else:
            legend_fontsize = 16
        leg = ax.legend(
            uniq.values(),
            uniq.keys(),
            loc="upper right",
            bbox_to_anchor=(1.0, 1.0),
            ncol=1,  # <-- vertical legend
            fontsize=legend_fontsize,
            frameon=True,
            borderaxespad=0.2,
            handlelength=1.2,
            labelspacing=0.4,
        )
        leg.get_frame().set_alpha(0.70)
        # -----------------------------------------------------------
        # -----------------------------------------------------
    for j in range(len(unique_carriers), len(axes)):
        axes[j].axis("off")
    # Leave space at the top of each axes legend; tighten overall
    plt.tight_layout()
    fig.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _compute_and_store_lcia_scores(act, lcia_methods, year: str, carrier_name: str, amount: float = 1, unit: str = None):
    lca_obj = act.lca(amount=amount)
    if unit is None:
        unit = act['unit']
    if '(new)' in act['reference product']:
        name = act['name'][:-6]
        product = act['reference product'][:-6]
    else:
        name = act['name']
        product = act['reference product']
    lcia_results = {'name': name, 'location': act['location'],
                    'product': product, 'scenario': year, 'unit': unit, 'carrier': carrier_name}
    for m in lcia_methods:
        lca_obj.switch_method(m)
        lca_obj.lcia()
        lcia_results[f"{str(m[1])}; {str(m[2])}"] = lca_obj.score
    return lcia_results

def analysis(custom_db_names: list,
             cutoff_db_name: str = 'premise_original_update', bw25_project_name: str = 'bw25_matrix'):
    # Set bw25 project
    bd.projects.set_current(bw25_project_name)

    # Set LCIA methods
    ef_31 = [m for m in bd.methods if str(m[0]) == 'EF v3.1']

    ### STEEL ###
    # Original steel
    print('starting steel')
    steel_results = {}
    steel_original_glo = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market for steel, low-alloyed'
                          and a['reference product'] == 'steel, low-alloyed'
                          and a['location'] == 'GLO'][0]
    lcia_results = _compute_and_store_lcia_scores(steel_original_glo, ef_31, year='current', carrier_name='steel')
    steel_results['steel current market (GLO)'] = lcia_results
    steel_original_bof = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'steel production, converter, low-alloyed'
                          and a['reference product'] == 'steel, low-alloyed'
                          and a['location'] == 'RER'][0]
    lcia_results = _compute_and_store_lcia_scores(steel_original_bof, ef_31, year='current', carrier_name='steel')
    steel_results['steel current BOF (RER)'] = lcia_results

    try:
        steel_original_rer = [a for a in bd.Database('additional_acts') if a['name'] == 'market for steel, low-alloyed, 2029'][0]
        lcia_results = _compute_and_store_lcia_scores(steel_original_rer, ef_31, year='current', carrier_name='steel')
        steel_results['steel current market (RER)'] = lcia_results
    except:
        pass

    # steel new pathways
    for custom_db_name in custom_db_names:
        steel_new_acts = [a for a in bd.Database(custom_db_name) if
                      'steel production' in a['name'] and a['reference product'] == 'steel, low-alloyed' and (
                                  a['location'] == 'RER' or ('electric' in a['name'] and a[
                              'location'] == 'Europe without Switzerland and Austria'))]
        for act in steel_new_acts:
            lcia_results = _compute_and_store_lcia_scores(act, ef_31, year=custom_db_name[-4:], carrier_name='steel')
            steel_results[f"steel custom - {act['name']} {custom_db_name[-4:]}"] = lcia_results

    ### ELECTRICITY ###
    # Original electricity
    print('starting electricity')
    electricity_carriers_demand = {}
    electricity_results = {}
    #act_hv = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market group for electricity, high voltage' and a['location'] == 'RER'][0]
    #act_mv = [a for a in bd.Database(cutoff_db_name) if
    #          a['name'] == 'market group for electricity, medium voltage' and a['location'] == 'RER'][0]
    act_lv = [a for a in bd.Database(cutoff_db_name) if
              a['name'] == 'market group for electricity, low voltage' and a['location'] == 'RER'][0]

    lcia_results = _compute_and_store_lcia_scores(act_lv, ef_31, year='current', carrier_name='electricity')
    electricity_results[f"electricity current - {act_lv['name']}"] = lcia_results

    electricity_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act_lv, amount=1)
    electricity_carriers_demand[f"{act_lv['name']} pes current"] = electricity_raw

    # new production routes
    for custom_db_name in custom_db_names:
        act_hv = [a for a in bd.Database(custom_db_name) if
                  a['name'] == 'market group for electricity, high voltage' and a['location'] == 'RER'][0]
        act_mv = [a for a in bd.Database(custom_db_name) if
                  a['name'] == 'market group for electricity, medium voltage' and a['location'] == 'RER'][0]
        act_lv = [a for a in bd.Database(custom_db_name) if
                  a['name'] == 'market group for electricity, low voltage' and a['location'] == 'RER'][0]
        lcia_results = _compute_and_store_lcia_scores(act_lv, ef_31, year=custom_db_name[-4:], carrier_name='electricity')
        electricity_results[f"electricity custom - {act_lv['name']} {custom_db_name[-4:]}"] = lcia_results

        hv_products_demand = demand_array(new_db_name=custom_db_name, analysis_act=act_hv, amount=1)
        electricity_carriers_demand[f"{act_hv['name']} carriers {custom_db_name[-4:]}"] = hv_products_demand
        mv_products_demand = demand_array(new_db_name=custom_db_name, analysis_act=act_mv, amount=1)
        electricity_carriers_demand[f"{act_mv['name']} carriers {custom_db_name[-4:]}"] = mv_products_demand
        lv_products_demand = demand_array(new_db_name=custom_db_name, analysis_act=act_lv, amount=1)
        electricity_carriers_demand[f"{act_lv['name']} carriers {custom_db_name[-4:]}"] = lv_products_demand

        electricity_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act_lv, amount=1)
        electricity_carriers_demand[f"{act_lv['name']} pes {custom_db_name[-4:]}"] = electricity_raw
        electricity_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act_mv, amount=1)
        electricity_carriers_demand[f"{act_mv['name']} pes {custom_db_name[-4:]}"] = electricity_raw
        electricity_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act_hv, amount=1)
        electricity_carriers_demand[f"{act_hv['name']} pes {custom_db_name[-4:]}"] = electricity_raw


    ### BIOMASS ###
    # Original biomass
    print('starting biomass')
    biomass_carriers_demand = {}
    biomass_results = {}
    wood_chips_wet = [a for a in bd.Database(cutoff_db_name) if
                      a['name'] == 'market for wood chips, wet, measured as dry mass'
                      and a['location'] == 'Europe without Switzerland'][0]
    wood_chips_dry = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market for wood chips, dry, measured as dry mass'
                      and a['location'] == 'RER'][0]
    wood_pellets = \
    [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market for wood pellet, measured as dry mass'
     and a['location'] == 'RER'][0]

    lcia_results = _compute_and_store_lcia_scores(wood_chips_wet, ef_31, year='current', amount=8.7, unit='MJ', carrier_name='biomass')
    biomass_results[f"biomass current - {wood_chips_wet['name']}"] = lcia_results
    lcia_results = _compute_and_store_lcia_scores(wood_chips_dry, ef_31, year='current', amount=19, unit='MJ', carrier_name='biomass')
    biomass_results[f"biomass current - {wood_chips_dry['name']}"] = lcia_results
    lcia_results = _compute_and_store_lcia_scores(wood_pellets, ef_31, year='current', amount=17, unit='MJ', carrier_name='biomass')
    biomass_results[f"biomass current - {wood_pellets['name']}"] = lcia_results

    biomass_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=wood_chips_wet, amount=8.7)
    biomass_carriers_demand[f"{wood_chips_wet['name']} current"] = biomass_raw
    biomass_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=wood_chips_dry, amount=19)
    biomass_carriers_demand[f"{wood_chips_dry['name']} current"] = biomass_raw
    biomass_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=wood_pellets, amount=17)
    biomass_carriers_demand[f"{wood_pellets['name']} current"] = biomass_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                          a['name'] == 'market for biomass, used as fuel (new)'
                          and a['location'] == 'Europe without Switzerland'][0]
        # 1. Caluclate LHV of new market:
        forest_act = False
        chips_act = False
        for ex in new_act.technosphere():
            if ex.input['name'] == 'supply of forest residue':
                forest_heat = ex['amount'] * 19
                forest_act = True
            elif ex.input['name'] == 'market for wood chips, wet, measured as dry mass':
                chips_heat = ex['amount'] * 8.7
                chips_act = True
        if not forest_act:
            forest_heat = 0
        if not chips_act:
            chips_heat = 0
        new_biomass_act_lhv = forest_heat + chips_heat
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:],
                                                      amount=new_biomass_act_lhv, unit='MJ', carrier_name='biomass')
        biomass_results[f"biomass custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        biomass_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=new_biomass_act_lhv)
        biomass_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = biomass_ec_demand
        biomass_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=wood_chips_wet, amount=new_biomass_act_lhv)
        biomass_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = biomass_raw

    ### HYDROGEN ###
    # Original hydrogen
    print('starting hydrogen')
    hydrogen_carriers_demand = {}
    hydrogen_results = {}
    h2_market_glo = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market for hydrogen, gaseous' and
                     a['location'] == 'GLO'][0]
    h2_sr = [a for a in bd.Database(cutoff_db_name) if
                      a['name'] == 'hydrogen production, steam methane reforming'][0]
    h2_autothermal = [a for a in bd.Database(cutoff_db_name) if
                      a['name'] == 'hydrogen production, auto-thermal reforming'][0]

    for act in [h2_autothermal, h2_sr, h2_market_glo]:
        lcia_results = _compute_and_store_lcia_scores(act, ef_31, year='current', carrier_name='hydrogen')
        hydrogen_results[f"hydrogen current - {act['name']}"] = lcia_results
        hydrogen_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act, amount=1)
        hydrogen_carriers_demand[f"{act['name']} current"] = hydrogen_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                          a['name'] == 'market for hydrogen (new)'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='hydrogen')
        hydrogen_results[f"hydrogen custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        hydrogen_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        hydrogen_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = hydrogen_ec_demand
        hydrogen_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        hydrogen_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = hydrogen_raw

    ### METHANOL ###
    # Original methanol (from natural gas)
    print('starting methanol')
    methanol_carriers_demand = {}
    methanol_results = {}
    methanol_market_glo = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market for methanol' and
                     a['location'] == 'GLO'][0]
    lcia_results = _compute_and_store_lcia_scores(act, ef_31, year='current', carrier_name='methanol')
    methanol_results[f"methanol current - {methanol_market_glo['name']}"] = lcia_results

    methanol_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act, amount=1)
    methanol_carriers_demand[f"{methanol_market_glo['name']} current"] = methanol_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                          a['name'] == 'market for methanol (new)'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='methanol')
        methanol_results[f"methanol custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        methanol_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        methanol_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = methanol_ec_demand
        methanol_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        methanol_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = methanol_raw

    ### KEROSENE ###
    # Original kerosene
    print('starting kerosene')
    kerosene_carriers_demand = {}
    kerosene_results = {}
    kerosene_markets = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market for kerosene'
                        and a['location'] in ['Europe without Switzerland', 'CH']]
    for market in kerosene_markets:
        lcia_results = _compute_and_store_lcia_scores(market, ef_31, year='current', carrier_name='kerosene')
        kerosene_results[f"kerosene current - {market['name']}"] = lcia_results
        kerosene_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=market, amount=1)
        kerosene_carriers_demand[f"{market['name']} current"] = kerosene_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                       a['name'] == 'market for kerosene (new)'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='kerosene')
        kerosene_results[f"kerosene custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        kerosene_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        kerosene_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = kerosene_ec_demand
        kerosene_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        kerosene_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = kerosene_raw

    ### DIESEL ###
    # Original diesel
    print('starting diesel')
    diesel_carriers_demand = {}
    diesel_results = {}
    diesel_markets = [a for a in bd.Database(cutoff_db_name) if 'market for diesel' in a['name']
                      and a['location'] in ['Europe without Switzerland', 'CH'] and a['unit'] == 'kilogram']
    for market in diesel_markets:
        lcia_results = _compute_and_store_lcia_scores(market, ef_31, year='current', carrier_name='diesel')
        diesel_results[f"diesel current - {market['name']}"] = lcia_results
        diesel_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=market, amount=1)
        diesel_carriers_demand[f"{market['name']} current"] = diesel_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                   a['name'] == 'market for diesel (new)'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='diesel')
        diesel_results[f"diesel custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        diesel_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        diesel_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = diesel_ec_demand
        diesel_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        diesel_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = diesel_raw

    ### LIQUEFIED PETROLEUM GAS ###
    # Original liquefied petroleum gas
    print('starting lpg')
    lpg_carriers_demand = {}
    lpg_results = {}
    lpg_markets = [a for a in bd.Database(cutoff_db_name) if a['name'] == 'market for liquefied petroleum gas'
                      and a['location'] in ['Europe without Switzerland', 'CH']]
    for market in lpg_markets:
        lcia_results = _compute_and_store_lcia_scores(market, ef_31, year='current', carrier_name='liquefied petroleum gas')
        lpg_results[f"lpg current - {market['name']}"] = lcia_results
        lpg_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=market, amount=1)
        lpg_carriers_demand[f"{market['name']} current"] = lpg_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                   a['name'] == 'market for liquefied petroleum gas (new)'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='liquefied petroleum gas')
        lpg_results[f"lpg custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        lpg_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        lpg_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = lpg_ec_demand
        lpg_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        lpg_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = lpg_raw

    ### METHANE ###
    # Original methane
    print('starting methane')
    methane_carriers_demand = {}
    methane_results = {}
    methane_markets = [a for a in bd.Database(cutoff_db_name) if
                       a['name'] == 'market group for natural gas, high pressure']
    for market in methane_markets:
        lcia_results = _compute_and_store_lcia_scores(market, ef_31, year='current', carrier_name='methane')
        methane_results[f"methane current - {market['name']}"] = lcia_results
        methane_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=market, amount=1)
        methane_carriers_demand[f"{market['name']} current"] = methane_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                   a['name'] == 'market group for natural gas, high pressure'
                   and a['location'] == 'Europe without Switzerland'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='methane')
        methane_results[f"methane custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        methane_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        methane_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = methane_ec_demand
        methane_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        methane_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = methane_raw

    ### LUBRICATING OIL ###
    # Original lubricating oil
    print('starting lubricating oil')
    lubricating_oil_carriers_demand = {}
    lubricating_oil_results = {}
    lubricating_oil_markets = [a for a in bd.Database(cutoff_db_name) if
                       a['name'] == 'market for lubricating oil']
    for market in lubricating_oil_markets:
        lcia_results = _compute_and_store_lcia_scores(market, ef_31, year='current', carrier_name='lubricating oil')
        lubricating_oil_results[f"lubricating oil current - {market['name']}"] = lcia_results
        lubricating_oil_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=market, amount=1)
        lubricating_oil_carriers_demand[f"{market['name']} current"] = lubricating_oil_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                   a['name'] == 'market for lubricating oil (new)'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='lubricating oil')
        lubricating_oil_results[f"lubricating oil custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        lubricating_oil_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        lubricating_oil_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = lubricating_oil_ec_demand
        lubricating_oil_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        lubricating_oil_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = lubricating_oil_raw

    ### HEAT ###
    # Original heat
    print('starting heat')
    heat_carriers_demand = {}
    heat_results = {}
    act_cs_nat_gas = [a for a in bd.Database(cutoff_db_name) if
              a['name'] == 'market for heat, central or small-scale, natural gas'
                      and a['location'] in ['Europe without Switzerland', 'CH']]
    act_cs_not_nat_gas = [a for a in bd.Database(cutoff_db_name) if
                      a['name'] == 'market for heat, central or small-scale, other than natural gas'
                      and a['location'] in ['Europe without Switzerland', 'CH']]
    act_district_nat_gas = [a for a in bd.Database(cutoff_db_name) if
                      a['name'] == 'market for heat, district or industrial, natural gas'
                      and a['location'] in ['Europe without Switzerland', 'CH']]
    act_district_not_nat_gas = [a for a in bd.Database(cutoff_db_name) if
                          a['name'] == 'market for heat, district or industrial, other than natural gas'
                          and a['location'] in ['Europe without Switzerland', 'CH']]

    for act_gourps in [act_cs_nat_gas, act_cs_not_nat_gas, act_district_nat_gas, act_district_not_nat_gas]:
        for act in act_gourps:
            lcia_results = _compute_and_store_lcia_scores(act, ef_31, year='current', carrier_name='heat')
            heat_results[f"heat current - {act['name']}"] = lcia_results
            heat_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act, amount=1)
            heat_carriers_demand[f"{act['name']} current"] = heat_raw

    # new production routes
    for custom_db_name in custom_db_names:
        act_cs = [a for a in bd.Database(custom_db_name) if
                  a['name'] == 'market for heat, central or small-scale (new)'][0]
        act_district = [a for a in bd.Database(custom_db_name) if
                  a['name'] == 'market for heat, district or industrial (new)'][0]
        for act in [act_cs, act_district]:
            lcia_results = _compute_and_store_lcia_scores(act, ef_31, year=custom_db_name[-4:], carrier_name='heat')
            heat_results[f"heat custom - {act['name']} {custom_db_name[-4:]}"] = lcia_results

            heat_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=act, amount=1)
            heat_carriers_demand[f"{act['name']} carriers {custom_db_name[-4:]}"] = heat_ec_demand
            heat_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=act, amount=1)
            heat_carriers_demand[f"{act['name']} pes {custom_db_name[-4:]}"] = heat_raw

    ### COAL ###
    # Original coal
    print('starting coal')
    coal_carriers_demand = {}
    coal_results = {}
    coal_market = [a for a in bd.Database(cutoff_db_name) if
                               a['name'] == 'market for hard coal'
                   and a['location'] == 'Europe, without Russia and Turkey'][0]

    lcia_results = _compute_and_store_lcia_scores(coal_market, ef_31, year='current', carrier_name='coal')
    coal_results[f"coal current - {coal_market['name']}"] = lcia_results
    coal_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=coal_market, amount=1)
    coal_carriers_demand[f"{coal_market['name']} current"] = coal_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                   a['name'] == 'market for coal, for energy uses (new)'][0]
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:], carrier_name='coal')
        coal_results[f"coal custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        coal_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=1)
        coal_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = coal_ec_demand
        coal_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=1)
        coal_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = coal_raw

    ### LIGNITE ###
    # Original lignite
    print('starting lignite')
    lignite_carriers_demand = {}
    lignite_results = {}
    lignite_market = [a for a in bd.Database(cutoff_db_name) if
                       a['name'] == 'market for lignite'
                       and a['location'] == 'RER'][0]

    lcia_results = _compute_and_store_lcia_scores(lignite_market, ef_31, year='current', unit='MJ', amount=11, carrier_name='lignite')
    lignite_results[f"lignite current - {lignite_market['name']}"] = lcia_results
    lignite_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=lignite_market, amount=11)
    lignite_carriers_demand[f"{lignite_market['name']} current"] = lignite_raw

    # new production route
    for custom_db_name in custom_db_names:
        new_act = [a for a in bd.Database(custom_db_name) if
                       a['name'] == 'market for lignite, for energy uses (new)'][0]
        lignite_is_present = False
        charcoal_is_present = False
        for ex in new_act.technosphere():
            if ex.input['name'] == 'market for lignite':
                lignite_heat = ex['amount'] * 11
                lignite_is_present = True
            if ex.input['reference product'] == 'charcoal':
                charcoal_heat = ex['amount'] * 30
                charcoal_is_present = True
        if not lignite_is_present:
            lignite_heat = 0
        if not charcoal_is_present:
            charcoal_heat = 0
        new_lignite_act_lhv = lignite_heat + charcoal_heat
        lcia_results = _compute_and_store_lcia_scores(new_act, ef_31, year=custom_db_name[-4:],
                                                      unit='MJ', amount=new_lignite_act_lhv, carrier_name='lignite')
        lignite_results[f"lignite custom - {new_act['name']} {custom_db_name[-4:]}"] = lcia_results

        lignite_ec_demand = demand_array(new_db_name=custom_db_name, analysis_act=new_act, amount=new_lignite_act_lhv)
        lignite_carriers_demand[f"{new_act['name']} carriers {custom_db_name[-4:]}"] = lignite_ec_demand
        lignite_raw = pes_demand(biosphere_db_name='biosphere3', analysis_act=new_act, amount=11)
        lignite_carriers_demand[f"{new_act['name']} pes {custom_db_name[-4:]}"] = lignite_raw

    all_lcia_results = (steel_results | electricity_results | biomass_results | hydrogen_results | methanol_results |
                   kerosene_results | diesel_results | lpg_results | lubricating_oil_results | coal_results
                   | lignite_results | heat_results | methane_results)
    all_carriers_results = (electricity_carriers_demand | biomass_carriers_demand | hydrogen_carriers_demand |
                            methanol_carriers_demand |
                   kerosene_carriers_demand | diesel_carriers_demand | lpg_carriers_demand |
                            lubricating_oil_carriers_demand | coal_carriers_demand
                   | lignite_carriers_demand | heat_carriers_demand | methane_carriers_demand)
    # TODO: maybe convert all_lcia_results in a dict of dicts where the key is the name of the carrier. Same with
    #  all_carriers_results. In this case, I'll need to change plot_analysis (maybe not!)?
    df_lcia = pd.DataFrame(all_lcia_results)
    df_carriers = pd.DataFrame(all_carriers_results)
    return df_lcia, df_carriers


def plot_analysis(
    df: pd.DataFrame,
    indicator: str,
    carrier_row: str = "carrier",
    scenario_row: str = "scenario",
    name_row: str = "name",
    location_row: str = "location",
    sort_by: str = "scenario",          # non-special sorting: "scenario" | "name" | None
    unit_row: str = "unit",
    rotation: int = 45,
    ylabel: str | None = None,
    title_prefix: str = "",
    cmap_name: str = "tab20",
    include_location_for_special: bool = True,
    fig_size: tuple[float, float] = (10, 7.5),
    bottom_margin: float = 0.42,
):
    """
    One figure per product. Within each figure, bars for all columns that share the same product
    for the given LCIA indicator row (wide dataframe).

    Special behavior for routes where `name` contains any keyword in `special_keywords`
    (default: steel/heat/electricity):
      - Map long route names to short codes (names_dict), robust to minor string differences.
      - Map locations to short codes (locations_dict), robust to minor string differences.
      - Order bars as:
          (A) scenario == 'current' block first (left)
          (B) all other scenarios block second (right)
        In each block: routes ranked by max(value) desc; within each route: value desc.
        (This groups the same production route next to each other across scenarios.)
      - Colors:
          - current block: dark neutral shades (black -> lighter blacks)
          - others: one base hue per route; within each route, progressively lighter shades.

    For non-special plots:
      - Keep previous sorting (scenario or name)
      - But force 'current' bars to use the same black as in special.
    """
    names_dict = {
        'steel production, electrowinning-electric arc furnace, low-alloyed': 'EW',
        'steel production, hydrogen-based direct reduction iron-electric arc furnace, low-alloyed': 'H2-DRI',
        'steel production, blast furnace-basic oxygen furnace, with top gas recycling, with carbon capture and storage, low-alloyed': 'BF/BOF top-gas CCS',
        'steel production, electric, low-alloyed': 'EAF',
        'steel production, blast furnace-basic oxygen furnace, with top gas recycling, low-alloyed': 'BF/BOF top-gas no CCS',
        'steel production, natural gas-based direct reduction iron-electric arc furnace, with carbon capture and storage, low-alloyed': 'NG-DRI CCS',
        'steel production, natural gas-based direct reduction iron-electric arc furnace, low-alloyed': 'NG_DRI no CCS',
        'steel production, blast furnace-basic oxygen furnace, with carbon capture and storage, low-alloyed': 'BF/BOF CCS',
        'market for steel, low-alloyed': 'market',
        'steel production, converter, low-alloyed': 'BF/BOF',
        'market for steel, low-alloyed, 2029': 'market',
        'market group for electricity, high voltage': 'electricity HV',
        'market group for electricity, medium voltage': 'electricity MV',
        'market group for electricity, low voltage': 'electricity LV',
        'hydrogen production, auto-thermal reforming': 'auto-thermal reforming',
        'hydrogen production, steam methane reforming': 'steam methane reforming',
        'market for hydrogen, gaseous': 'market',
        'market for diesel': 'diesel',
        'market for diesel, low-sulfur': 'diesel, low-sulfur',
        'market for heat, central or small-scale, natural gas': 'small-scale (nat gas)',
        'market for heat, central or small-scale, other than natural gas': 'small scale (other)',
        'market for heat, district or industrial, natural gas': 'district (nat gas)',
        'market for heat, district or industrial, other than natural gas': 'district (other)',
        'market for wood chips, wet, measured as dry mass': 'chips (wet)',
        'market for wood chips, dry, measured as dry mass': 'chips (dry)',
        'market for wood pellet, measured as dry mass': 'pellets',
        'market group for natural gas, high pressure': 'methane',
        'market for heat, district or industrial': 'district',
        'market for lignite': 'lignite',
        'market for lignite, for energy uses': 'lignite',
        'market for hard coal': 'coal',
        'market for coal, for energy uses': 'coal - charcoal',
        'market for lubricating oil': 'lubricating oil',
        'market for liquefied petroleum gas': 'LPG',
        'market for kerosene': 'kerosene',
        'market for methanol': 'methanol',
        'market for hydrogen': 'hydrogen',
        'market for biomass, used as a fuel': 'biomass'
    }

    locations_dict = {'Europe without Switzerland': 'RER wo CH',
                      'Europe without Switzerland and Austria': 'RER wo CH/AT',
                      'Europe, without Russia and Turkey': 'RER wo RU/TR'}

    units_dict = {'kilogram': 'kg', 'kilowatt hour': 'kWh', 'cubic meter': 'm3'}

    # ---------- helpers ----------
    def norm(s: str) -> str:
        s = str(s)
        s = s.replace("\u00A0", " ")  # NBSP -> space
        s = s.replace("–", "-").replace("—", "-").replace("−", "-")
        s = re.sub(r"\s+", " ", s).strip()
        return s.casefold()

    def strip_trailing_year(s: str) -> str:
        return re.sub(r"(?:,?\s+)\d{4}$", "", str(s)).strip()

    def expand_names_mapping(d: dict[str, str]) -> dict[str, str]:
        out = {}
        for k, v in d.items():
            candidates = {str(k)}
            if str(k).startswith("steel custom - "):
                candidates.add(str(k).replace("steel custom - ", "", 1))
            for c in list(candidates):
                candidates.add(strip_trailing_year(c))
            for c in candidates:
                out[norm(c)] = v
        return out

    def map_series_norm(s: pd.Series, mapping_norm: dict[str, str]) -> pd.Series:
        s_clean = (
            s.astype(str)
            .map(lambda x: re.sub(r"\s+", " ", x.replace("\u00A0", " ")).strip())
        )
        return s_clean.map(lambda x: mapping_norm.get(norm(strip_trailing_year(x)), x))

    def lighten(color, amount=0.35):
        r, g, b = mcolors.to_rgb(color)
        return (r + (1 - r) * amount, g + (1 - g) * amount, b + (1 - b) * amount)

    def is_current_scenario(x: str) -> bool:
        return str(x).strip().casefold() == "current"

    # ---------- checks ----------
    for needed in [carrier_row, scenario_row, indicator]:
        if needed not in df.index:
            raise ValueError(
                f"Row '{needed}' not found in df.index. "
                f"Index begins with: {list(df.index[:15])}"
            )

    # metadata per column
    product_of_col = df.loc[carrier_row]
    scenario_of_col = df.loc[scenario_row].astype(str)

    name_of_col = df.loc[name_row].astype(str) if name_row in df.index else None
    location_of_col = df.loc[location_row].astype(str) if location_row in df.index else None
    unit_of_col = df.loc[unit_row].astype(str) if unit_row in df.index else None

    # indicator values per column
    values = pd.to_numeric(df.loc[indicator], errors="coerce")

    # group columns by product
    products = product_of_col.dropna().unique()

    figs, axes = {}, {}
    cmap = plt.get_cmap(cmap_name)

    names_dict_norm = expand_names_mapping(names_dict)
    locations_dict_norm = {norm(k): v for k, v in locations_dict.items()}

    def scen_key_simple(s):
        s0 = str(s).strip().lower()
        if s0 == "current":
            return (-1, -1)
        try:
            return (0, int(float(s0)))
        except Exception:
            return (1, s0)

    for prod in products:
        cols = product_of_col[product_of_col == prod].index

        sub = pd.DataFrame(
            {
                "value": values[cols],
                "scenario": scenario_of_col[cols].astype(str),
            },
            index=cols,
        )

        if name_of_col is not None:
            sub["name"] = name_of_col[cols].astype(str)
        if location_of_col is not None:
            sub["location"] = location_of_col[cols].astype(str)

        # ---- always apply the route routines (when name exists) ----
        if "name" in sub.columns:
            name_clean = sub["name"].astype(str)
            short_name = map_series_norm(name_clean, names_dict_norm)
            is_special = True
        else:
            short_name = None
            is_special = False  # can't group by route without a name

        # location mapping (same routine)
        if "location" in sub.columns:
            loc_clean = sub["location"].astype(str)
            short_loc = loc_clean.map(lambda x: locations_dict_norm.get(norm(x), x))
        else:
            short_loc = None

        if unit_of_col is not None:
            sub["unit"] = unit_of_col[cols].astype(str)

        # ---- ordering + labels + colors ----
        if is_special:
            sub = sub.copy()
            sub["route"] = short_name.loc[sub.index].astype(str)

            # current block first
            is_cur = sub["scenario"].map(is_current_scenario)
            sub["block"] = is_cur.map({True: 0, False: 1})  # 0 first

            # rank routes within each block by max value (high -> low)
            def rank_routes(sdf: pd.DataFrame) -> list[str]:
                if sdf.empty:
                    return []
                return (
                    sdf.groupby("route")["value"]
                    .max()
                    .sort_values(ascending=False)
                    .index
                    .tolist()
                )

            routes_current = rank_routes(sub[sub["block"] == 0])
            routes_other = rank_routes(sub[sub["block"] == 1])

            order_current = {r: i for i, r in enumerate(routes_current)}
            order_other = {r: i for i, r in enumerate(routes_other)}

            def route_order(row):
                r = row["route"]
                return (order_current if row["block"] == 0 else order_other).get(r, 10_000)

            sub["route_order"] = sub.apply(route_order, axis=1)

            # sort: block -> route rank -> route -> value desc
            # (this keeps same routes adjacent across scenarios, and orders high->low)
            sub = sub.sort_values(
                by=["block", "route_order", "route", "value"],
                ascending=[True, True, True, False],
            )

            # labels
            name_sorted = short_name.loc[sub.index].astype(str)
            if include_location_for_special and short_loc is not None:
                loc_sorted = short_loc.loc[sub.index].astype(str)
                xlabels = (sub["scenario"].astype(str) + " | " + name_sorted + " | " + loc_sorted).tolist()
            else:
                xlabels = (sub["scenario"].astype(str) + " | " + name_sorted).tolist()

            # colors: current = pale gray shades; others = pre-lightened route hues + gradient per route
            all_routes = list(dict.fromkeys(routes_current + routes_other))
            for r in sub["route"].astype(str).unique():
                if r not in all_routes:
                    all_routes.append(r)

            # 1) Make base route colors paler up-front
            base_colors_raw = {r: cmap(i % cmap.N) for i, r in enumerate(all_routes)}
            base_colors = {r: lighten(base_colors_raw[r], amount=0.25) for r in all_routes}

            # 2) "current" base should also be paler (dark gray, not near-black)
            current_base = (0.65, 0.65, 0.65)

            colors = []
            seen_current = {}
            seen_other = {}

            for _, row in sub.iterrows():
                r = str(row["route"])
                if row["block"] == 0:
                    # pale gray gradient
                    seen_current[r] = seen_current.get(r, 0) + 1
                    amt = min(0.55, 0.18 * (seen_current[r] - 1))  # gentle lightening steps
                    colors.append(lighten(current_base, amount=amt))
                else:
                    # paler route gradient
                    seen_other[r] = seen_other.get(r, 0) + 1
                    amt = min(0.70, 0.30 * (seen_other[r] - 1))
                    colors.append(lighten(base_colors[r], amount=amt))

        else:
            # non-special: previous sorting
            if sort_by == "scenario":
                sub = sub.sort_values(by="scenario", key=lambda x: x.map(scen_key_simple))
            elif sort_by == "name" and "name" in sub.columns:
                sub = sub.sort_values("name")

            xlabels = sub["scenario"].astype(str).tolist()

            # colors, but force "current" to same black as special
            current_base = (0.1, 0.1, 0.1)
            colors = []
            k = 0
            for _, row in sub.iterrows():
                if is_current_scenario(row["scenario"]):
                    colors.append(current_base)
                else:
                    colors.append(cmap(k % cmap.N))
                    k += 1

        # ---- plot ----
        n = len(sub)
        fig, ax = plt.subplots(figsize=fig_size)

        ax.bar(range(n), sub["value"].values, color=colors)

        ax.set_xticks(range(n))
        ax.set_xticklabels(xlabels, rotation=rotation, ha="right")

        if ylabel is not None and "unit" in sub.columns:
            # assume unit is constant within a product; if not, just show first
            u0 = sub["unit"].dropna().astype(str).iloc[0] if sub["unit"].notna().any() else ""
            u0_short = units_dict.get(u0, u0)
            ax.set_ylabel(f"{ylabel}/{u0_short}" if u0_short else ylabel)
        elif ylabel is not None:
            ax.set_ylabel(ylabel)
        else:
            ax.set_ylabel(indicator)
        ax.set_title(f"{title_prefix}{prod} — {indicator.split(';')[0]}")

        fig.subplots_adjust(bottom=bottom_margin)

        figs[str(prod)] = fig
        axes[str(prod)] = ax

    return figs, axes


def run():
    create_custom_database(output_database_name='custom_2020',
                           year=2020,
                           )
    substitute_windtrace_onshore(database_windtrace_should_substitute='custom_2020',
                                 ecoinvent_database_name='cutoff391', location_new_wind_act='RER',
                                     fleet_turbines_definition=config_parameters.BALANCED_ON_WIND_FLEET,
                             biosphere3=bd.Database('biosphere3'), european_locations_only=True)

    substitute_windtrace_offshore(database_windtrace_should_substitute='custom_2020',
        ecoinvent_database_name='cutoff391', location_new_wind_act='RER',
                                    fleet_turbines_definition=config_parameters.BALANCED_OFF_WIND_FLEET,
                              european_locations_only=True)

    substitute_windtrace_onshore(database_windtrace_should_substitute='custom_2050',
                                 ecoinvent_database_name='cutoff391', location_new_wind_act='RER',
                                     fleet_turbines_definition=config_parameters.BALANCED_ON_WIND_FLEET,
                             biosphere3=bd.Database('biosphere3'), european_locations_only=True)

    substitute_windtrace_offshore(database_windtrace_should_substitute='custom_2050',
        ecoinvent_database_name='cutoff391', location_new_wind_act='RER',
                                    fleet_turbines_definition=config_parameters.BALANCED_OFF_WIND_FLEET,
                              european_locations_only=True)

    plot_input_data(
        path=r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\scenario_data\scenario_data_eur_custom_2020_2050.csv",
        year='2020',
        save_path=r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\plots\input_data_2020.png",
        region=None,          # aggregate across all regions
        region_agg="mean",    # mean is usually what you want for “average country”
    )
    plot_input_data(
        path=r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\scenario_data\scenario_data_eur_custom_2020_2050.csv",
        year='2050',
        save_path=r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\plots\input_data_2050.png",
        region=None,          # aggregate across all regions
        region_agg="mean",    # mean is usually what you want for “average country”
    )


df = analysis(custom_db_names=['custom_2020', 'custom_2050'])
figs, axes = plot_analysis(
    df=df,
    indicator="climate change; global warming potential (GWP100)",
    ylabel="kg CO2-eq",
)
plt.show()
pass