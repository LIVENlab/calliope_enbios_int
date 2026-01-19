from premise import NewDatabase
from datapackage import Package
import bw2data as bd
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
def create_custom_database():
    bd.projects.set_current('bw25_matrix')
    pkg = Package(r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\datapackage.json")

    external_scenario = [
        {"scenario": "Business As Usual", "data": pkg},
    ]
    ndb = NewDatabase(
        scenarios=[
            {
                "model": "image",
                "pathway": "SSP2-L",
                "year": 2020,
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
    for ex in ws.technosphere(new_biomass_act):
        if ex['name'] == 'supply of forest residue':
            forest_heat = ex['amount'] * 19
        elif ex['name'] == 'market for wood chips, wet, measured as dry mass':
            chips_heat = ex['amount'] * 8.7

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
    for ex in ws.technosphere(new_lignite_act):
        if ex['name'] == 'market for lignite':
            lignite_heat = ex['amount'] * 11
        elif ex['product'] == 'charcoal':
            charcoal_heat = ex['amount'] * 30
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

    ndb.write_db_to_brightway('test_2')


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

def apply_windtrace_onshore(db_wind_name: str, location: str,
                       fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                       biosphere3: bd.Database = bd.Database('biosphere3')):
    print('Creating onshore wind fleet')
    bd.projects.set_current(config_parameters.PROJECT_NAME)
    create_additional_acts_db()

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
            print("Park name duplicates found. Try other names")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit()

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
                                 location_new_wind_act: str,
                                 fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                                 biosphere3: bd.Database = bd.Database('biosphere3'),
                                 european_locations_only: bool = True):
    new_onshore_act = apply_windtrace_onshore(db_wind_name=ecoinvent_database_name, location=location_new_wind_act,
                                              fleet_turbines_definition=fleet_turbines_definition, biosphere3=biosphere3
                                              )
    european_locations = ['ES', 'BG', 'SE', 'AT', 'MK', 'MD', 'HR', 'XK', 'LU', 'GR', 'IS', 'BA', 'EE', 'SK',
                          'ME', 'LT', 'SI', 'IE', 'BE', 'RS', 'RO', 'NL', 'UA', 'PL', 'FR', 'GB', 'NO', 'CZ',
                          'MT', 'DK', 'IT', 'LV', 'DE', 'PT', 'FI', 'BY', 'GI', 'AL', 'HU', 'CH']
    onshore_wind_names = ['electricity production, wind, 1-3MW turbine, onshore',
                          'electricity production, wind, <1MW turbine, onshore',
                          'electricity production, wind, >3MW turbine, onshore']
    if european_locations_only:
        wind_acts_to_substitute = [a for a in bd.Database(ecoinvent_database_name) if a['name'] in onshore_wind_names
                               and a['location'] in european_locations]
    else:
        wind_acts_to_substitute = [a for a in bd.Database(ecoinvent_database_name) if a['name'] in onshore_wind_names]

    for act in wind_acts_to_substitute:
        for ex in act.consumers():
            ex.input = new_onshore_act
            ex.save()


def apply_windtrace_offshore(db_wind_name: str, location: str,
                             fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]]
                            ):
    print('Creating offshore wind fleet')
    bd.projects.set_current(config_parameters.PROJECT_NAME)
    create_additional_acts_db()

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
            print("Park name duplicates found. Try other names")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit()

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
                                 location_new_wind_act: str,
                                 fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                                 european_locations_only: bool = True):
    new_offshore_act = apply_windtrace_offshore(db_wind_name=ecoinvent_database_name, location=location_new_wind_act,
                                              fleet_turbines_definition=fleet_turbines_definition,
                                              )
    european_locations = ['ES', 'BG', 'SE', 'AT', 'MK', 'MD', 'HR', 'XK', 'LU', 'GR', 'IS', 'BA', 'EE', 'SK',
                          'ME', 'LT', 'SI', 'IE', 'BE', 'RS', 'RO', 'NL', 'UA', 'PL', 'FR', 'GB', 'NO', 'CZ',
                          'MT', 'DK', 'IT', 'LV', 'DE', 'PT', 'FI', 'BY', 'GI', 'AL', 'HU', 'CH']
    if european_locations_only:
        wind_acts_to_substitute = [a for a in bd.Database(ecoinvent_database_name) if
                                   a['name'] == 'electricity production, wind, 1-3MW turbine, offshore'
                                   and a['location'] in european_locations]
    else:
        wind_acts_to_substitute = [a for a in bd.Database(ecoinvent_database_name) if
                                   a['name'] == 'electricity production, wind, 1-3MW turbine, offshore']

    for act in wind_acts_to_substitute:
        for ex in act.consumers():
            ex.input = new_offshore_act
            ex.save()

##########################
# CREATE SCENARIO VALUES #
##########################

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


create_custom_database()
substitute_windtrace_onshore(ecoinvent_database_name='test_2', location_new_wind_act='RER',
                             fleet_turbines_definition=config_parameters.BALANCED_ON_WIND_FLEET,
                             biosphere3=bd.Database('biosphere3'), european_locations_only=True)

substitute_windtrace_offshore(ecoinvent_database_name='test_2', location_new_wind_act='RER',
                            fleet_turbines_definition=config_parameters.BALANCED_OFF_WIND_FLEET,
                              european_locations_only=True)

plot_input_data(
    path=r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\scenario_data\scenario_data_eur_template_random.csv",
    save_path=r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\plots\input_data.png",
    region=None,          # aggregate across all regions
    region_agg="mean",    # mean is usually what you want for “average country”
)
pass