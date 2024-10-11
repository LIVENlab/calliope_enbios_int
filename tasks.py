from typing import Literal, Optional, Dict, Any, List, Union
import pandas as pd
from premise.geomap import Geomap
import wurst.searching as ws
import sys
import bw2data as bd
import WindTrace.WindTrace_onshore
import consts
import wurst


# TODO:
#  1. Tests on battery, hydrogen and wind fleets
#  2. Change electricity production hydro dataset!! (land use to infrastructure and keep operation)
#  6. Setup databases and tests the functions (with workflow for foreground)
#  7. Formalise general workflow


##### assign location #####
def iam_location_equivalence():
    """
    :return: dictionary with location equivalence. IAM locations (keys) and locations included in
    ISO alpha-2 (values) in a list.
    """
    geo = Geomap('image')
    regions_codes = {}
    for region in geo.iam_regions:
        list_countries = []
        for iso_2 in geo.iam_to_ecoinvent_location(region):
            country_alpha2 = iso_2
            list_countries.append(country_alpha2)
        unique_countries = list(set(list_countries))
        regions_codes[region] = unique_countries
    return regions_codes


def activity_filter_check(bw_db_name: str, activity_name: str, location: str, unit: str):
    """
    :return: returns None if there is more than one activity, and returns the activity if it finds a single one
    (and it exits if it finds more than one activity)
    """
    bw_db = bd.Database(bw_db_name)
    if len(bw_db) == 0:
        print(f'empty database! You wrote {bw_db_name}, check the spelling')
    activity_filter = ws.get_many(bw_db,
                                  ws.contains('name', activity_name),
                                  ws.equals('location', location),
                                  ws.equals('unit', unit))
    activity_list = list(activity_filter)
    if len(activity_list) > 1:
        print(f'More than one {activity_name}, {location}, {unit} found')
        sys.exit()
    elif len(activity_list) == 0:
        print(f'No exact location for the activity {activity_name}, {location}, {unit}. '
              f'Trying next geography.')
        return None
    else:
        return activity_list[0]


def find_activity(bw_db_name: str, activity_name: str, calliope_location: str,
                  unit: str, known_ei_location: str = None):
    """
    :param calliope_location: string of the country/region acronym as it comes from Calliope
    :return: checks locations in this priority: 1. given location as a parameter, 2. exact,
    3. Europe without Switzerland, 4. RER, 5. IAMs locations, 6. CH, 7. Any European country, 8. RoW or GLO,
    9. Any other. It returns database and code of the activity.
    """

    # Check known location
    if known_ei_location:
        act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name,
                                    location=known_ei_location, unit=unit)
        if act:
            print(f'name: {activity_name}, location: {act._data["location"]}, '
                  f'code: {act.key[1]}, database: {act.key[0]}')
            return act.key[0], act.key[1]  # database and code

    # Check exact location
    location_alpha = consts.LOCATION_EQUIVALENCE[calliope_location]
    act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name, location=location_alpha, unit=unit)
    if act:
        print(f'name: {activity_name}, location: {act._data["location"]}, '
              f'code: {act.key[1]}, database: {act.key[0]}')
        return act.key[0], act.key[1]  # database and code

    # Check 'Europe without Switzerland'
    act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name,
                                location='Europe without Switzerland', unit=unit)
    if act:
        print(f'name: {activity_name}, location: {act._data["location"]}, '
              f'code: {act.key[1]}, database: {act.key[0]}')
        return act.key[0], act.key[1]  # database and code

    # Check 'RER'
    act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name,
                                location='RER', unit=unit)
    if act:
        print(f'name: {activity_name}, location: {act._data["location"]}, '
              f'code: {act.key[1]}, database: {act.key[0]}')
        return act.key[0], act.key[1]  # database and code

    # Check iams locations
    premise_locations = iam_location_equivalence()
    for iam_loc, ei_locs_list in premise_locations.items():
        if location_alpha in ei_locs_list:
            location_alpha = iam_loc
            continue
        else:
            print(f'{location_alpha} was not found in any iam region')
            sys.exit()
    act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name,
                                location=location_alpha, unit=unit)
    if act:
        print(f'name: {activity_name}, location: {act._data["location"]}, '
              f'code: {act.key[1]}, database: {act.key[0]}')
        return act.key[0], act.key[1]  # database and code

    # Check 'CH'
    act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name,
                                location='CH', unit=unit)
    if act:
        print(f'name: {activity_name}, location: {act._data["location"]}, '
              f'code: {act.key[1]}, database: {act.key[0]}')
        return act.key[0], act.key[1]  # database and code

    # Check any country in Europe
    european_countries = list(consts.LOCATION_EQUIVALENCE.values())
    for country in european_countries:
        act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name,
                                    location=country, unit=unit)
        if act:
            print(f'name: {activity_name}, location: {act._data["location"]}, '
                  f'code: {act.key[1]}, database: {act.key[0]}')
            return act.key[0], act.key[1]  # database and code

    # Check 'RoW' or 'GLO'
    # Skip this step if the activity is 'electricity production, hydro, run-of-river', as we want CA-QC because
    # it is clearer the power capacity (MW) of the dataset
    if activity_name != 'electricity production, hydro, run-of-river':
        for loc in ['RoW', 'GLO']:
            act = activity_filter_check(bw_db_name=bw_db_name, activity_name=activity_name,
                                        location=loc, unit=unit)
            if act:
                print(f'name: {activity_name}, location: {act._data["location"]}, '
                      f'code: {act.key[1]}, database: {act.key[0]}')
                return act.key[0], act.key[1]  # database and code

    # Check everything else
    activity_filter = ws.get_many(bw_db_name,
                                  ws.equals('name', activity_name),
                                  ws.equals('unit', unit))
    act = next(activity_filter, None)
    if act:
        print(f'name: {activity_name}, location: {act._data["location"]}, '
              f'code: {act.key[1]}, database: {act.key[0]}')
        return act.key[0], act.key[1]

    # print an error if nothing worked
    print(f'No activity named {activity_name} was found.')


##### eliminate infrastructure #####
def eliminate_infrastructure(electricity_heat_act):
    """
    :return: given an activity it checks if it is an electricity or heat production activity, it creates a copy
    in a new database 'electricity_and_heat_db' and it deletes its infrastructure exchanges (inputs).
    It returns the infrastructure input activity.
    """
    # test that it is an electricity or heat production activity
    if electricity_heat_act._data['unit'] not in ['megajoule', 'kilowatt hour']:
        print(f'The activity {electricity_heat_act} does neither produce heat nor electricity.')
        sys.exit()
    # create a copy in a new database
    if 'electricity_and_heat_db' not in bd.databases:
        ea_db = bd.Database('electricity_and_heat_db')
        ea_db.register()
    act_copy = electricity_heat_act.copy(database='electricity_and_heat_db')

    # find the infrastructure and eliminate it from the foreground
    infrastructure_acts = [e for e in act_copy.technosphere() if e.input._data['unit'] == 'unit']
    for e in infrastructure_acts:
        e.delete()

    return infrastructure_acts


##### individual changes #####
def create_additional_acts_db():
    if 'additional_acts' not in bd.databases:
        ea_db = bd.Database('additional_acts')
        ea_db.register()


def chp_waste_update(db_waste_name: str, db_original_name: str, locations: list):
    """
    Creates a copy of the activity 'treatment of municipal solid waste, incineration' for all locations given in the
    variable ``locations``.
    Creates an activity for the municipal solid waste incinerator.
    """
    # delete technosphere
    create_additional_acts_db()
    for location in locations:
        try:
            waste_original = ws.get_one(bd.Database(db_waste_name),
                                        ws.equals('name',
                                                  'treatment of municipal solid waste, incineration'),
                                        ws.equals('location', location),
                                        ws.contains('reference product', 'electricity')
                                        )
            print(f'original_location: {location}, assigned location: {location}')
            waste_act = waste_original.copy(database='additional_acts')
            print(f'creating copy of {waste_act._data["name"]}')
            waste_act.technosphere().delete()
            print(f'deleting technosphere')
        # if we do not find the location, Germany is chosen by default.
        except wurst.errors.NoResults:
            waste_original = ws.get_one(bd.Database(db_waste_name),
                                        ws.equals('name',
                                                  'treatment of municipal solid waste, incineration'),
                                        ws.equals('location', 'CH'),
                                        ws.contains('reference product', 'electricity')
                                        )
            print(f'original_location: {location}, assigned location: CH')
            waste_act = waste_original.copy(database='additional_acts')
            print(f'copy of {waste_act._data["name"]} created in "additional_acts"')
            print('changing location')
            waste_act['location'] = location
            waste_act['comment'] = waste_act['comment'] + '\n' + 'Taken dataset from CH'
            waste_act.save()
            waste_act.technosphere().delete()
            print(f'deleting technosphere')

    # create municipal solid waste incinerator
    incinerator_parts = ['furnace production, wood chips, with silo, 5000kW',
                         'heat and power co-generation unit construction, organic Rankine cycle, 1000kW electrical',
                         'dust collector production, electrostatic precipitator, for industrial use']
    new_act = bd.Database('additional_acts').new_activity(name='municipal solid waste incinerator',
                                                          code='municipal solid waste incinerator',
                                                          location='RER',
                                                          unit='unit',
                                                          )
    new_act['reference product'] = 'municipal solid waste incinerator'
    new_act.save()
    production_ex = new_act.new_exchange(input=new_act.key, type='production', amount=1)
    production_ex.save()
    for part in incinerator_parts:
        acts = ws.get_many(bd.Database(db_original_name), ws.contains('name', part),
                           ws.exclude(ws.contains('name', 'market')))
        list_acts = list(acts)
        if len(list_acts) > 1:
            part_act = [a for a in list_acts if a._data['location'] == 'CH'][0]
        else:
            part_act = list_acts[0]
        print(part_act._data['name'], part_act._data['location'])
        new_ex = new_act.new_exchange(input=part_act, type='technosphere', amount=1)
        new_ex.save()


def biofuel_to_methanol_update(db_methanol_name: str):
    """
    In the background of the methanol production, the function substitutes H2 production with CCS
    for H2 production without CCS.
    To execute only if we don't want CCS!!!
    """

    # Change name of the methanol distillation activity
    methanol_distillation_original = ws.get_one(bd.Database(db_methanol_name),
                                                ws.equals('name',
                                                          'methanol distillation, from wood, with CCS'),
                                                )
    create_additional_acts_db()
    methanol_distillation_act = methanol_distillation_original.copy(database='additional_acts')
    methanol_distillation_act['name'] = 'methanol distillation, from wood, without CCS'
    methanol_distillation_act.save()
    # Change name of the methanol synthesis activity
    methanol_synthesis_original = [ex.input for ex in methanol_distillation_act.technosphere() if
                                   'methanol synthesis, from wood, with CCS' in ex.input._data['name']][0]
    methanol_synthesis_act = methanol_synthesis_original.copy(database='additional_acts')
    methanol_synthesis_act['name'] = 'methanol synthesis, from wood, without CCS'
    methanol_synthesis_act.save()
    # Re-link methanol synthesis act to methanol distillation act
    methanol_synthesis_input_to_distillation = [ex for ex in methanol_distillation_act.technosphere() if
                                                'methanol synthesis, from wood, with CCS' in ex.input._data['name']][0]
    methanol_synthesis_input_to_distillation.input = methanol_synthesis_act.key
    methanol_synthesis_input_to_distillation.save()
    # Substitute CCS for gasification without CCS
    h2_exchange = [ex for ex in methanol_synthesis_act.technosphere() if
                   ex.input._data[
                       'name'] == ('hydrogen production, gaseous, 25 bar, from gasification of woody biomass'
                                   ' in entrained flow gasifier, with CCS, at gasification plant')][0]
    h2_new_act = ws.get_one(bd.Database(h2_exchange.input.key[0]),
                            ws.equals('name',
                                      'hydrogen production, gaseous, 25 bar, from gasification of woody biomass '
                                      'in entrained flow gasifier, at gasification plant'),
                            ws.equals('location', 'RER')
                            )
    h2_exchange.input = h2_new_act.key
    h2_exchange.save()


def trucks_update(db_truck_name: str):
    """
    Creates a copy of 'light duty truck, fuel cell electric, 3.5t gross weight, long haul' and
    'medium duty truck, fuel cell electric, 26t gross weight, long haul' in the database 'additional_acts_db' and
    deletes all inputs that are not in the keep_inputs list. The idea is to model the truck as ONLY those inputs that
    make the vehicle electric instead of using an internal combustion engine.
    """
    # light truck
    light_truck_original = ws.get_one(bd.Database(db_truck_name),
                                      ws.equals('name',
                                                'light duty truck, fuel cell electric, 3.5t gross weight, '
                                                'long haul'),
                                      )
    create_additional_acts_db()
    light_truck_act = light_truck_original.copy(database='additional_acts')
    light_truck_technosphere = list(light_truck_act.technosphere())
    keep_inputs = ['converter', 'inverter', 'fuel tank', 'power electronics', 'other components', 'electric motor',
                   'fuel cell system', 'electricity storage capacity']
    for ex in light_truck_technosphere:
        if not any(input_name in ex.input['name'] for input_name in keep_inputs):
            ex.delete()

    # medium truck
    medium_truck_original = ws.get_one(bd.Database(db_truck_name),
                                       ws.equals('name',
                                                 'medium duty truck, fuel cell electric, 26t gross weight, '
                                                 'long haul'),
                                       )
    medium_truck_act = medium_truck_original.copy(database='additional_acts')
    medium_truck_technosphere = list(medium_truck_act.technosphere())
    keep_inputs = ['power distribution', 'converter', 'inverter', 'fuel tank', 'power electronics', 'other components',
                   'electric motor', 'fuel cell system', 'battery capacity']
    for ex in medium_truck_technosphere:
        if not any(input_name in ex.input['name'] for input_name in keep_inputs):
            ex.delete()


def passenger_car_update(db_passenger_name: str):
    """
    Creates a copy of 'transport, passenger car, battery electric, Medium' in the database 'additional_acts_db' and
    deletes glider inputs.
    """
    car_original = ws.get_one(bd.Database(db_passenger_name),
                              ws.equals('name', 'passenger car, battery electric, Medium'),
                              )
    create_additional_acts_db()
    car_act = car_original.copy(database='additional_acts')
    car_technosphere = list(car_act.technosphere())
    for ex in car_technosphere:
        if 'glider' in ex.input['name']:
            ex.delete()


def gas_to_liquid_update(db_cobalt_name: str, db_gas_to_liquid_name: str):
    """
    Creates a copy of 'gas-to-liquid plant construction' in 'additional_acts' and adds 1250000 kg of cobalt
    as input (catalyst).
    """
    gas_to_liquid_original_act = ws.get_one(bd.Database(db_gas_to_liquid_name),
                                            ws.equals('name', 'gas-to-liquid plant construction'),
                                            )
    create_additional_acts_db()
    gas_to_liquid_act = gas_to_liquid_original_act.copy(database='additional_acts')
    cobalt_act = list(ws.get_many(bd.Database(db_cobalt_name), ws.equals('name', 'market for cobalt'),
                                  ws.equals('reference product', 'cobalt'),
                                  ))[0]
    production_ex = gas_to_liquid_act.new_exchange(input=gas_to_liquid_act.key, type='production', amount=1)
    production_ex.save()
    new_ex = gas_to_liquid_act.new_exchange(input=cobalt_act, type='technosphere', amount=1250000)
    new_ex.save()


def methane_from_biomass_factory():
    # Need from 'biomethane production, high pressure from synthetic gas, wood, fluidised technology' (CH)
    #  synthetic gas factory construction (0.14 units) and industrial furnace production, natural gas (1 unit)
    # TODO: wait for conversation with Jann. We would only need it if we do something similar as
    #  with the electricity and heat. Otherwise, we use the electricity_output and not the capacity,
    #  so do not remove the infrastructure.
    pass


def methanol_from_biomass_factory():
    # Need from 'methanol distillation, from wood, with CCS'
    # methanol production facility, construction (12.457+12.89 units per 1 kg of distilled methanol)
    # TODO: wait for conversation with Jann. We would only need it if we do something similar as
    #  with the electricity and heat. Otherwise, we use the electricity_output and not the capacity,
    #  so do not remove the infrastructure.
    pass


# TODO: biosphere 1 kg CO2 removal does not count as negative emissions. Ask Samantha how to deal with it.

##### create fleets #####
# wind_onshore
def wind_onshore_fleet(db_wind_name: str, location: str,
                       fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                       ):
    """
    ´´fleet_turbines_definition´´ structure:
    {'turbine_1': [
    {
    'power': , 'location': , 'manufacturer': , 'rotor_diameter': , 'hub_height': , 'commissioning_year': ,
    'generator_type': , 'recycled_share_steel': , 'lifetime': , 'eol_scenario':
    },
    0.5], # where this 0.5 is the share of turbine_1
    'turbine_2': [
    {
    'power': , 'location': , 'manufacturer': , 'rotor_diameter': , 'hub_height': , 'commissioning_year': ,
    'generator_type': , 'recycled_share_steel': , 'lifetime': , 'eol_scenario':
    },
    0.5], # where this 0.5 is the share of turbine_2
    """
    create_additional_acts_db()

    expected_keys = {'power', 'location', 'manufacturer', 'rotor_diameter', 'hub_height', 'commissioning_year',
                     'generator_type', 'recycled_share_steel', 'lifetime', 'eol_scenario'}
    park_names = []
    for turbine, info in fleet_turbines_definition.items():
        turbine_parameters = info[0]
        park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["location"]}'
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
        park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["location"]}'
        WindTrace.WindTrace_onshore.lci_wind_turbine(
            new_db=bd.Database('additional_acts'), cutoff391=bd.Database(db_wind_name),
            park_name=park_name, park_power=turbine_parameters['power'], number_of_turbines=1,
            park_location=turbine_parameters['location'], park_coordinates=(51.181, 13.655),
            manufacturer=turbine_parameters['manufacturer'], rotor_diameter=turbine_parameters['rotor_diameter'],
            turbine_power=turbine_parameters['power'], hub_height=turbine_parameters['hub_height'],
            commissioning_year=turbine_parameters['commissioning_year'],
            generator_type=turbine_parameters['generator_type'],
            recycled_share_steel=turbine_parameters['recycled_share_steel'],
            lifetime=turbine_parameters['lifetime'], eol_scenario=turbine_parameters['eol_scenario'],
            use_and_maintenance=False
        )

    # create fleet activity
    fleet_activity = bd.Database('additional_acts').new_activity(
        name='wind turbine fleet, 1 MW, for enbios',
        code='wind turbine fleet, 1 MW, for enbios',
        unit='unit',
        location=location
    )
    fleet_activity.save()
    new_ex = fleet_activity.new_exchange(input=fleet_activity.key, type='production', amount=1)
    new_ex.save()
    # add inputs
    for turbine, info in fleet_turbines_definition.items():
        share = info[1]
        turbine_parameters = info[0]
        park_name = f'{turbine}_{turbine_parameters["power"]}_{turbine_parameters["location"]}'
        single_turbine_activity = bd.Database('additional_acts').get(park_name + '_single_turbine')
        # to fleet activity (infrastructure)
        new_ex = fleet_activity.new_exchange(input=single_turbine_activity, type='technosphere',
                                             amount=share * turbine_parameters["power"])
        new_ex.save()
    return park_names


# wind_offshore
def wind_offshore_fleet():
    pass


# solar_pv
def solar_pv_fleet(db_solar_name: str,
                   open_technology_share: Dict[str, float] = consts.OPEN_TECHNOLOGY_SHARE,
                   roof_technology_share: Dict[str, float] = consts.ROOF_TECHNOLOGY_SHARE,
                   roof_3kw_share: Dict[str, float] = consts.ROOF_3KW_SHARE,
                   roof_93kw_share: Dict[str, float] = consts.ROOF_93KW_SHARE,
                   roof_156kw_share: Dict[str, float] = consts.ROOF_156KW_SHARE,
                   roof_280kw_share: Dict[str, float] = consts.ROOF_280KW_SHARE):
    """
    For open-ground, it creates a fleet of 570 kWp with the following technologies
    ('CdTe', 'CIS', 'micro-Si', 'multi-Si', 'single-Si').
    For rooftop:
    It creates a fleet of 3kWp with the following technologies
    ('single-Si', 'CIS', 'a-Si', 'multi-Si', 'CdTe', 'ribbon-Si').
    It creates a fleet of 93kWp with the following technologies ('multi-Si', 'single-Si').
    It creates a fleet of 156kWp with the following technologies ('multi-Si', 'single-Si').
    It creates a fleet of 280kWp with the following technologies ('multi-Si', 'single-Si').
    Finally, it creates a 1MW fleet with 3kWp, 93kWp, 156kWp, and 280kWp. This is the inventory we will use in enbios.
    Dictionaries:
    ´´open_technology_share´´ -> share of open-ground technologies, with the following keys ('CdTe', 'CIS', 'micro-Si', 'multi-Si', 'single-Si')
    ´´roof_3kw_share´´ -> share of 3kWp technologies with the following keys ('single-Si', 'CIS', 'a-Si', 'multi-Si', 'CdTe', 'ribbon-Si')
    ´´roof_93kw_share´´ -> share of 93kWp technologies with the following keys ('multi-Si', 'single-Si')
    ´´roof_156kw_share´´ -> share of 156kWp technologies with the following keys ('multi-Si', 'single-Si')
    ´´roof_280kw_share´´ -> share of 280kWp technologies with the following keys ('multi-Si', 'single-Si')
    ´´roof_technology_share´´ -> share of rooftop powers with the following keys ('3kWp', '93kWp', '156kWp', '280kWp')

    Default scenario defined in consts.py based on BaU taking data from Photovoltaics Report (2024) Fraunhofer Institute
    It returns the open-ground activity and the rooftop activity.

    Note: it deliberately avoids 1.3 MWp inventories and technologies stored in a different format
    (i.e., perovskite-on-silicon and GaAs). It also avoids laminated installations (all integrated).
    """
    # TODO: add maintenance (tap water, wastewater and water to air)
    create_additional_acts_db()
    # test technology shares
    # Runtime check to enforce battery types as keys
    open_expected_keys = {'CdTe', 'CIS', 'micro-Si', 'multi-Si', 'single-Si'}
    roof_3kw_expected_keys = {'single-Si', 'CIS', 'a-Si', 'multi-Si', 'CdTe', 'ribbon-Si'}
    roof_93kw_expected_keys = {'multi-Si', 'single-Si'}
    roof_156kw_expected_keys = {'multi-Si', 'single-Si'}
    roof_280kw_expected_keys = {'multi-Si', 'single-Si'}
    roof_technology_keys = {'93kWp', '3kWp', '156kWp', '280kWp'}
    if (
            (set(open_technology_share.keys()) != open_expected_keys or
             set(roof_3kw_share.keys()) != roof_3kw_expected_keys or
             set(roof_93kw_share.keys()) != roof_93kw_expected_keys or
             set(roof_156kw_share.keys()) != roof_156kw_expected_keys or
             set(roof_280kw_share.keys()) != roof_280kw_expected_keys or
             set(roof_technology_share.keys()) != roof_technology_keys)
    ):
        raise ValueError(f"technology shares must contain exactly the keys {open_expected_keys} (open-ground), "
                         f" {roof_3kw_expected_keys} (roof, 3kWp), {roof_93kw_expected_keys} (roof, 93kWp), "
                         f" {roof_156kw_expected_keys} (roof, 156kWp), {roof_280kw_expected_keys} (roof, 280kWp),and "
                         f"{roof_technology_keys} (share between 3kWp, 93kWp, 156kWp, and 280kWp)")
    if (sum(open_technology_share.values()) != 1 or sum(roof_technology_share.values()) != 1 or
            sum(roof_93kw_share.values()) != 1 or sum(roof_3kw_share.values()) != 1 or
            sum(roof_156kw_share.values()) != 1 or sum(roof_280kw_share.values()) != 1):
        raise ValueError(f"each technology share must sum 1")

    # open ground
    open_fleet_activity = bd.Database('additional_acts').new_activity(
        name='photovoltaic, open ground, 570 kWp, for enbios',
        code='photovoltaic, open ground, 570 kWp, for enbios',
        location='RER',
        unit='unit',
        comment=f'technology share: {open_technology_share}'
    )
    open_fleet_activity['reference product'] = 'photovoltaic, open ground, 570 kWp'
    open_fleet_activity.save()
    production_ex = open_fleet_activity.new_exchange(input=open_fleet_activity.key, type='production', amount=1)
    production_ex.save()

    open_pv = list(ws.get_many(bd.Database(db_solar_name),
                               ws.contains('name', 'photovoltaic open ground installation'),
                               ws.contains('location',
                                           'RER')))  # CdTe, CIS, micro-Si, multi-Si, single-Si (all 570 kWp)

    tech_to_activity = {tech: act for act in open_pv for tech in open_technology_share.keys() if tech in act['name']}
    for tech, share in open_technology_share.items():
        act = tech_to_activity.get(tech)
        if act:
            new_ex = open_fleet_activity.new_exchange(input=act, type='technosphere', amount=share)
            new_ex.save()

    # roof
    # slanted-roof
    roof_pv_3kw = ws.get_many(bd.Database(db_solar_name), ws.contains('name', 'photovoltaic'),
                              ws.contains('name', 'panel, mounted, on roof'),
                              ws.exclude(ws.contains('name', '1.3 MWp')),
                              ws.exclude(ws.contains('name', '93')),
                              ws.equals('location', 'CH'))
    roof_pv_93kw = ws.get_many(bd.Database(db_solar_name), ws.contains('name', 'photovoltaic'),
                               ws.contains('name', 'panel, mounted, on roof'),
                               ws.contains('name', '93 kWp'),
                               ws.equals('location', 'CH'))
    # flat roof
    roof_pv_156kw = ws.get_many(bd.Database(db_solar_name), ws.contains('name', '156'),
                                ws.contains('name', 'on roof'), ws.equals('location', 'CH'))
    roof_pv_280kw = ws.get_many(bd.Database(db_solar_name), ws.contains('name', '280'),
                                ws.contains('name', 'on roof'), ws.equals('location', 'CH'))

    # create 3 kWp, 93 kWp, 156 kWp, and 280 kWp activities
    act_3kw_fleet = bd.Database('additional_acts').new_activity(
        name='photovoltaic slanted-roof installation, 3kWp, fleet',
        code='photovoltaic slanted-roof installation, 3kWp, fleet',
        location='RER',
        unit='unit',
        comment=f'technology share: {roof_3kw_share}'
    )
    act_3kw_fleet['reference product'] = 'photovoltaic slanted-roof installation, 3kWp, fleet'
    act_3kw_fleet.save()
    act_93kw_fleet = bd.Database('additional_acts').new_activity(
        name='photovoltaic slanted-roof installation, 93kWp, fleet',
        code='photovoltaic slanted-roof installation, 93kWp, fleet',
        location='RER',
        unit='unit',
        comment=f'technology share: {roof_93kw_share}'
    )
    act_93kw_fleet['reference product'] = 'photovoltaic slanted-roof installation, 93kWp, fleet'
    act_93kw_fleet.save()
    act_156kw_fleet = bd.Database('additional_acts').new_activity(
        name='photovoltaic slanted-roof installation, 156kWp, fleet',
        code='photovoltaic slanted-roof installation, 156kWp, fleet',
        location='RER',
        unit='unit',
        comment=f'technology share: {roof_156kw_share}'
    )
    act_156kw_fleet['reference product'] = 'photovoltaic slanted-roof installation, 156kWp, fleet'
    act_156kw_fleet.save()
    act_280kw_fleet = bd.Database('additional_acts').new_activity(
        name='photovoltaic slanted-roof installation, 280kWp, fleet',
        code='photovoltaic slanted-roof installation, 280kWp, fleet',
        location='RER',
        unit='unit',
        comment=f'technology share: {roof_280kw_share}'
    )
    act_280kw_fleet['reference product'] = 'photovoltaic slanted-roof installation, 280kWp, fleet'
    act_280kw_fleet.save()

    # add inputs to 3 KWp, 93 kWp, 156 kWp, and 280 kWp activities
    tech_to_3kw_activity = {tech: act for act in roof_pv_3kw for tech in roof_3kw_share.keys() if
                            tech in act['name']}
    for tech, share in roof_3kw_share.items():
        act = tech_to_3kw_activity.get(tech)
        if act:
            new_ex = act_3kw_fleet.new_exchange(input=act, type='technosphere', amount=share)
            new_ex.save()
    tech_to_93kw_activity = {tech: act for act in roof_pv_93kw for tech in roof_93kw_share.keys() if
                             tech in act['name']}
    for tech, share in roof_93kw_share.items():
        act = tech_to_93kw_activity.get(tech)
        if act:
            new_ex = act_93kw_fleet.new_exchange(input=act, type='technosphere', amount=share)
            new_ex.save()
    tech_to_156kw_activity = {tech: act for act in roof_pv_156kw for tech in roof_156kw_share.keys() if
                              tech in act['name']}
    for tech, share in roof_156kw_share.items():
        act = tech_to_156kw_activity.get(tech)
        if act:
            new_ex = act_156kw_fleet.new_exchange(input=act, type='technosphere', amount=share)
            new_ex.save()
    tech_to_280kw_activity = {tech: act for act in roof_pv_280kw for tech in roof_280kw_share.keys() if
                              tech in act['name']}
    for tech, share in roof_280kw_share.items():
        act = tech_to_280kw_activity.get(tech)
        if act:
            new_ex = act_280kw_fleet.new_exchange(input=act, type='technosphere', amount=share)
            new_ex.save()

    # create roof activity (which contains 3kWp, 93kWp, 156kWp, and 280kWp)
    act_roof_fleet = bd.Database('additional_acts').new_activity(
        name='photovoltaic slanted-roof installation, 1MW, fleet, for enbios',
        code='photovoltaic slanted-roof installation, 1MW, fleet, for enbios',
        location='RER',
        unit='unit',
        comment=f'Equivalent to 1MW. Technology share: {roof_technology_share}. '
                f'93kWp: {roof_93kw_share}, 3kWp: {roof_3kw_share}, 156kWp: {roof_156kw_share}, '
                f'280kWp: {roof_280kw_share}'
    )
    act_roof_fleet['reference product'] = 'photovoltaic slanted-roof installation, 1MW, fleet'
    act_roof_fleet.save()
    # add inputs from 3kWp, 93kWp, 156kWp, and 280kWp fleets
    new_ex = act_roof_fleet.new_exchange(
        input=act_3kw_fleet, type='technosphere', amount=roof_technology_share['3kWp'] * 1000 / 3
    )
    new_ex.save()
    new_ex = act_roof_fleet.new_exchange(
        input=act_93kw_fleet, type='technosphere', amount=roof_technology_share['93kWp'] * 1000 / 93
    )
    new_ex.save()
    new_ex = act_roof_fleet.new_exchange(
        input=act_156kw_fleet, type='technosphere', amount=roof_technology_share['156kWp'] * 1000 / 156
    )
    new_ex.save()
    new_ex = act_roof_fleet.new_exchange(
        input=act_280kw_fleet, type='technosphere', amount=roof_technology_share['280kWp'] * 1000 / 280
    )
    new_ex.save()
    return open_fleet_activity, act_roof_fleet


# batteries
def batteries_fleet(db_batteries_name: str, scenario: Optional[Literal['cont', 'tc']],
                    technology_share: Optional[Dict[str, float]] = None):
    """
    :return: returns the appropriate battery fleet activity. Either CONT, TC or manual scenario.
    """
    if scenario == 'cont':
        battery_fleet = ws.get_one(bd.Database(db_batteries_name),
                                   ws.equals('name', 'market for battery capacity, stationary (CONT scenario)'))
    elif scenario == 'tc':
        battery_fleet = ws.get_one(bd.Database(db_batteries_name),
                                   ws.equals('name', 'market for battery capacity, stationary (TC scenario)'))
    else:
        print(f'Manual battery scenario with the following technology shares: {technology_share}')
        # Runtime check to enforce battery types as keys
        expected_keys = {'LFP', 'NMC111', 'NMC523', 'NMC622', 'NMC811', 'NMC955', 'SiB',
                         'Vanadium', 'Lead', 'Sodium-Nickel'}
        if set(technology_share.keys()) != expected_keys:
            raise ValueError(f"'technology_share' must contain exactly the keys {expected_keys}")
        if sum(technology_share.values()) != 1:
            raise ValueError(f"'technology_share' shares must sum 1")

        # create manual battery scenario
        battery_original = ws.get_one(bd.Database(db_batteries_name),
                                      ws.equals('name', 'market for battery capacity, stationary (TC scenario)'))
        battery_fleet = battery_original.copy(database='additional_acts')
        battery_fleet['name'] = 'market for battery capacity, stationary (manual scenario), for enbios'
        battery_fleet.save()

        battery_type_to_exchange = {battery_type: ex for ex in battery_fleet.technosphere() for battery_type in
                                    technology_share.keys() if battery_type in ex.input.name}

        for battery_type, share in technology_share.items():
            ex = battery_type_to_exchange.get(battery_type)
            if ex:
                ex.amount = share
                ex.save()

        # check that the total amount is 1
        if sum([ex.amount for ex in battery_fleet.technosphere()]) != 1:
            raise ValueError(f"something went wrong. The input amounts sum more than 1")

    return battery_fleet


# electrolysis
def hydrogen_from_electrolysis_market(db_hydrogen_name: str, soec_share: float, aec_share: float, pem_share: float):
    """
    ´´soec_share´´, ´´aec_share´´´and ´´pem_share´´ need to be shares between 0 and 1, summing 1 in total.
    The function creates a market activity in 'additional_acts' with the shares of hydrogen production from
    soec, aec and pem technologies named 'hydrogen production, gaseous, for enbios'.
    """
    # TODO: revise. Should it be a market for electrolysers instead o a market for hydrogen production?
    #  It depends on what is Calliope refering to. So far, I think we are doing good.
    if (soec_share + aec_share + pem_share) != 1:
        print(f'your inputs for soc ({soec_share}), aec ({aec_share}) and pem ({pem_share}) do not sum 1. '
              f'They sum {soec_share + aec_share + pem_share}. Try a combination that sums 1)')
        sys.exit()

    create_additional_acts_db()

    soec_act = ws.get_one(bd.Database(db_hydrogen_name),
                          ws.equals('name', 'hydrogen production, gaseous, 1 bar, '
                                            'from SOEC electrolysis, from grid electricity'),
                          ws.equals('location', 'CH')
                          )
    aec_act = ws.get_one(bd.Database(db_hydrogen_name),
                         ws.equals('name', 'hydrogen production, gaseous, 20 bar, '
                                           'from AEC electrolysis, from grid electricity'),
                         ws.equals('location', 'CH')
                         )
    pem_act = ws.get_one(bd.Database(db_hydrogen_name),
                         ws.equals('name', 'hydrogen production, gaseous, 30 bar, '
                                           'from PEM electrolysis, from grid electricity'),
                         ws.equals('location', 'RER')
                         )
    market_act = bd.Database('additional_acts').new_activity(name='hydrogen production, gaseous, for enbios',
                                                             code='hydrogen production, gaseous, for enbios',
                                                             location='RER',
                                                             unit='kilogram',
                                                             comment=f'aec: {aec_share * 100}%, '
                                                                     f'pem: {pem_share * 100}%,'
                                                                     f'soec: {soec_share * 100}%')
    market_act.save()
    production_exchange = market_act.new_exchange(input=market_act.key, type='production', amount=1)
    production_exchange.save()
    for act, share in {soec_act: soec_share, aec_act: aec_share, pem_act: pem_share}.items():
        new_ex = market_act.new_exchange(input=act, type='technosphere', amount=share)
        new_ex.save()


def hydrogen_relink():
    # TODO: do I need to relink the market for hydrogen???
    pass


##### substitute and unlink #####
# Note: eliminate downstream connections of all hydrogen activities (not only those from electrolysers)
# (biofuel_to_liquids).
# Note: maybe relink_technosphere_exchanges() from wurst?


##### materials as indicator #####
# 1. foreground
def foreground_materials():
    # TODO:
    # 1. CRMs; 2. list of heavily used materials: steel, iron, copper, aluminium, cement, concrete, water, plastics;
    # 3. group materials per ISIC classification.
    pass


# 2. all value chain
def solve_lci(activity):
    """
    :return: a list of tuples, where each tuple is (activity name, amount, unit, category)
    """
    lca = activity.lca(amount=1)
    lca.lci()
    array = lca.inventory.sum(axis=1)
    if hasattr(lca, 'dicts'):
        mapping = lca.dicts.biosphere
    else:
        mapping = lca.biosphere_dict
    data = []
    for key, row in mapping.items():
        data.append((bd.get_activity(key).get('name'), array[row, 0], bd.get_activity(key).get('unit'),
                     bd.get_activity(key).get('categories')))
    df = pd.DataFrame([{
        'name': name,
        'amount': amount,
        'unit': unit,
        'categories': categories,
    } for name, amount, unit, categories in data
    ])
    return df


def natural_resources(activity):
    """
    :return: filters solved activities, so it only includes the corresponding categories to natural resources
    """
    df = solve_lci(activity=activity)
    nat_res = df.loc[df['categories'].apply(lambda x: x[3] == 'natural resource')]
    return nat_res


def total_materials(activity):
    """
    :return: all material extraction amounts through the entire life-cycle (solved inventory).
    """
    df = solve_lci(activity=activity)
    total_mat = df.loc[
        df['categories'].apply(lambda x: x[3] == 'natural resource, in ground' and x[2] == 'kilogram')]
    return total_mat['amount'].sum()


##### land use as indicator #####
# 1. foreground
def foreground_land(activity):
    """
    :return: square meters of transformation, and square meters per year of occupation in the foreground
    """
    transformation = [ex.amount for ex in activity.biosphere() if 'Transformation, from' in ex._data['name']]  # in m2
    occupation = [ex.amount for ex in activity.biosphere() if 'Occupation,' in ex._data['name']]  # in m2*year
    return sum(transformation), sum(occupation)


# 2. all value chain
def total_land(activity):
    """
    :return: returns the total amount of square meters, and occupation (m2*year) of an activity through its life-cycle
    """
    nat_res = natural_resources(activity=activity)
    total_squared_meters = nat_res.loc[nat_res['name'].apply(lambda x: x[1] == 'Transformation, from')]
    total_occupation = nat_res.loc[nat_res['name'].apply(lambda x: x[1] == 'Occupation,')]
    return total_squared_meters['amount'].sum(), total_occupation['amount'].sum()


##### get the emissions of the biosphere only ####
def direct_emissions(activity, method=('EF v3.1', 'climate change', 'global warming potential (GWP100)')):
    """
    :return: gwp EF v3.1 direct emissions of an activity
    """
    direct_emissions_db = bd.Database('direct_emissions_db')
    if 'direct_emissions_db' not in bd.databases:
        direct_emissions_db.register()
    new_act = activity.copy(database='direct_emissions_db')
    new_act.technosphere().delete()
    lca = new_act.lca(amount=1, method=method)
    return lca.score


def get_co2_emissions_only(activity):
    """
    :return: CO2 emissions (not CO2-eq) in kg of an activity. Needed to get DAC carbon intake capacity.
    """
    co2 = [ex.amount for ex in activity.biosphere() if 'Carbon dioxide' in ex._data['name']]
    return sum(co2)
