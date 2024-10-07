from typing import Literal, Optional, Dict
import pandas as pd
from premise.geomap import Geomap
import wurst.searching as ws
import sys
import bw2data as bd

##### assign location #####

LOCATION_EQUIVALENCE = {
    'ALB': 'AL', 'FRA': 'FR', 'SWE': 'SE', 'DNK': 'DK', 'POL': 'PL', 'IRL': 'IE', 'EST': 'EE', 'HRV': 'HR', 'PRT': 'PT',
    'BIH': 'BA', 'LVA': 'LV', 'SVN': 'SI', 'AUT': 'AT', 'GBR': 'GB', 'DEU': 'DE', 'MNE': 'ME', 'NOR': 'NO', 'BGR': 'BG',
    'NLD': 'NL', 'HUN': 'HU', 'BEL': 'BE', 'CHE': 'CH', 'CZE': 'CZ', 'ROU': 'RO', 'CYP': 'CY', 'ESP': 'ES', 'GRC': 'GR',
    'MKD': 'MK', 'ISL': 'IS', 'ITA': 'IT', 'LTU': 'LT', 'FIN': 'FI', 'SVK': 'SK', 'SRB': 'RS', 'LUX': 'LU'
}


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
    location_alpha = LOCATION_EQUIVALENCE[calliope_location]
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
    european_countries = list(LOCATION_EQUIVALENCE.values())
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
    if 'additional_acts_db' not in bd.databases:
        ea_db = bd.Database('additional_acts_db')
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
        waste_original = ws.get_one(bd.Database(db_waste_name),
                                    ws.equals('name',
                                              'treatment of municipal solid waste, incineration'),
                                    ws.equals('location', location),
                                    ws.contains('reference product', 'electricity')
                                    )
        waste_act = waste_original.copy(database='additional_acts_db')
        waste_act.technosphere().delete()

    # create municipal solid waste incinerator
    incinerator_parts = ['furnace, wood chips, with silo, 5000kW',
                         'heat and power co-generation unit, organic Rankine cycle, 1000kW electrical',
                         'dust collector, electrostatic precipitator, for industrial use']
    new_act = bd.Database('additional_acts_db').new_activity(name='municipal solid waste incinerator',
                                                             code='municipal solid waste incinerator',
                                                             location='RER',
                                                             unit='unit')
    new_act.save()
    production_ex = new_act.new_exchange(input=new_act.key, type='production', amount=1)
    production_ex.save()
    for part in incinerator_parts:
        acts = ws.get_many(bd.Database(db_original_name), ws.contains('name', part),
                           ws.exclude(ws.contains('name', 'market')))
        part_act = ws.best_geo_match(acts, ['CH'])
        new_ex = new_act.new_exchange(input=part_act, type='technosphere', amount=1)
        new_ex.save()


def biofuel_to_methanol_update(db_methanol_name: str):
    """
    In the background of the methanol production, the function substitutes H2 production with CCS
    for H2 production without CCS.
    """
    # Change name of the methanol distillation activity
    methanol_distillation_original = ws.get_one(bd.Database(db_methanol_name),
                                                ws.equals('name',
                                                          'methanol distillation, from wood, with CCS'),
                                                )
    create_additional_acts_db()
    methanol_distillation_act = methanol_distillation_original.copy(database='additional_acts_db')
    methanol_distillation_act['name'] = 'methanol distillation, from wood, without CCS'
    methanol_distillation_act.save()
    # Change name of the methanol synthesis activity
    methanol_synthesis_original = [ex.input for ex in methanol_distillation_act.technosphere() if
                                   'methanol synthesis, from wood, with CCS' in ex.input._data['name']][0]
    methanol_synthesis_act = methanol_synthesis_original.copy(database='additional_acts_db')
    methanol_synthesis_act['name'] = 'methanol synthesis, from wood, without CCS'
    methanol_synthesis_act.save()
    # Re-link methanol synthesis act to methanol distillation act
    methanol_synthesis_input_to_distillation = [ex for ex in methanol_distillation_act.technosphere() if
                                                'methanol synthesis, from wood, with CCS' in ex.input._data['name']][0]
    methanol_synthesis_input_to_distillation.input = methanol_synthesis_act.key
    methanol_synthesis_input_to_distillation.save()
    # Substitute CCS for gasification withot CCS
    h2_exchange = [ex for ex in methanol_distillation_act.technosphere() if
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
    light_truck_act = light_truck_original.copy(database='additional_acts_db')
    light_truck_technosphere = list(light_truck_act.technosphere())
    keep_inputs = ['converter', 'inverter', 'fuel tank', 'power electronics', 'other components', 'electric motor',
                   'fuel cell system', 'electricity storage capacity']
    for ex in light_truck_technosphere:
        if not any(input_name in ex.input.name for input_name in keep_inputs):
            ex.delete()

    # medium truck
    medium_truck_original = ws.get_one(bd.Database(db_truck_name),
                                       ws.equals('name',
                                                 'medium duty truck, fuel cell electric, 26t gross weight, '
                                                 'long haul'),
                                       )
    medium_truck_act = medium_truck_original.copy(database='additional_acts_db')
    medium_truck_technosphere = list(medium_truck_act.technosphere())
    keep_inputs = ['power distribution', 'converter', 'inverter', 'fuel tank', 'power electronics', 'other components',
                   'electric motor', 'fuel cell system', 'electricity storage capacity']
    for ex in medium_truck_technosphere:
        if not any(input_name in ex.input.name for input_name in keep_inputs):
            ex.delete()


def passenger_car_update(db_passenger_name: str):
    """
    Creates a copy of 'transport, passenger car, battery electric, Medium' in the database 'additional_acts_db' and
    deletes glider inputs.
    """
    car_original = ws.get_one(bd.Database(db_passenger_name),
                              ws.equals('name', 'transport, passenger car, battery electric, Medium'),
                              )
    create_additional_acts_db()
    car_act = car_original.copy(database='additional_acts_db')
    car_technosphere = list(car_act.technosphere())
    for ex in car_technosphere:
        if 'glider' in ex.input.name:
            ex.delete()


def gas_to_liquid_update(db_cobalt_name: str, db_gas_to_liquid_name: str):
    """
    Creates a copy of 'gas-to-liquid plant construction' in 'additional_acts_db' and adds 1250000 kg of cobalt
    as input (catalyst).
    """
    gas_to_liquid_original_act = ws.get_one(bd.Database(db_gas_to_liquid_name),
                                            ws.equals('name', 'gas-to-liquid plant construction'),
                                            )
    create_additional_acts_db()
    gas_to_liquid_act = gas_to_liquid_original_act.copy(database='additional_acts_db')
    cobalt_act = list(ws.get_many(bd.Database(db_cobalt_name), ws.equals('name', 'market for cobalt'),
                                  ws.equals('reference product', 'cobalt'),
                                  ))[0]
    production_ex = gas_to_liquid_act.new_exchange(input=gas_to_liquid_act.key, type='production', amount=1)
    production_ex.save()
    new_ex = gas_to_liquid_act.new_exchange(input=cobalt_act, type='technosphere', amount=1250000)
    new_ex.save()


# TODO: biosphere 1 kg CO2 removal does not count as negative emissions. Ask Samantha how to deal with it.

##### create fleets #####
# wind_onshore
# TODO: have windtrace code as a package in the project
# wind_offshore
# solar_pv

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
        battery_fleet = battery_original.copy(database='additional_acts_db')
        battery_fleet['name'] = 'market for battery capacity, stationary (manual scenario), for enbios'
        battery_fleet.save()

        for battery_type, share in technology_share.items():
            for ex in battery_fleet.technosphere():
                if battery_type in ex.input.name:
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
    market_act = bd.Database('additional_acts_db').new_activity(name='hydrogen production, gaseous, for enbios',
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
