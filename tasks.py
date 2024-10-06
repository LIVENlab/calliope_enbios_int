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
                                  ws.contains('location', location),
                                  ws.contains('unit', unit))
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
                                  ws.contains('name', activity_name),
                                  ws.contains('unit', unit))
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
    if len(infrastructure_acts) > 1:
        # TODO: create a new activity with the two/three of them. OR better do it manually?
        #  -> case of: 1. heat and power co-generation, wood chips; 2. chp_wte_back_pressure
        pass
    for e in infrastructure_acts:
        e.delete()

    return infrastructure_acts


##### individual changes #####
# 1. chp_wte_back_pressure: use APOS in this case (comment with Joan!). Create an activity that includes dust collector,
# electrostatic precipitator, for industrial use plus heat and
# power co-generation unit, organic Rankine cycle, 1000kW electrical. This will be the infrastructure activity.
# Remove all technosphere.
# 2. biofuel_to_methane: increase efficiency from 50% to 70% (HOW!!??!)
# 3. biofuel_to_methanol: 'methanol distillation, from wood, with CCS'. Change the input
# 'hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, with CCS, at gasification plant'
# for 'hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, at gasification plant' (WEU)
# 4. 'medium duty truck, fuel cell electric, 26t gross weight, long haul' keep 'power distribution', 'converter',
# 'inverter', 'fuel tank', 'power electronics', 'other components', 'electric motor', 'fuel cell system',
# 'electricity storage capacity'.
# 5. 'light duty truck, fuel cell electric, 3.5t gross weight, long haul', 'converter', 'inverter', 'fuel tank',
# 'power electronics', 'other components', 'electric motor', 'fuel cell system', 'electricity storage capacity'.
# 6. 'transport, passenger car, battery electric, Medium' (in km). Remove all inputs with 'glider' in the name.
# 7. 'gas-to-liquid plant construction'. Add 1250000 kg of cobalt as input (catalyst).

# TODO: biosphere 1 kg CO2 removal does not count as negative emissions. Ask Samantha how to deal with it.

##### create fleets #####
# wind_onshore
# wind_offshore
# solar_pv
# batteries
# electrolysis


##### substitute and unlink #####
# Note: eliminate downstream connections of all hydrogen activities (not only those from electrolysers
# (biofuel_to_liquids).
# Note: maybe relink_technosphere_exchanges() from wurst?


##### materials as indicator #####
# 1. foreground
def foreground_materials():
    # TODO
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
    :return: filters solved activities so it only includes the corresponding categories to natural resources
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
    :return: square meters of transformation in the foreground
    """
    biosphere_exchanges = [ex.amount for ex in activity.biosphere() if 'Transformation, from' in ex._data['name']]
    return sum(biosphere_exchanges)


# 2. all value chain
def total_land(activity):
    """
    :return: returns the total amount of square meters of an activity through its life-cycle
    """
    nat_res = natural_resources(activity=activity)
    total_squared_meters = nat_res.loc[nat_res['name'].apply(lambda x: x[1] == 'Transformation, from')]
    return total_squared_meters['amount'].sum()


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
