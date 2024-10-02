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
def eliminate_infrastructure(electricity_act):
    pass

##### individual changes #####


##### create fleets #####


##### substitute and unlink #####
# Note: maybe relink_technosphere_exchanges() from wurst?


##### metals as indicator #####
# 1. foreground
# 2. all value chain


##### land use as indicator #####
# 1. foreground
# 2. all value chain