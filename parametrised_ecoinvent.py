from functions import biomass_update
import bw2data as bd
import uuid
from typing import Literal

EUROPE_COUNTRIES = [
    "AT", "BE", "BG", "CH", "CY", "CZ", "DE", "DK", "EE", "ES",
    "FI", "FR", "GB", "GR", "HR", "HU", "IE", "IS", "IT", "LT",
    "LU", "LV", "MT", "NL", "NO", "PL", "PT", "RO", "SE", "SI",
    "SK"
]

EUROPE_REGIONS = [
    "RER",
    "Europe without Switzerland",
    "Europe, without Russia and Turkey",
    "Europe without Austria",
    "Europe without Switzerland and Austria",
    "ENTSO-E",
    "UCTE"
]

EUROPEAN_LOCATIONS = EUROPE_COUNTRIES + EUROPE_REGIONS


def calculate_shares(activity, search_word):
    exs = list(activity.technosphere())

    if isinstance(search_word, str):
        tech_exs = [e for e in exs if search_word in e.input['name']]
    elif isinstance(search_word, list):
        tech_exs = [e for e in exs if any(word in e.input['name'] for word in search_word)]
    else:
        raise ValueError("search_word must be a string or list of strings")

    tech_total = sum(e.amount for e in tech_exs)
    try:
        tech_shares = {e.input: e.amount / tech_total for e in tech_exs}
    except ZeroDivisionError:
        tech_shares = {}
        print(f"Amount was 0 for {search_word}")
    return tech_shares, exs


def find_activity(db, name, location):
    return next(a for a in db if a['name'] == name and a['location'] == location)

def replace_shares(tech_shares, exs, new_amount):
    for ex in exs:
        if ex.input in tech_shares:
            ex._data['amount'] = new_amount * tech_shares[
                ex.input]
            ex.save()


def adapt_electricity_mix(ecoinvent_database_name: str, pv_share, n_waste_share, r_waste_share,
                          hydro_share, hydroeolic_share,
                          nuclear_share, coal_share,
                          fuel_gas_share, diesel_motor_share, gas_turbine_share, vapour_turbine_share, cc_share,
                          wind_share, solar_share,
                          cogen_share, other_renewables_share, country_location_code: str = 'ES'):
    """
        The electricity mix of a country in Ecoinvent is nested into voltage levels. From high to low voltage:
        1. 'market for electricity, high voltage' contains:
            1.1. Electricity production activities from all technologies operating in high voltage (e.g., coal, hydro...)
            1.2. Imports from other countries
            1.3. Residual electricity from industries (e.g., treatment of blast furnace gas)
            1.4. Itself (transmission losses)
            1.5. Transmission networks (high voltage)
        2. 'market for electricity, medium voltage' contains:
            2.1. Electricity production from municipal waste incineration
            2.2. Electricity voltage transformation (from high to medium. Its amount is less than 1 due to losses)
            2.3. Itself (transmission losses)
            2.4. Transmission networks (medium voltage)
            2.5. Sulfur hexafluoride (fugitive emissions during transformation)
        3. 'market for electricity, low voltage' contains:
            3.1. Electricity production from photovoltaics (open-ground and rooftop)
            3.2. Electricity voltage transformation (from medium to low)
            3.3. Itself (transmission losses)
            3.4. Distribution networks (low voltage)
            3.5. Sulfur hexafluoride (fugitive emissions during transformation)
        This function creates a regional market given the electricity mix of this region, departing from the country where
        this region is circumscribed and following the above-mentioned inventories structure. The procedure is the following:
        1. Find the activity 'market for electricity, low voltage' of the country.
        2. Take all photovoltaic inputs and calculate their relative share.
        3. Substitute these activities amounts for the real amount in the region.
        4. Substitute the waste incineration amount for the real amount in the region.
           (NOTE: because it is a cutoff approach, it assigns NO burden to it).
        5. Substitute each technology amount for the real amount in the region. Some are summed as defined in the dictionary
           variable technology_share.
        6. Add co-generation if it is not included initially.
        7. Remove electricity from industry
        8. IMPORTANT!: all inputs are rescaled to comply with mass balances!
        """
    # -- Tier 0 --
    low_voltage_act = find_activity(
        bd.Database(ecoinvent_database_name), 'market for electricity, low voltage', country_location_code
    )

    # -- Rescale photovoltaic shares --
    photovoltaic_shares, exs = calculate_shares(low_voltage_act, 'photovoltaic')

    for ex in exs:
        if ex.input in photovoltaic_shares:
            ex._data['amount'] = pv_share * photovoltaic_shares[ex.input]
        elif ex.input['name'] == 'electricity voltage transformation from medium to low voltage':
            transformation_med_to_low = ex.input
            ex._data['amount'] = 1 - pv_share
        ex.save()

    # -- Tier 1: medium-voltage activities from transformation_med_to_low --
    for e in transformation_med_to_low.technosphere():
        medium_voltage = e.input

        # -- Tier 2: define waste and transformation shares --
        for me in medium_voltage.technosphere():
            waste_share = n_waste_share + r_waste_share
            if me.input['name'] == 'electricity, from municipal waste incineration to generic market for electricity, medium voltage':
                me._data['amount'] = waste_share / (1 - pv_share)  # rescaling to have a mass balance
            elif me.input['name'] == 'electricity voltage transformation from high to medium voltage':
                transformation_high_to_med = me.input
                me._data['amount'] = 1 - waste_share / (1 - pv_share)
            me.save()

    # -- Tier 3: high-voltage activities from transformation_high_to_med --
    for e in transformation_high_to_med.technosphere():
        high_voltage = e.input

        # -- Tier 4: changes in high-voltage --
        # hydro
        new_amount = hydro_share + hydroeolic_share

        shares, exs = calculate_shares(high_voltage, 'hydro')
        replace_shares(shares, exs, new_amount)

        # nuclear
        shares, exs = calculate_shares(high_voltage, 'nuclear')
        replace_shares(shares, exs, nuclear_share)

        # coal
        shares, exs = calculate_shares(high_voltage, ['lignite', 'hard coal'])
        replace_shares(shares, exs, coal_share)

        # fuel_gas
        new_amount = fuel_gas_share + diesel_motor_share
        shares, exs = calculate_shares(high_voltage, ' oil')
        replace_shares(shares, exs, new_amount)

        # gas turbine
        new_amount = gas_turbine_share + vapour_turbine_share
        shares, exs = calculate_shares(high_voltage, 'electricity production, natural gas, conventional power plant')
        replace_shares(shares, exs, new_amount)

        # combined cycle
        shares, exs = calculate_shares(high_voltage, 'combined cycle')
        replace_shares(shares, exs, cc_share)

        # wind
        shares, exs = calculate_shares(high_voltage, 'wind')
        replace_shares(shares, exs, wind_share)

        # solar
        shares, exs = calculate_shares(high_voltage, 'solar')
        replace_shares(shares, exs, solar_share)

        # other renewables
        shares, exs = calculate_shares(high_voltage, ["biogas", 'wood chips'])
        replace_shares(shares, exs, other_renewables_share)

        # co-gen. This one is not there, should be added
        if 'heat and power co-generation, natural gas, conventional power plant, 100MW electrical' not in [exchange.input['name'] for exchange in high_voltage.technosphere()]:
            try:
                cogen_act = [a for a in bd.Database(ecoinvent_database_name) if
                       a['name'] == 'heat and power co-generation, natural gas, conventional power plant, 100MW electrical' and
                       a['location'] == country_location_code and
                       a['reference product'] == 'electricity, high voltage'][0]
            except Exception as e:
                cogen_act = [a for a in bd.Database(ecoinvent_database_name) if
                             a[
                                 'name'] == 'heat and power co-generation, natural gas, conventional power plant, 100MW electrical' and
                             a['location'] == 'DE' and
                             a['reference product'] == 'electricity, high voltage'][0]
            new_ex = high_voltage.new_exchange(input=cogen_act, amount=cogen_share, unit='kilowatt hour', type='technosphere')
            new_ex.save()
        else:
            # other renewables
            shares, exs = calculate_shares(high_voltage, "heat and power co-generation, natural gas, conventional power plant")
            replace_shares(shares, exs, other_renewables_share)

        # remove treatment of coal gas and treatment of blast furnace gas
        other_deletable_exs = [e for e in high_voltage.technosphere() if
                               e.input['name'] == 'treatment of blast furnace gas, in power plant' or
                               e.input['name'] == 'treatment of coal gas, in power plant']
        for e in other_deletable_exs:
            e.delete()

        # rescale inputs so the output is 1 kWh
        high_voltage_techs = [e for e in high_voltage.technosphere() if e.unit == 'kilowatt hour'
                              and 'market for electricity, high voltage' not in e.input['name']]
        high_voltage_techs_amounts = sum([e.amount for e in high_voltage.technosphere() if e.unit == 'kilowatt hour'
                                          and 'market for electricity, high voltage' not in e.input['name']])
        for e in high_voltage_techs:
            e['amount'] = e['amount'] / high_voltage_techs_amounts
            e.save()



def substitute_fossil_sources(
        database_name: str,
        biomass_from_residues: float, biomass_location: Literal["Europe", "RoW"],
        ccs_gasification: float,
        diesel_syn: float,
        diesel_bio: float,
        syn_diesel_hydrogen: float,
        europe_only: bool = False,  # if False, the whole world
):
    """
    For each carrier:
        1. create new activities (if needed) for the synthetic and biomass alternatives
        2. create a market mix with the fossil, synthetic and biomass mix
        3. substitute current fossils for this mix
    """
    # BIOMASS
    # 1. and 2. (create new activity and market mix)
    biomass_act = biomass_update(residues_share=biomass_from_residues)  # RER

    # 3. substitute new biomass for wood chips
    list_of_products = ['heat,', 'electricity,', 'biomethane,', 'ethanol,',
                        'hydrogen,', 'methanol,', 'syngas,', 'synthetic gas,']
    if biomass_location == 'Europe':
        wood_chips_acts = [a for a in bd.Database(database_name) if
                           a['name'] == 'market for wood chips, wet, measured as dry mass' and
                           a['location'] in EUROPEAN_LOCATIONS]
    else:
        wood_chips_acts = [a for a in bd.Database(database_name) if
                           a['name'] == 'market for wood chips, wet, measured as dry mass' and
                           a['location'] not in EUROPEAN_LOCATIONS]
    for act in wood_chips_acts:
        for ex in act.upstream():
            if any(product in ex['reference product'] for product in list_of_products):
                ex.input = biomass_act
                ex.save()



    # HYDROGEN
    # 1. find alternative activities
    # wood gasification
    hydrogen_wood_gasification = [
        a for a in bd.Database(database_name) if
        a['name'] == 'hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier']
    # create CCS mix activity. It will have as technosphere inputs the CCS and non-CCS techs.
    hydrogen_wood_new = bd.Database(database_name).new_activity(
        code=uuid.uuid4().hex,
        name='hydrogen production, from wood gasification, CCS mix',
        location='RER',
        unit='kilogram',
        type='process'
    )
    hydrogen_wood_new.save()
    ex = hydrogen_wood_new.new_exchange(type='production', input=hydrogen_wood_new.key, amount=1)
    ex.save()
    hydrogen_wood_new.save()
    for act in hydrogen_wood_gasification:
        if 'CCS' in act['name']:
            amount = ccs_gasification
        else:
            amount = 1 - ccs_gasification
        ex = hydrogen_wood_new.new_exchange(
            input=act,
            type='technosphere',
            amount=amount,
        )
        ex['reference product'] = 'hydrogen, gaseous, low pressure'
        ex.save()

    # electrolysis (PEM)
    hydrogen_electrolysis = [a for a in bd.Database(database_name) if
                             a['name'] == 'hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity']  # RER

    # fossil: steam reforming
    hydrogen_fossil_steam = [a for a in bd.Database(database_name) if
                             a['name'] == 'hydrogen production, steam methane reforming']  # RER

    # fossil: auto-thermal reforming
    hydrogen_fossil_autothermal = [a for a in bd.Database(database_name) if
                                   a['name'] == 'hydrogen production, auto-thermal reforming']  # RER

    # fossil: coal gasification
    hydrogen_fossil_coal = [a for a in bd.Database(database_name) if
                            'hydrogen production, coal gasification' in a['name']]  # RoW
    # create CCS mix activity. It will have as technosphere inputs the CCS and non-CCS techs.
    hydrogen_fosil_coal_new = bd.Database(database_name).new_activity(
        code=uuid.uuid4().hex,
        name='hydrogen production, coal gasification, CCS mix',
        location='RER',
        unit='kilogram',
        type='process'
    )
    hydrogen_fosil_coal_new.save()
    ex = hydrogen_fosil_coal_new.new_exchange(type='production', input=hydrogen_fosil_coal_new.key, amount=1)
    ex.save()
    hydrogen_fosil_coal_new.save()
    for act in hydrogen_fossil_coal:
        if 'CCS' in act['name']:
            amount = ccs_gasification
        else:
            amount = 1 - ccs_gasification
        ex = hydrogen_fosil_coal_new.new_exchange(
            input=act,
            type='technosphere',
            amount=amount,
        )
        ex['reference product'] = 'hydrogen, gaseous, low pressure'
        ex.save()
