import bw2data as bd
import math

TECH_ACT_DICT = {
    'open_pv': 'electricity production, photovoltaic, 570kWp open ground installation, multi-Si',
    'roof_pv': 'electricity production, photovoltaic, 3kWp slanted-roof installation, single-Si, panel, mounted',
    'waste': 'electricity, from municipal waste incineration to generic market for electricity, medium voltage',
    'wind_onshore': 'electricity production, wind, 1-3MW turbine, onshore',
    'wind_offshore': 'electricity production, wind, 1-3MW turbine, offshore',
    'hydro': 'electricity production, hydro, reservoir, non-alpine region',
    'oil': 'electricity production, oil',
    'cc': 'electricity production, natural gas, combined cycle power plant',
    'gas_turbine': 'electricity production, natural gas, conventional power plant',
    'cogen': 'heat and power co-generation, natural gas, conventional power plant, 100MW electrical',
    'biomass': 'heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014',
    'coal': 'electricity production, hard coal',
    'biogas': 'heat and power co-generation, biogas, gas engine',
    'nuclear': 'electricity production, nuclear, pressure water reactor',
    'solar': 'electricity production, solar thermal parabolic trough, 50 MW',
    'geothermal': 'electricity production, deep geothermal',
    'hydro_storage': 'electricity production, hydro, pumped storage',
}

TECH_NAME_SEARCH_NAME = {
    'photovoltaic, 570kWp': 'open_pv',
    'electricity production, photovoltaic, 3kWp slanted-roof installation, single-Si, panel, mounted': 'roof_pv',
    'electricity, from municipal waste incineration to generic market for electricity, medium voltage': 'waste',
    'onshore': 'wind_onshore',
    'offshore': 'wind_offshore',
    'hydro, r': 'hydro',
    "[' oil', 'diesel']": 'oil',
    'combined cycle': 'cc',
    'electricity production, natural gas, conventional power plant': 'gas_turbine',
    'heat and power co-generation, natural gas': 'cogen',
    'wood chips': 'biomass',
    "['lignite', 'hard coal', 'peat']": 'coal',
    'heat and power co-generation, biogas': 'biogas',
    'nuclear': 'nuclear',
    'solar ': 'solar',
    'deep geothermal': 'geothermal',
    'pumped storage': 'hydro_storage',
}

REGION_GROUPS = {

    # Western Europe
    "WEU": [
        "AT", "BE", "CH", "DE", "DK", "ES", "FI", "FR", "GB", "IE",
        "IS", "IT", "LU", "MT", "NL", "NO", "PT", "SE"
    ],

    # Central Europe
    "CEU": [
        "AL", "BA", "BG", "CZ", "HR", "HU", "LT", "LV", "MD",
        "MK", "PL", "RO", "RS", "SI", "SK", "XK"
    ],

    # Eastern Europe
    "EEU": [
        "EE", "UA"
    ],

    # Former Soviet Union
    "FSU": [
        "RU", "BY", "KZ", "KG", "TJ", "TM", "UZ", "AZ", "AM", "GE"
    ],

    # USA
    "USA": [
        "US-PR", "US-NPCC", "US-WECC", "US-MRO", "US-RFC",
        "US-TRE", "US-SERC", "US-HICC", "US-ASCC"
    ],

    # Canada
    "CAN": [
        "CA-AB", "CA-BC", "CA-MB", "CA-NB", "CA-NF", "CA-NS",
        "CA-NT", "CA-NU", "CA-ON", "CA-PE", "CA-QC", "CA-SK", "CA-YK"
    ],

    # Mexico
    "MEX": [
        "MX"
    ],

    # Brazil
    "BRA": [
        "BR-Northern grid", "BR-North-eastern grid",
        "BR-South-eastern/Mid-western grid", "BR-Southern grid"
    ],

    # Rest of Central America & Caribbean
    "RCAM": [
        "CR", "CU", "DO", "GT", "HN", "HT", "JM",
        "NI", "PA", "SV", "TT", "CW"
    ],

    # Rest of South America
    "RSAM": [
        "AR", "BO", "CL", "CO", "EC", "PE", "PY", "UY", "VE"
    ],

    # North Africa
    "NAF": [
        "DZ", "EG", "LY", "MA", "TN"
    ],

    # West Africa
    "WAF": [
        "BJ", "CI", "GH", "NG", "SN", "TG"
    ],

    # East Africa
    "EAF": [
        "ET", "KE", "TZ", "UG", "RW", "BI"
    ],

    # Southern Africa
    "SAF": [
        "BW", "MZ", "NA", "ZA", "ZW", "ZM"
    ],

    # Middle East
    "ME": [
        "AE", "BH", "IL", "IQ", "IR", "JO", "KW",
        "LB", "OM", "QA", "SA", "SY", "TR", "YE", "ME"
    ],

    # China
    "CHN": [
        "CN-CSG", "CN-NWG", "CN-SWG", "CN-NECG",
        "CN-ECGC", "CN-NCGC", "CN-CCG", "HK", "TW"
    ],

    # India
    "IND": [
        "IN-Northern grid", "IN-Western grid",
        "IN-Southern grid", "IN-Eastern grid",
        "IN-North-eastern grid"
    ],

    # Japan
    "JPN": [
        "JP"
    ],

    # South Korea
    "KOR": [
        "KR"
    ],

    # Southeast Asia
    "SEAS": [
        "ID", "MY", "PH", "SG", "TH", "VN"
    ],

    # Indonesia (sometimes separated)
    "IDN": [
        "ID"
    ],

    # Rest of South Asia
    "RSAS": [
        "BD", "LK", "NP", "PK"
    ],

    # Rest of Centrally Planned Asia
    "RCAS": [
        "KP", "MN"
    ],

    # Oceania
    "OCE": [
        "AU", "NZ"
    ],
}


def _as_list_searchword(search_word: str | list[str]) -> str:
    """
    Your code sometimes passes search_word as a list (e.g. [' oil','diesel']).
    But TECH_NAME_SEARCH_NAME uses stringified lists as keys.
    This helper reproduces your current convention safely.

    If you refactor later: prefer passing a technology key directly ("oil", "coal", ...).
    """
    if isinstance(search_word, str):
        return search_word

    # If it's a list, convert to the same canonical string representation
    # as your keys like "[' oil', 'diesel']"
    return str(search_word)


def _find_activities_by_name_and_location(db_name: str, activity_name: str, location: str | None = None,
                                          unit: str | None = 'kilowatt hour'):
    db = bd.Database(db_name)
    def matches(a):
        if a["name"] != activity_name:
            return False
        if location is not None and a.get("location") != location:
            return False
        if unit is not None and a.get("unit") != unit:
            return False
        return True

    return [a for a in db if matches(a)]


def _get_region_group_countries(region: str, region_groups: dict[str, list[str]]) -> list[str] | None:
    """
    Returns the *country list* of the first group that contains `region`.
    Example: region="ES" -> returns REGION_GROUPS["RER"] list (country codes).
    """
    for countries in region_groups.values():
        if region in countries:
            return countries
    return None


def choose_alternative(
        db_name: str,
        search_word: str | list[str],
        region: str,
        verbose: bool = False,
):
    """
    Fallback order:
      1) exact region (country code)
      2) representative country of region group (first element)
      3) any other country in same group (in list order)
      4) anywhere (first match)  [warn: non-deterministic order]

    Returns:
      A single Brightway activity (bd Activity proxy)

    Raises:
      KeyError if mapping keys are missing
      ValueError if no activity exists anywhere
    """
    # 0) Resolve the technology key and target activity name
    key = _as_list_searchword(search_word)
    tech_key = TECH_NAME_SEARCH_NAME[key]  # e.g. "oil"
    activity_name = TECH_ACT_DICT[tech_key]  # e.g. "electricity production, oil"

    # 1) Exact region
    acts = _find_activities_by_name_and_location(db_name, activity_name, region)
    if verbose:
        print(f"[choose_alternative] exact region={region}: {len(acts)} matches")
    if len(acts) == 1:
        return acts[0]

    # 2) Representative country in same group
    countries = _get_region_group_countries(region, REGION_GROUPS)
    if countries:
        rep = countries[0]
        acts = _find_activities_by_name_and_location(db_name, activity_name, rep)
        if verbose:
            print(f"[choose_alternative] representative={rep}: {len(acts)} matches")
        if len(acts) == 1:
            return acts[0]

        # 3) Other countries in group
        for c in countries:
            acts = _find_activities_by_name_and_location(db_name, activity_name, c)
            if verbose:
                print(f"[choose_alternative] group country={c}: {len(acts)} matches")
            if len(acts) == 1:
                return acts[0]

    # 4) Anywhere
    acts = _find_activities_by_name_and_location(db_name, activity_name, None)
    if verbose:
        print(f"[choose_alternative] anywhere: {len(acts)} matches")
    if acts:
        if verbose:
            print(f"[choose_alternative] WARNING: falling back to first match anywhere.")
        return acts[0]

    raise ValueError(
        f"No activity found for tech_key={tech_key!r} "
        f"(activity_name={activity_name!r}) in database {db_name!r}"
    )


def calculate_shares(db_name: str, activity: bd.backends.peewee.proxies.Activity,
                     search_word: str | list[str], region: str,
                     exclude_word: str | list[str] | None = None):
    exs = list(activity.technosphere())

    def not_excluded(name: str) -> bool:
        if exclude_word is None:
            return True
        if isinstance(exclude_word, str):
            return exclude_word not in name
        return not any(w in name for w in exclude_word)

    if isinstance(search_word, str):
        tech_exs = [
            e for e in exs
            if search_word in e.input["name"]
               and not_excluded(e.input["name"])
        ]

    elif isinstance(search_word, list):
        tech_exs = [
            e for e in exs
            if any(word in e.input["name"] for word in search_word)
               and not_excluded(e.input["name"])
        ]

    if tech_exs != [] and search_word != 'imports':
        tech_total = sum(e["amount"] for e in tech_exs)
        tech_shares = {e.input: e.amount / tech_total for e in tech_exs}
    elif tech_exs == [] and search_word != 'imports':
        tech_act = choose_alternative(db_name=db_name, search_word=search_word, region=region)
        tech_shares = {tech_act: 1}
    else:
        tech_shares = {}
    return tech_shares, exs, tech_exs


def find_activity(db, name, location):
    return next(a for a in db if a['name'] == name and a['location'] == location)


def replace_shares(tech_shares, exs, new_amount, new_act):
    if new_act != []:
        for ex in exs:
            if ex.input in tech_shares:
                ex._data['amount'] = new_amount * tech_shares[
                    ex.input]
                ex.save()
    else:
        electricity_mix_act = exs[0].output
        ex = electricity_mix_act.new_exchange(input=list(tech_shares.keys())[0], amount=new_amount, type='technosphere')
        ex.save()
        electricity_mix_act.save()


def adapt_electricity_mix(ecoinvent_database_name: str,
                          open_pv_share: float, roof_pv_share: float,
                          waste_share: float,
                          wind_onshore_share: float, wind_offshore_share: float,
                          hydro_share: float,
                          oil_share: float,
                          cc_share: float,
                          gas_turbine_share: float,
                          cogen_share: float,
                          biomass_share: float,
                          coal_share: float,
                          biogas_share: float,
                          nuclear_share: float,
                          solar_share: float,
                          geothermal_share: float,
                          hydro_storage_share: float,
                          imports_share: float,
                          country_location_code: str,
                          implement_tests: bool = False):
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
    # check that inputs amount 1
    total_share = (
            open_pv_share
            + roof_pv_share
            + wind_onshore_share
            + wind_offshore_share
            + waste_share
            + hydro_share
            + oil_share
            + cc_share
            + gas_turbine_share
            + cogen_share
            + nuclear_share
            + solar_share
            + geothermal_share
            + biomass_share
            + coal_share
            + biogas_share
            + hydro_storage_share
            + imports_share
    )

    if not abs(total_share - 1.0) < 1e-6:
        print(f"⚠️ Shares do not sum to 1 (sum = {total_share:.6f})")

    # -- Tier 0 --
    low_voltage_act = find_activity(
        bd.Database(ecoinvent_database_name), 'market for electricity, low voltage', country_location_code
    )

    # -- Rescale photovoltaic shares --
    pv_open_shares, exs, tech_exs = calculate_shares(activity=low_voltage_act, search_word='photovoltaic, 570kWp',
                                                     region=country_location_code, db_name=ecoinvent_database_name)
    replace_shares(pv_open_shares, exs, open_pv_share, tech_exs)

    pv_roof_shares, exs, tech_exs = calculate_shares(activity=low_voltage_act, search_word='photovoltaic, 3kWp',
                                                     region=country_location_code, db_name=ecoinvent_database_name)
    replace_shares(pv_roof_shares, exs, roof_pv_share, tech_exs)

    for ex in exs:
        if ex.input['name'] == 'electricity voltage transformation from medium to low voltage':
            transformation_med_to_low = ex.input
            ex._data['amount'] = 1 - (open_pv_share + roof_pv_share)
        ex.save()

    # -- Tier 1: medium-voltage activities from transformation_med_to_low --
    for e in transformation_med_to_low.technosphere():
        medium_voltage = e.input

        # -- Tier 2: define waste and transformation shares --
        waste_name = 'electricity, from municipal waste incineration to generic market for electricity, medium voltage'
        target_waste_amount = waste_share / (1 - (open_pv_share + roof_pv_share))

        shares, exs, tech_exs = calculate_shares(
            db_name=ecoinvent_database_name,
            activity=medium_voltage,
            search_word=waste_name,
            region=country_location_code,
        )
        replace_shares(shares, exs, target_waste_amount, tech_exs)

        # then only handle the HV->MV transformation amount
        for me in medium_voltage.technosphere():
            if me.input['name'] == 'electricity voltage transformation from high to medium voltage':
                transformation_high_to_med = me.input
                me._data['amount'] = 1 - target_waste_amount
                me.save()


    # -- Tier 3: high-voltage activities from transformation_high_to_med --
    for e in transformation_high_to_med.technosphere():
        high_voltage = e.input

        # -- Tier 4: changes in high-voltage --
        # wind onshore
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='onshore',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, wind_onshore_share, tech_exs)

        # wind offshore
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='offshore',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, wind_offshore_share, tech_exs)

        # hydro
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='hydro, r',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, hydro_share, tech_exs)

        # oil
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word=[' oil', 'diesel'],
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, oil_share, tech_exs)

        # nuclear
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='nuclear',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, nuclear_share, tech_exs)

        # coal
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word=['lignite', 'hard coal', 'peat'],
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, coal_share, tech_exs)

        # gas turbine
        shares, exs, tech_exs = calculate_shares(activity=high_voltage,
                                                 search_word='electricity production, natural gas, conventional power plant',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, gas_turbine_share, tech_exs)

        # combined cycle
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='combined cycle',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, cc_share, tech_exs)

        # co-generation
        shares, exs, tech_exs = calculate_shares(activity=high_voltage,
                                                 search_word='heat and power co-generation, natural gas',
                                                 region=country_location_code, db_name=ecoinvent_database_name,
                                                 exclude_word='combined cycle')
        replace_shares(shares, exs, cogen_share, tech_exs)

        # biomass
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='wood chips',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, biomass_share, tech_exs)

        # biogas
        shares, exs, tech_exs = calculate_shares(activity=high_voltage,
                                                 search_word='heat and power co-generation, biogas',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, biogas_share, tech_exs)

        # solar
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='solar ',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, solar_share, tech_exs)

        # geothermal
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='deep geothermal',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, geothermal_share, tech_exs)

        # pumped storage
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='pumped storage',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        replace_shares(shares, exs, hydro_storage_share, tech_exs)

        # imports
        shares, exs, tech_exs = calculate_shares(activity=high_voltage, search_word='import',
                                                 region=country_location_code, db_name=ecoinvent_database_name)
        if shares != {}:
            replace_shares(shares, exs, imports_share, tech_exs)

        # remove industrial residual electricity production acts
        other_deletable_exs = [e for e in high_voltage.technosphere() if
                               e.input['name']
                               in ['treatment of blast furnace gas, in power plant',
                                   'treatment of coal gas, in power plant',
                                   'treatment of bagasse, from sugarcane, in heat and power co-generation unit, 6400kW thermal',
                                   'sulfate pulp production, from eucalyptus, bleached',
                                   'sugarcane processing, modern autonomous plant',
                                   'sugarcane processing, modern annexed plant'
                                   ]]
        for e in other_deletable_exs:
            e.delete()

        if implement_tests:
            print('STARTING TESTS')
            low_voltage_test = find_activity(
                bd.Database(ecoinvent_database_name), 'market for electricity, low voltage', country_location_code
            )
            lv_open_pv = _sum_amounts(low_voltage_test, 'photovoltaic, 570kWp')
            lv_roof_pv = _sum_amounts(low_voltage_test, 'photovoltaic, 3kWp')
            _assert_close(lv_open_pv, open_pv_share, "LV: open PV total amount")
            _assert_close(lv_roof_pv, roof_pv_share, "LV: roof PV total amount")

            # Transformation MV->LV should be 1 - PV total (your code sets it that way)
            lv_transf_amt = 0.0
            found = False
            for ex in low_voltage_test.technosphere():
                if ex.input['name'] == 'electricity voltage transformation from medium to low voltage':
                    lv_transf_amt = float(ex.amount)
                    found = True
                    break
            _assert_true(found, "LV: missing exchange 'electricity voltage transformation from medium to low voltage'")
            _assert_close(lv_transf_amt, 1.0 - (open_pv_share + roof_pv_share), "LV: MV->LV transformation amount")

            # (B) Test medium voltage waste amount matches your internal rescaling logic
            # Expected: waste_share / (1 - PV_total)
            mv_expected_waste = waste_share / (1.0 - (open_pv_share + roof_pv_share))

            mv_waste = _sum_amounts(
                medium_voltage,
                'electricity, from municipal waste incineration to generic market for electricity, medium voltage'
            )
            _assert_close(mv_waste, mv_expected_waste, "MV: waste amount (rescaled)")

            mv_transf_amt = 0.0
            found = False
            for ex in medium_voltage.technosphere():
                if ex.input['name'] == 'electricity voltage transformation from high to medium voltage':
                    mv_transf_amt = float(ex.amount)
                    found = True
                    break
            _assert_true(found, "MV: missing exchange 'electricity voltage transformation from high to medium voltage'")
            _assert_close(mv_transf_amt, 1.0 - mv_expected_waste, "MV: HV->MV transformation amount")

            # (C) Test HIGH VOLTAGE technology amounts match function inputs
            # IMPORTANT: this test is executed BEFORE the rescale-to-1-kWh block below.
            _assert_close(_sum_amounts(high_voltage, 'onshore'), wind_onshore_share, "HV pre-rescale: wind onshore total")
            _assert_close(_sum_amounts(high_voltage, 'offshore'), wind_offshore_share,
                          "HV pre-rescale: wind offshore total")
            _assert_close(_sum_amounts(high_voltage, 'hydro, r'), hydro_share, "HV pre-rescale: hydro total")
            _assert_close(_sum_amounts(high_voltage, [' oil', 'diesel']), oil_share, "HV pre-rescale: oil total")
            _assert_close(_sum_amounts(high_voltage, 'nuclear'), nuclear_share, "HV pre-rescale: nuclear total")
            _assert_close(_sum_amounts(high_voltage, ['lignite', 'hard coal', 'peat']), coal_share,
                          "HV pre-rescale: coal total")
            _assert_close(_sum_amounts(high_voltage, 'wood chips'), biomass_share, "HV pre-rescale: biomass total")
            _assert_close(_sum_amounts(high_voltage, 'heat and power co-generation, biogas'), biogas_share,
                          "HV pre-rescale: biogas total")
            _assert_close(_sum_amounts(high_voltage, 'solar '), solar_share, "HV pre-rescale: solar total")
            _assert_close(_sum_amounts(high_voltage, 'deep geothermal'), geothermal_share,
                          "HV pre-rescale: geothermal total")
            _assert_close(_sum_amounts(high_voltage, 'pumped storage'), hydro_storage_share,
                          "HV pre-rescale: pumped storage total")

            _assert_close(_sum_amounts(high_voltage, 'combined cycle'), cc_share, "HV pre-rescale: combined cycle total")
            _assert_close(
                _sum_amounts(high_voltage, 'electricity production, natural gas, conventional power plant'),
                gas_turbine_share,
                "HV pre-rescale: gas conventional plant total",
            )
            _assert_close(
                _sum_amounts(high_voltage, 'heat and power co-generation, natural gas', exclude_word='combined cycle'),
                cogen_share,
                "HV pre-rescale: cogen (excluding combined cycle) total",
            )

            # Imports: your logic *does not create* an import exchange if none existed originally
            # (calculate_shares returns {} for imports only in the 'imports' special case, but you call 'import'). :contentReference[oaicite:3]{index=3}
            hv_imports = _sum_amounts(high_voltage, 'import', unit='kilowatt hour')
            if hv_imports > 0:
                _assert_close(hv_imports, imports_share, "HV pre-rescale: imports total")

            # (D) Save the pre-rescale total of tech kWh inputs (excluding the market itself), then rescale, then test sum==1
            hv_kwh_total_before = sum(
                float(e.amount) for e in high_voltage.technosphere()
                if e.unit == 'kilowatt hour' and 'market for electricity, high voltage' not in e.input['name']
            )
            _assert_true(hv_kwh_total_before > 0, "HV: pre-rescale total kWh inputs (excluding HV market) must be > 0")
            print('TESTS SUCCESFULLY COMPLETED!')

        # rescale inputs so the output is 1 kWh
        high_voltage_techs = [e for e in high_voltage.technosphere() if e.unit == 'kilowatt hour'
                              and 'market for electricity, high voltage' not in e.input['name']]
        high_voltage_techs_amounts = sum([e.amount for e in high_voltage.technosphere() if e.unit == 'kilowatt hour'
                                          and 'market for electricity, high voltage' not in e.input['name']])
        for e in high_voltage_techs:
            e['amount'] = e['amount'] / high_voltage_techs_amounts
            e.save()

        # Post-rescale test: all technologies sum to 1 kWh (excluding 'market for electricity, high voltage')
        hv_kwh_total_after = sum(
            float(e.amount) for e in high_voltage.technosphere()
            if e.unit == 'kilowatt hour' and 'market for electricity, high voltage' not in e.input['name']
        )
        _assert_close(hv_kwh_total_after, 1.0, "HV post-rescale: total kWh inputs excluding HV market == 1")


# -----------------------------
# Helpers for tests
# -----------------------------
def _matches_search_word(name: str, search_word: str | list[str]) -> bool:
    if isinstance(search_word, str):
        return search_word in name
    return any(w in name for w in search_word)


def _not_excluded(name: str, exclude_word: str | list[str] | None) -> bool:
    if exclude_word is None:
        return True
    if isinstance(exclude_word, str):
        return exclude_word not in name
    return not any(w in name for w in exclude_word)


def _sum_amounts(activity, search_word: str | list[str], exclude_word: str | list[str] | None = None,
                 unit: str | None = "kilowatt hour") -> float:
    """
    Sum technosphere exchange amounts whose input['name'] matches search_word
    (string contains, or any of list entries), with optional exclude_word.
    This mirrors your calculate_shares selection logic, but sums amounts. :contentReference[oaicite:2]{index=2}
    """
    total = 0.0
    for e in activity.technosphere():
        if unit is not None and getattr(e, "unit", None) != unit:
            continue
        in_name = e.input["name"]
        if _matches_search_word(in_name, search_word) and _not_excluded(in_name, exclude_word):
            total += float(e.amount)
    return total


def _assert_close(actual: float, expected: float, label: str, tol: float = 1e-9):
    if not (math.isfinite(actual) and math.isfinite(expected)):
        raise AssertionError(f"[TEST FAIL] {label}: non-finite actual/expected (actual={actual}, expected={expected})")
    if abs(actual - expected) > tol:
        raise AssertionError(f"[TEST FAIL] {label}: expected {expected}, got {actual} (diff={abs(actual - expected)})")


def _assert_true(cond: bool, label: str):
    if not cond:
        raise AssertionError(f"[TEST FAIL] {label}")


bd.projects.set_current('fossil_free_ecoinvent')
adapt_electricity_mix(ecoinvent_database_name='ei391-testss',
                      open_pv_share=0.2, roof_pv_share=0.15, waste_share=0.01,
                      wind_onshore_share=0.2, wind_offshore_share=0.05,
                      hydro_share=0.15,
                      oil_share=0.01,
                      cc_share=0.01,
                      gas_turbine_share=0.01,
                      cogen_share=0.02,
                      biomass_share=0.05,
                      coal_share=0.01,
                      biogas_share=0.02,
                      nuclear_share=0.01,
                      solar_share=0.02,
                      geothermal_share=0.01,
                      hydro_storage_share=0.02,
                      imports_share=0.05,
                      country_location_code='FR')
