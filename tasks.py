from typing import Literal, Optional, Dict, Any, List, Union
import pandas as pd
from premise.geomap import Geomap
import wurst.searching as ws
import sys
import bw2data as bd
import WindTrace.WindTrace_onshore
import WindTrace.WindTrace_offshore
import consts
import wurst
from collections import defaultdict


def unlink_electricity(db_name: str = 'premise_base'):
    market_group_locations = ['ENSTO-E', 'UCTE', 'Europe without Switzerland', 'RER']
    for location in market_group_locations:
        # get the market groups for electricity high voltage, medium voltage, low voltage.
        market_groups = ws.get_many(
            bd.Database(db_name),
            ws.contains('name', 'market group for electricity'),
            ws.equals('location', location)
        )
        for market_group_act in market_groups:
            # in the technosphere we have the local markets (per country)
            technosphere = [e for e in market_group_act.technosphere()]
            for ex in technosphere:
                # delete upstream of the local market (country)
                for e in ex.input.upstream():
                    e.delete()
            # delete market group upstream also
            for e in market_group_act.upstream():
                e.delete()


def unlink_heat(db_name: str = 'premise_base'):
    """
    It gets the market groups for heat ('central or small-scale, biomethane', 'central or small-scale, natural gas',
    'central or small scale, other than natural gas', 'district or industrial, natural gas',
    'district or industrial, other than natural gas') and heat production processes in Europe (RER, CH,
    Europe without Switzerland) and deletes upstream.
    """

    market_groups = ws.get_many(
        bd.Database(db_name),
        ws.contains('name', 'market group for heat'),
        ws.equals('location', 'RER')
    )
    for market_group_act in market_groups:
        # Each market group has CH and Europe without Switzerland
        technosphere = [e for e in market_group_act.technosphere()]
        for ex in technosphere:
            # delete upstream of the local market (country)
            for e in ex.input.upstream():
                e.delete()
        for e in market_group_act.upstream():
            e.delete()

    # heat production activities and market for heat activities
    locations = ['RER', 'Europe without Switzerland', 'CH']
    for location in locations:
        heat_production_acts = ws.get_many(
            bd.Database(db_name),
            ws.contains('name', 'heat production,'),
            ws.equals('location', location),
            ws.exclude(ws.contains('name', 'wheat'))
        )
        for act in heat_production_acts:
            for e in act.upstream():
                e.delete()
        market_for_heat_acts = ws.get_many(
            bd.Database(db_name),
            ws.contains('name', 'market for heat,'),
            ws.equals('location', location)
        )
        for act in market_for_heat_acts:
            for e in act.upstream():
                e.delete()


def unlink_co2(db_name: str = 'premise_base'):
    """
     It deletes CO2 inputs for methane production, methanol production and syngas production.
    """
    co2_from_dac_acts = ws.get_many(
        bd.Database(db_name),
        ws.equals('reference product', 'carbon dioxide, captured from atmosphere'),
        ws.equals('location', 'RER')
    )
    for act in co2_from_dac_acts:
        for e in act.upstream():
            e.delete()


def unlink_hydrogen(db_name: str = 'premise_base'):
    """
    It deletes hydrogen outputs for syngas, carbon monoxide production, methanol production, kerosene, diesel, gasoline,
    and ammonia production
    """
    for source in ['electrolysis', 'woody biomass']:
        hydrogen_acts = ws.get_many(
            bd.Database(db_name),
            ws.contains('name', 'hydrogen production, gaseous'),
            ws.contains('name', source)
        )
        for act in hydrogen_acts:
            for e in act.upstream():
                e.delete()


def unlink_biomass(db_name: str = 'premise_base'):
    """
    To avoid double accounting, we want to delete the upstream of those biomass activities that are used as
    fuel to produce heat or electricity, or as feedstock to produce synthetic fuels (kerosene and diesel), methanol and
    methane. Thus, we don't want to include those exchanges where biomass is used for other stuff (e.g., build
    furniture).
    This function finds European markets for wood pellet, wood chips and bark chips, and deletes their upstream exchanges
    that give service to the above-mentioned activities.
    """
    pellet_act = ws.get_one(
        bd.Database(db_name),
        ws.equals('name', 'market for wood pellet, measured as dry mass'),
        ws.equals('location', 'RER')
    )
    for e in pellet_act.upstream():
        if any(ref_prod in e.output['reference product'] for ref_prod in
               ['heat,', 'electricity,', 'methanol,', 'methane,', 'kerosene,', 'diesel,']):
            e.delete()
    # market for biomass, used as fuel
    biomass_as_fuel_act = ws.get_one(
        bd.Database(db_name),
        ws.equals('name', 'market for biomass, used as fuel'),
        ws.equals('location', 'RER')
    )
    for e in biomass_as_fuel_act.upstream():
        e.delete()
    for location in ['Europe without Switzerland', 'CH', 'RER']:
        # wood chips, dry; wood chips, wet; wood chips, post-consumer
        chips_acts = ws.get_many(
            bd.Database(db_name),
            ws.contains('name', 'market for wood chips,'),
            ws.equals('location', location)
        )
        for act in chips_acts:
            for e in act.upstream():
                if any(ref_prod in e.output['reference product'] for ref_prod in
                       ['heat,', 'electricity,', 'methanol,', 'methane,', 'kerosene,', 'diesel,']):
                    e.delete()
        if location != 'RER':
            bark_chips_act = ws.get_one(
                bd.Database(db_name),
                ws.contains('name', 'market for bark chips,'),
                ws.equals('location', location)
            )
            for e in bark_chips_act.upstream():
                if any(ref_prod in e.output['reference product'] for ref_prod in
                       ['heat,', 'electricity,', 'methanol,', 'methane,', 'kerosene,', 'diesel,']):
                    e.delete()


def unlink_methane(db_name: str = 'premise_base'):
    """
        To avoid double accounting, we want to delete the upstream of those biomass activities that are used as
        fuel to produce heat or electricity, or as feedstock to produce synthetic fuels (kerosene and diesel), methanol and
        methane. Thus, we don't want to include those exchanges where natural gas is used as feedstock.
        This function finds European markets for biomethane, methane and natural gas, and deletes their upstream exchanges
        that give service to the above-mentioned activities.
        """
    for location in ['CH', 'RER']:
        biomethane_acts = ws.get_many(
            bd.Database(db_name),
            ws.startswith('reference product', 'biomethane'),
            ws.exclude(ws.contains('reference product', 'mixed')),
            ws.equals('location', location)
        )
        for act in biomethane_acts:
            for e in act.upstream():
                if any(ref_prod in e.output['reference product'] for ref_prod in
                       ['heat,', 'electricity,', 'methanol,', 'kerosene,', 'diesel,']):
                    e.delete()

    methane_acts = ws.get_many(
        bd.Database(db_name),
        ws.startswith('reference product', 'methane,'),
    )  # all in RER
    for act in methane_acts:
        for e in act.upstream():
            if any(ref_prod in e.output['reference product'] for ref_prod in
                   ['heat,', 'electricity,', 'methanol,', 'kerosene,', 'diesel,']):
                e.delete()
    european_locations = list(consts.LOCATION_EQUIVALENCE.values()) + ['RER', 'RoE', 'Europe without Switzerland']
    for location in european_locations:
        nat_gas_acts = ws.get_many(
            bd.Database(db_name),
            ws.startswith('reference product', 'natural gas,'),
            ws.contains('reference product', 'pressure'),
            ws.equals('location', location)
        )
        for act in nat_gas_acts:
            for e in act.upstream():
                if any(ref_prod in e.output['reference product'] for ref_prod in
                       ['heat,', 'electricity,', 'methanol,', 'kerosene,', 'diesel,']):
                    e.delete()


def unlink_methanol(db_name: str = 'premise_base'):
    """
    Because methanol can be used as a feedstock, we want to delete the entire upstream (unless in gives service to
    another methanol production activity).
    """
    for location in ['CH', 'RER']:
        methanol_acts = ws.get_many(
            bd.Database(db_name),
            ws.startswith('reference product', 'methanol,'),
            ws.equals('location', location)
        )
        for act in methanol_acts:
            for e in act.upstream():
                if 'methanol' not in e.output['reference product']:
                    e.delete()


def unlink_kerosene(db_name: str = 'premise_base'):
    for location in ['RER', 'Europe without Switzerland', 'CH']:
        kerosene_acts = ws.get_many(
            bd.Database(db_name),
            ws.startswith('reference product', 'kerosene'),
            ws.equals('location', location)
        )
        for act in kerosene_acts:
            for e in act.upstream():
                if any(ref_prod in e.output['reference product'] for ref_prod in
                       ['heat,', 'electricity,', 'methanol,', 'methane,', 'diesel,']):
                    e.delete()


def unlink_diesel(db_name: str = 'premise_base'):
    for location in ['RER', 'Europe without Switzerland', 'CH']:
        diesel_acts = ws.get_many(
            bd.Database(db_name),
            ws.startswith('reference product', 'diesel'),
            ws.equals('location', location)
        )
        for act in diesel_acts:
            for e in act.upstream():
                if any(ref_prod in e.output['reference product'] for ref_prod in
                       ['heat,', 'electricity,', 'methanol,', 'methane,', 'kerosene,']):
                    e.delete()


# 1.2.1 Cement update
def cement_update():
    """
    1. It creates a copy of the activity clinker production, efficient, with on-site CCS from premise_cement base
    2. It re-links its inputs to those of premise_base
    3. It deletes premise_cement (as we won't need it anymore)
    4. It substitutes clinker production (without CCS) upstream for clinker production with CCS in European regions
    NOTE: premise_cement database is a premise database (image_rcp19, 2050) with ndb.update('cement')
    """
    cement_ccs_original = ws.get_one(
        bd.Database('premise_cement'),
        ws.equals('name', 'clinker production, efficient, with on-site CCS'),
        ws.equals('location', 'WEU')
    )
    # create a copy of the clinker production with CCS
    cement_ccs = cement_ccs_original.copy(database='premise_base')
    cement_ccs['location'] = 'RER'
    cement_ccs.save()
    # relink inputs to 'premise_base' acts
    for ex in cement_ccs.technosphere():
        if ex.input['location'] == 'WEU':
            location = 'RER'
        else:
            location = ex.input['location']
        ex.input = ws.get_one(bd.Database('premise_base'),
                              ws.equals('name', ex.input['name']),
                              ws.equals('location', location),
                              ws.equals('reference product', ex.input['reference product']))
        ex.save()
    # substitute upstream of clinker production in Europe for clinker production with CCS
    for location in ['Europe without Switzerland', 'CH']:
        cement_original = ws.get_one(
            bd.Database('premise_base'),
            ws.equals('name', 'clinker production'),
            ws.equals('location', location),
            ws.equals('reference product', 'clinker')
        )
        for ex in cement_original.upstream():
            ex.input = cement_ccs
            ex.save()


# 1.2.2 Change rail market, so it is only electric
def train_update(db_name: str = 'premise_base'):
    """
    Change train freight transport to 100% electric.
    Two European markets:
    1. market for transport, freight train (Europe without Switzerland)
    2. market group for transport, freight train (RER)
    The second one has as inputs the first one for both Europe without Switzerland and CH. These two,
    have electric and diesel inputs. The function deletes the diesel input and changes the amount of electric to 1.
    """
    market_europe = ws.get_one(
        bd.Database(db_name),
        ws.equals('name', 'market for transport, freight train'),
        ws.equals('location', 'Europe without Switzerland')
    )
    market_ch = ws.get_one(
        bd.Database(db_name),
        ws.equals('name', 'market for transport, freight train'),
        ws.equals('location', 'CH'))
    for act in [market_europe, market_ch]:
        ex_diesel = [e for e in act.technosphere() if 'diesel' in e.input['name']][0]
        ex_electric = [e for e in act.technosphere() if 'electricity' in e.input['name']][0]
        ex_electric['amount'] = 1
        ex_electric.save()
        ex_diesel.delete()


def biomass_update(residues_share: float = 1.0):
    """
    Calliope assumes biomass as all coming from forest and waste residues (ref:
    https://www.sciencedirect.com/science/article/pii/S2542435120303366#sectitle0125).
    """
    create_additional_acts_db()
    market_biomass = ws.get_one(
        bd.Database('premise_cement'),
        ws.equals('name', 'market for biomass, used as fuel'),
        ws.equals('location', 'WEU')
    )
    biomass_act = market_biomass.copy(database='additional_acts')
    # change amounts AND relink inputs
    for ex_input in biomass_act.technosphere():
        if 'residue' in ex_input.input['reference product']:
            ex_input['amount'] = residues_share
            new_input = ex_input.input.copy(database='additional_acts')
            ex_input.input = new_input
            ex_input.save()
            for ex in new_input.technosphere():
                ex.input = ws.get_one(bd.Database('premise_base'), ws.equals('name', ex.input['name']),
                                      ws.equals('location', ex.input['location']),
                                      ws.equals('reference product', ex.input['reference product']))
                ex.save()
        else:
            ex_input['amount'] = 1 - residues_share
            ex_input.input = ws.get_one(bd.Database('premise_base'), ws.equals('name', ex_input.input['name']),
                                        ws.equals('location', ex_input.input['location']),
                                        ws.equals('reference product', ex_input.input['reference product']))
            ex_input.save()

    upstream_activity_amount_dict = {}
    # change upstream
    for ex in market_biomass.upstream():
        ex.input = biomass_act
        upstream_activity_amount_dict[ex.output] = ex['amount']
        ex.output = ws.get_one(bd.Database('premise_base'), ws.equals('name', ex.output['name']),
                               ws.equals('location', ex.output['location']),
                               ws.equals('reference product', ex.output['reference product']))
        ex.save()
    biomass_act['location'] = 'RER'
    biomass_act.save()

    # delete wood chips inputs to avoid duplicates
    for act, amount in upstream_activity_amount_dict.items():
        for e in act.technosphere():
            if e['amount'] == amount:
                e.delete()


# 1.2.3 steel update
def iron_steel_h2_dri_eaf():
    """
    1. Create iron pellet, with data from Nurdiawati et al., 2023 (https://doi.org/10.1016/j.jclepro.2023.136262)
    and Remus et al. 2013 (https://doi.org/10.2791/97469). Infrastructure added as in RoW Ecoinvent dataset. NOTE:
    direct CO2 emissions adjusted to 0.01, assuming 60% reduction from RoW Ecoinvent dataset.
    2. Create iron pellet market, which includes the transport from LKAB mines in Sweden to central Sweden by train +
    cargo vessel + truck
    3. Create iron production (from H2-DRI). Hydrogen and Heat inputs calculated stochiometrically. Other inoputs and
    outputs from Nurdiawati et al., 2023.
    4. Steel production (from H2-DRI). For both steel low-alloyed, and chromium steel. Assumption: 50% scrap iron, 50%
    iron from H2-DRI route. Exact same proces as steel production with EAF, but changing the iron input.
    """
    # create iron pellet (Europe)
    new_act = bd.Database('premise_base').new_activity(
        name='iron pellet production',
        code='iron pellet production',
        unit='kilogram',
        location='RER',
        comment='based on Nurdiawati et al., 2023 (SI Table A8) [https://doi.org/10.1016/j.jclepro.2023.136262], and'
                'Remus et al., 2013 [https://doi.org/10.2791/97469]. It assumes the production in LKAB facilities '
                'in Sweden. Infrastructure added as in RoW Ecoinvent dataset. NOTE: '
                'direct CO2 emissions adjusted to 0.01, assuming 60% reduction from RoW Ecoinvent dataset.'
    )
    new_act['reference product'] = 'iron pellet'
    new_act.save()
    # output
    new_ex = new_act.new_exchange(input=new_act.key, amount=1, type='production')
    new_ex.save()
    # inputs from technosphere
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'aluminium oxide factory '
                                                                     'construction'),
                                                   ws.equals('location', 'RER')),
                                  amount=0.000000000025, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'iron ore beneficiation'),
                                                   ws.equals('location', 'RoW')),
                                  amount=0.9499, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'market for hard coal'),
                                                   ws.equals('location', 'Europe, without Russia and Turkey')),
                                  amount=0.0077, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'bentonite quarry operation'),
                                                   ws.equals('location', 'DE')
                                                   ),
                                  amount=0.0053, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'market for dolomite'),
                                                   ws.equals('location', 'RER')),
                                  amount=0.0068, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'market for lime'),
                                                   ws.equals('location', 'RER')),
                                  amount=0.0025, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'compressed air production, '
                                                                     '1000 kPa gauge, <30kW, optimised generation'),
                                                   ws.equals('location', 'RER')),
                                  amount=0.0089, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'market group for light fuel oil'),
                                                   ws.equals('location', 'RER')),
                                  amount=0.0020, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'market group for electricity, '
                                                                     'medium voltage'),
                                                   ws.equals('location', 'Europe without Switzerland')),
                                  amount=0.0203, type='technosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name', 'market group for tap water'),
                                                   ws.equals('location', 'RER')),
                                  amount=0.0004, type='technosphere')
    new_ex.save()
    # biosphere
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Cadmium II'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(0.02 * 2.2 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Chromium III'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(5.1 * 22.4 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Copper ion'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(1.6 * 6.7 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Mercury II'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(0.4 * 24.2 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Manganese II'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(5.1 * 64.3 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Nickel II'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(6.5 * 12.7 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Lead II'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(15.6 * 70.8 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Tellurium'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(0.6 * 3.0 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Vanadium V'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(13.4 * 15.1 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Zinc II'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(3 * 1300 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Particulate Matter, < 2.5 um'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(14 * 150 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Particulate Matter, < 2.5 um'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(14 * 150 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Hydrogen fluoride'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(1.8 * 5.8 / 1000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Hydrochloric acid'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(2.4 * 41.0 / 1000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Sulfur oxides'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(11 * 213 / 1000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Nitrogen oxides'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(150 * 550 / 1000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Carbon monoxide, fossil'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(10 * 410 / 1000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.equals('name', 'Carbon dioxide, fossil'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=0.01, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.startswith('name', 'NMVOC,'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(5 * 40 / 1000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.startswith('name', 'PAH,'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(0.7 * 1.1 / 1000000000000) ** 0.5, type='biosphere')
    new_ex.save()
    new_ex = new_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.startswith('name', 'Dioxins,'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=(0.7 * 1.1 / 1000000000000000) ** 0.5, type='biosphere')
    new_ex.save()

    # create market for iron pellet (Europe), which includes transportation from Sweden to DRI plant site
    market_act = bd.Database('premise_base').new_activity(
        name='market for iron pellet',
        code='market for iron pellet',
        unit='kilogram',
        location='RER',
        comment='based on Nurdiawati et al., 2023. '
    )
    market_act['reference product'] = 'iron pellet'
    market_act.save()
    # output
    new_ex = market_act.new_exchange(input=market_act.key, amount=1, type='production')
    new_ex.save()
    # technosphere: iron pellet + transport
    new_ex = market_act.new_exchange(input=new_act,
                                     amount=1, type='technosphere')
    new_ex.save()
    new_ex = market_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                      ws.equals('name', 'transport, freight train, electricity'),
                                                      ws.equals('location', 'Europe without Switzerland')),
                                     amount=220 * 0.001, type='technosphere')
    new_ex.save()
    new_ex = market_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                      ws.equals('name',
                                                                'transport, freight, sea, bulk carrier for dry goods'),
                                                      ws.equals('location', 'GLO')),
                                     amount=776 * 0.001, type='technosphere')
    new_ex.save()
    new_ex = market_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                      ws.equals('name',
                                                                'transport, freight, lorry >32 metric ton, EURO6'),
                                                      ws.equals('location', 'RER')),
                                     amount=64 * 0.001, type='technosphere')
    new_ex.save()

    # Create DRI activity
    dri_act = bd.Database('premise_base').new_activity(
        name='iron production, from DRI',
        code='iron production, from DRI',
        unit='kilogram',
        location='RER',
        comment='based on Nurdiawati et al., 2023. Table A12'
    )
    dri_act['reference product'] = 'iron, from H2-DRI'
    dri_act.save()
    # output
    new_ex = dri_act.new_exchange(input=dri_act.key, amount=1, type='production')
    new_ex.save()
    # technosphere
    new_ex = dri_act.new_exchange(input=market_act,
                                  amount=1.391, type='technosphere')
    new_ex.save()
    new_ex = dri_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name',
                                                             'hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity'),
                                                   ws.equals('location', 'RER')),
                                  amount=0.054,
                                  type='technosphere')  # stochiometric recalculation based on Nurdiawati et al., 2023
    new_ex.save()
    new_ex = dri_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name',
                                                             'market group for heat, district or industrial, natural gas'),
                                                   ws.equals('location', 'RER')),
                                  amount=2.5, type='technosphere')  # stochiometric calculation
    new_ex.save()
    new_ex = dri_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                   ws.equals('name',
                                                             'water production, decarbonised'),
                                                   ws.equals('location', 'DE')),
                                  amount=1.5, type='technosphere')
    new_ex.save()
    # biosphere
    new_ex = dri_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                   ws.startswith('name', 'Carbon dioxide, fossil'),
                                                   ws.equals('type', 'emission'),
                                                   ws.equals('categories', ('air',))
                                                   ),
                                  amount=0.04, type='biosphere')
    new_ex.save()

    # steel-EAF process: 50% scrap iron, 50% iron from H2-DRI. For both steel and chromium steel
    steel_acts_original = ws.get_many(bd.Database('premise_base'),
                                      ws.startswith('name', 'steel production, electric,'),
                                      ws.contains('reference product', 'steel')
                                      )
    steel_act_to_return = []
    chromium_steel_act_to_return = []
    for act in steel_acts_original:
        if any(location in act['location'] for location in ['Europe without Switzerland', 'RER']):
            steel_act = act.copy()
            steel_act['reference product'] = f'{steel_act["reference product"]}, from DRI'
            steel_act['name'] = f'{steel_act["name"]}, from DRI-EAF'
            steel_act.save()
            iron_exchange = \
                [e for e in steel_act.technosphere() if e.input['name'] == 'market for iron scrap, sorted, pressed'][0]
            new_ex = steel_act.new_exchange(input=dri_act, type='technosphere', amount=iron_exchange.amount / 2)
            new_ex.save()
            iron_exchange['amount'] = iron_exchange.amount / 2
            iron_exchange.save()
            if steel_act['location'] == 'RER':
                chromium_steel_act_to_return.append(steel_act)
            else:
                steel_act_to_return.append(steel_act)

    return chromium_steel_act_to_return[0], steel_act_to_return[0], dri_act


def steel_update():
    """
    1. Executes iron_steel_h2_dri_eaf(), so the iron and steel H2-DRI-EAF inventories are created
    2. Filters the markets for steel (unalloyed, low-alloyed, chromium steel) and in case they give a service to an
    activity happening in Europe in the upstream, they relink it to the equivalent H2-DRI-EF
    3. Same process with cast iron act
    """
    chromium_act, steel_act, iron_act = iron_steel_h2_dri_eaf()
    # substitute steel
    steel_market_acts = ws.get_many(bd.Database('premise_base'),
                                    ws.startswith('name', 'market for steel,'),
                                    )
    locations = list(consts.LOCATION_EQUIVALENCE.values()) + ['RER', 'Europe']
    for act in steel_market_acts:
        for ex in act.upstream():
            if any(loc in ex.output._data['location'] for loc in locations) or 'market for steel' in ex.output['name']:
                if 'chromium' in ex.input['name']:
                    ex.input = chromium_act
                    ex.save()
                else:
                    ex.input = steel_act
                    ex.save()
    # substitute iron
    cast_iron_act = ws.get_one(bd.Database('premise_base'),
                               ws.equals('name', 'market for cast iron'))
    for ex in cast_iron_act.upstream():
        if any(loc in ex.output._data['location'] for loc in locations):
            if 'chromium' in ex.input['name']:
                ex.input = chromium_act
                ex.save()
            else:
                ex.input = steel_act
                ex.save()


def delete_methanol_facility_duplicate():
    """
    The original data source (https://doi.org/10.1039/C9SE00658C) does not report a facility use for the purification
    of methanol. However, premise adds it and it represents 99% of impacts of methanol production because it is
    over-dimensioned. This function deletes it.
    """
    methanol_facility_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'methanol production facility, construction'))
    for ex in methanol_facility_act.upstream():
        if ', purified' in ex.output._data['reference product']:
            ex.delete()


# plastics
def methanol_to_olefins():
    delete_methanol_facility_duplicate()
    for output in ['propylene', 'ethylene', 'butene']:
        output_act = bd.Database('premise_base').new_activity(
            name=f'{output} production, from methanol (energy allocation)',
            code=f'{output} production, from methanol (energy allocation)',
            unit='kilogram',
            location='RER',
            comment='based on Chen et al., 2024. '
                    '(https://research.tue.nl/en/studentTheses/comparative-life-cycle-assessment-of-methanol-to-olefins-and-meth). '
                    'Table 14. Methanol-to-olefin process. Energy allocation. '
                    'Outputs (propylene: 46.4 MJ/kg, ethylene: 47.2 MJ/kg, butane: 45.7 MJ/kg)'
        )
        output_act['reference product'] = f'{output}, from methanol'
        output_act.save()
        # output
        new_ex = output_act.new_exchange(input=output_act.key, amount=1, type='production')
        new_ex.save()
        # technosphere
        if output == 'propylene':
            allocation_factor = (1 * 46.4) / (1 * 46.4 + 0.85 * 47.2 + 0.3 * 45.7)
        elif output == 'ethylene':
            allocation_factor = (0.85 * 47.2) / (1 * 46.4 + 0.85 * 47.2 + 0.3 * 45.7)
        else:
            allocation_factor = (0.3 * 45.7) / (1 * 46.4 + 0.85 * 47.2 + 0.3 * 45.7)
        new_ex = output_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                          ws.equals('name',
                                                                    'methanol distillation, hydrogen from electrolysis, CO2 from DAC')
                                                          ),
                                         amount=5.93 * allocation_factor, type='technosphere')
        new_ex.save()
        new_ex = output_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                          ws.equals('name',
                                                                    'market group for electricity, medium voltage'),
                                                          ws.equals('location', 'RER')
                                                          ),
                                         amount=0.98973 * allocation_factor, type='technosphere')
        new_ex.save()
        new_ex = output_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                          ws.equals('name',
                                                                    'steam production, in chemical industry'),
                                                          ws.equals('location', 'RER')
                                                          ),
                                         amount=10.75 * allocation_factor, type='technosphere')
        new_ex.save()
        new_ex = output_act.new_exchange(input=ws.get_one(bd.Database('premise_base'),
                                                          ws.equals('name',
                                                                    'water production, deionised'),
                                                          ws.equals('location', 'Europe without Switzerland')
                                                          ),
                                         amount=23.70 * allocation_factor, type='technosphere')
        new_ex.save()
        # biosphere
        new_ex = output_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                          ws.startswith('name', 'Carbon dioxide, fossil'),
                                                          ws.equals('type', 'emission'),
                                                          ws.equals('categories', ('air',))
                                                          ),
                                         amount=0.36 * allocation_factor, type='biosphere')
        new_ex.save()
        new_ex = output_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                          ws.startswith('name', 'Carbon monoxide, fossil'),
                                                          ws.equals('type', 'emission'),
                                                          ws.equals('categories', ('air',))
                                                          ),
                                         amount=0.00003365 * allocation_factor, type='biosphere')
        new_ex.save()
        new_ex = output_act.new_exchange(input=ws.get_one(bd.Database('biosphere3'),
                                                          ws.startswith('name', 'Nitrogen oxides'),
                                                          ws.equals('type', 'emission'),
                                                          ws.equals('categories', ('air',))
                                                          ),
                                         amount=0.00017516 * allocation_factor, type='biosphere')
        new_ex.save()


def relink_olefins():
    for chemical in ['propylene', 'ethylene']:  # butene does not have European market
        act = ws.get_one(bd.Database('premise_base'),
                         ws.equals('name', f'market for {chemical}'),
                         ws.equals('location', 'RER')
                         )
        for ex in act.upstream():
            ex.input = ws.get_one(bd.Database('premise_base'),
                                  ws.startswith('name', f'{chemical} production, from methanol'))
            ex.save()


def plastics_update():
    methanol_to_olefins()
    relink_olefins()


def methanol_update():
    """
    Substitutes all methanol exchanges in Europe (where methanol is used as feedstock) for methanol from electrolysis.
    """
    methanol_act = ws.get_one(bd.Database('premise_base'), ws.equals('name', 'market for methanol'))
    european_locations = list(consts.LOCATION_EQUIVALENCE.values()) + ['RER', 'RoE', 'Europe']
    for ex in methanol_act.upstream():
        if any(loc in ex.output['location'] for loc in european_locations):
            ex.input = ws.get_one(
                bd.Database('premise_base'),
                ws.equals('name', 'methanol distillation, hydrogen from electrolysis, CO2 from DAC'))
            ex.save()


def ammonia_update():
    """
    Substitutes all ammonia exchanges in Europe for ammonia from hydrogen.
    NOTE: takes around 25 min.
    """
    ammonia_act = ws.get_one(bd.Database('premise_base'),
                             ws.equals('name', 'market for ammonia, anhydrous, liquid'),
                             ws.equals('location', 'RER'))
    for ex in ammonia_act.upstream():
        ex.input = ws.get_one(bd.Database('premise_base'),
                              ws.equals('name', 'ammonia production, hydrogen from electrolysis'))
        ex.save()


def trucks_update():
    """
    1. It improves road transport efficiency by changing EUROX to EURO6.
    2. It makes road transport work with synthetic diesel, from biomass.
    3. All services given by a truck in premise_base, are now 50% provided by an electric truck and 50% provided by a
    synthetic diesel truck.
    NOTE: it takes 15-20 min.
    """
    freight_lorry_acts = ws.get_many(bd.Database('premise_base'),
                                     ws.startswith('name', 'market for transport, freight, lorry'),
                                     ws.equals('location', 'RER'))
    for act in freight_lorry_acts:
        print(act['name'])
        if '16-32' in act['name']:
            electric_mass = '26'
        elif '3.5-7.5' in act['name']:
            electric_mass = '7.5'
        elif '7.5-16' in act['name']:
            electric_mass = '18'
        else:
            electric_mass = '40'

        if 'unspecified' in act['name']:
            print('unspecified. Substituting technosphere for EURO6')
            act.technosphere().delete()
            new_ex = act.new_exchange(
                input=ws.get_one(bd.Database('premise_base'),
                                 ws.equals('name', 'transport, freight, lorry, all sizes, EURO6 to '
                                                   'generic market for transport, freight, lorry, unspecified'),
                                 ws.equals('location', 'RER')
                                 ),
                amount=1, type='technosphere'
            )
            new_ex.save()
            # divide the service: 50% electric, 50% synthetic diesel
            print('Dividing service: 50% electric, 50% synthetic diesel')
            for ex in act.upstream():
                print(f'changing {ex}')
                amount = ex['amount']
                new_ex = ex.output.new_exchange(
                    input=ws.get_one(
                        bd.Database('premise_base'),
                        ws.equals('name', 'transport, freight, lorry, battery electric, 18t gross weight, long haul')
                    ), amount=amount / 2, type='technosphere'
                )
                new_ex.save()
                ex['amount'] = amount / 2
                ex.save()
        elif 'EURO6' not in act['name']:
            print('Dividing service: 50% electric, 50% synthetic diesel')
            for ex in act.upstream():
                print('updating efficiency to EURO6')
                # update efficiency to EURO6
                ex.input = ws.get_one(bd.Database('premise_base'),
                                      ws.equals('name', ex.input['name'][:-1] + '6'),
                                      ws.equals('location', 'RER')
                                      )
                ex.save()
                print(f'changing {ex}')
                # divide the service: 50% electric, 50% synthetic diesel
                amount = ex['amount']
                new_ex = ex.output.new_exchange(
                    input=ws.get_one(
                        bd.Database('premise_base'),
                        ws.equals('name',
                                  f'transport, freight, lorry, battery electric, {electric_mass}t gross weight, long haul')
                    ), amount=amount / 2, type='technosphere'
                )
                new_ex.save()
                ex['amount'] = amount / 2
                ex.save()
        else:
            print('EURO6 vehicle. Adding synthetic diesel')
            # For EURO6 vehicles, synthetic diesel as input
            transpot_act = list(act.technosphere())[0].input
            for ex in transpot_act.technosphere():
                if 'diesel' in ex.input['reference product']:
                    ex.input = ws.get_one(
                        bd.Database('premise_base'),
                        ws.equals('name', 'diesel production, synthetic, Fischer Tropsch process, '
                                          'hydrogen from wood gasification, energy allocation'))
                    ex.save()
            print('Dividing service: 50% electric, 50% synthetic diesel')
            for ex in act.upstream():
                # divide the service: 50% electric, 50% synthetic diesel
                amount = ex['amount']
                new_ex = ex.output.new_exchange(
                    input=ws.get_one(
                        bd.Database('premise_base'),
                        ws.equals('name',
                                  f'transport, freight, lorry, battery electric, {electric_mass}t gross weight, long haul')
                    ), amount=amount / 2, type='technosphere'
                )
                new_ex.save()
                ex['amount'] = amount / 2
                ex.save()


def sea_transport_update():
    """
    1. Takes 'transport, freight, sea' activities (which have GLO locations only) and creates a RER location for them
    2. Substitutes heavy oil for synthetic diesel (from biomass) with the same demand amount. Plus, it deletes heavy oil
    wastes.
    3. Re-links all exchanges where GLO  transport act was giving a service to any European location. Now the service is
    provided by the RER act.
    4. Takes 'market for transport, freight, sea' activities. It creates the RER market, changing its inputs to the
    RER 'transport, freight, sea' activity.
    5. Re-links all exchanges where GLO market act was giving a service to any European location. Now the service is
    provided by the RER act.
    """
    sea_transport_acts = ws.get_many(bd.Database('premise_base'),
                                     ws.startswith('name', 'transport, freight, sea,')
                                     )
    sea_transport_market_acts = ws.get_many(bd.Database('premise_base'),
                                            ws.startswith('name', 'market for transport, freight, sea')
                                            )
    european_locations = list(consts.LOCATION_EQUIVALENCE.values()) + ['RER', 'RoE', 'Europe']
    for act in sea_transport_acts:
        # create European location for the same activity
        european_act = act.copy()
        european_act['location'] = 'RER'
        european_act.save()
        # substitute heavy fuel oil for synthetic diesel
        heavy_fuel_oil_amounts = [e['amount'] for e in european_act.technosphere() if
                                  'heavy fuel oil' in e.input['reference product']]
        oil = False
        for ex in european_act.technosphere():
            if 'oil' in ex.input['reference product']:
                ex.delete()
                oil = True
        if oil:
            diesel_ex = european_act.new_exchange(
                input=ws.get_one(
                    bd.Database('premise_base'),
                    ws.equals('name',
                              f'diesel production, synthetic, Fischer Tropsch process, '
                              'hydrogen from wood gasification, energy allocation')
                ), amount=sum(heavy_fuel_oil_amounts), type='technosphere'
            )
            diesel_ex.save()
        # change the provider of all those activities that have sea transport as an input to the new RER location
        for ex in act.upstream():
            if any(loc in ex.output['location'] for loc in european_locations):
                ex.input = european_act
                ex.save()
    for market in sea_transport_market_acts:
        european_market = market.copy()
        european_market['location'] = 'RER'
        european_market.save()
        # delete technosphere and make it only RER
        european_market.technosphere().delete()
        european_ex = european_market.new_exchange(
            input=ws.get_one(bd.Database('premise_base'),
                             ws.equals('name', european_market['name'][11:]),
                             ws.equals('location', 'RER')
                             ),
            type='technosphere', amount=1
        )
        european_ex.save()
        # change the provider of all those activities that have sea transport as an input to the new RER location
        for ex in market.upstream():
            if any(loc in ex.output['location'] for loc in european_locations):
                ex.input = european_market
                ex.save()


def transport_update():
    """
    Updates transport background for the European regions.
    CONSIDERATIONS
    Air transport: Aircraft transport does not give service to any activity in Europe. Left out.
    Road transport (cars): Cars give service to very few activities in the European market, so they are left out.
    Road transport (trucks): improves efficiency to EURO6, uses synthetic diesel, electrifies half of the fleet
    Sea transport: uses synthetic diesel instead of heavy fuel oil. No diesel wastes accounted for.
    """
    trucks_update()
    sea_transport_update()


def premise_base_auxiliary():
    """
    It creates a copy of premise_base after all background and foreground changes and before infrastructure deletion
    and O&M separation. We want to have these databases because we want to keep premise_base unaltered.
    'premise_auxiliary_for_infrastructure' will be the base for 'infrastructure (with European steel and concrete)'
    """
    if 'premise_auxiliary_for_infrastructure' not in bd.databases:
        bd.Database('premise_base').copy('premise_auxiliary_for_infrastructure')


def update_cement_iron_foreground(
        file_path: str = r'C:\Users\mique\OneDrive - UAB\PhD_ICTA_Miquel\research stay Delft\technology_mapping_clean.xlsx'):
    """
    Because energy infrastructure usually has GLO location, the steel and cement are not updated for them. This function
    does the following to address it:
    1. checks every infrastructure act that is going to be used in the Calliope-Enbios integration (mapping file)
    2. looks for concrete, steel or chromium steel inputs
    3. re-links them to the updated activities for Europe (following H2-DRI-EAF pathway). In the case of concrete,
    'concrete, normal strength' in 'CH' is used no matter what was the type of initial concrete.
    4. Stores all these activities in a new database called 'infrastructure (with European steel and concrete)'
    ASSUMPTIONS:
    - Batteries and PV panels won't be produced in Europe! Their iron, steel and cement in the first tier is not updated
    - Vehicles are left out of these analysis.
    """
    # TODO: address it so it does not break when reaching the end of the document!
    # create infrastructure database
    if 'infrastructure (with European steel and concrete)' not in bd.databases:
        new_db = bd.Database('infrastructure (with European steel and concrete)')
        new_db.register()

    df = pd.read_excel(file_path, sheet_name='Foreground')
    failed = []
    solved = []
    for index, row in df.iterrows():
        print(row['tech'])
        if row['LCI_energy_cap'] in solved:
            continue
        if row['cap_database'] == 'Ecoinvent':
            database = 'premise_auxiliary_for_infrastructure'
        elif row['cap_database'] == 'premise_base':
            database = 'premise_auxiliary_for_infrastructure'
        else:
            database = row['cap_database']
        # address wind fleets
        if 'wind' in row['tech']:
            wind_materials_acts = ws.get_many(bd.Database('additional_acts'),
                                              ws.contains('name', '_materials')
                                              )
            for act in wind_materials_acts:
                if '_offshore_materials' in act['name']:
                    # materials are divided into turbine and substation
                    for e in act.technosphere():
                        cement_iron_steel_subs(e.input)
                        # steel in WindTrace is different. Let's substitute it
                        for ex in e.input.technosphere():
                            if 'steel, low-alloyed' in ex.input['name']:
                                ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                                      ws.equals('name',
                                                                'steel production, electric, low-alloyed, from DRI-EAF'))
                                ex.save()
                else:
                    cement_iron_steel_subs(act)
                    # steel in WindTrace is different. Let's substitute it
                    for ex in act.technosphere():
                        if 'steel, low-alloyed' in ex.input['name']:
                            ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                                  ws.equals('name',
                                                            'steel production, electric, low-alloyed, from DRI-EAF'), )
                            ex.save()

        try:
            org_act = ws.get_one(bd.Database(database),
                                 ws.equals('name', row['LCI_energy_cap']),
                                 ws.equals('location', row['cap_location'])
                                 )
            act = org_act.copy(database='infrastructure (with European steel and concrete)')
            # 'if' statements to deal with EXCEPTIONS
            # it only affects market for heat pumps.
            if 'market' in act['name'] or 'fuel cell system' in act['name']:
                for ex in act.technosphere():
                    cement_iron_steel_subs(act=ex.input)
            # biomethane factory act contains two infrastructure acts, so their inputs are not being changed. We take
            # the industrial furnace act and substitute it by the equivalent act (production) from CH, which already
            # uses European steel and concrete.
            elif 'biomethane factory' in act['name']:
                for ex in act.technosphere():
                    if 'industrial furnace' in ex.input['name']:
                        furnace_act = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                                 ws.equals('name', 'industrial furnace production, 1MW, oil'),
                                                 ws.equals('location', 'CH'))
                        new_act = furnace_act.copy(database='additional_acts')
                        new_ex = act.new_exchange(input=new_act, type='technosphere', amount=ex['amount'])
                        new_ex.save()
                        ex.delete()
            # municipal solid waste incinerator act consists of three inputs. We copy them to additional_acts,
            # apply steel and iron changes to Europe, and relink to the activity.
            elif 'municipal solid waste' in act['name']:
                input_acts = []
                for ex in act.technosphere():
                    ex.input.copy(database='additional_acts')
                    new_act = ws.get_one(bd.Database('additional_acts'), ws.equals('name', ex.input['name']),
                                         ws.equals('location', ex.input['location']))
                    cement_iron_steel_subs(new_act)
                    input_acts.append(new_act)
                act.technosphere().delete()
                for new_act in input_acts:
                    new_ex = act.new_exchange(input=new_act, type='technosphere', amount=1)
                    new_ex.save()
            else:
                cement_iron_steel_subs(act=act)
            solved.append(row['LCI_energy_cap'])

        except Exception as e:
            failed.append(row['LCI_energy_cap'])

    return failed


def cement_iron_steel_subs(act):
    steel_list = {
        'market for steel, chromium steel 18/8': 'steel production, electric, chromium steel 18/8, from DRI-EAF',
        'market for steel, low-alloyed': 'steel production, electric, low-alloyed, from DRI-EAF',
        'market for steel, unalloyed': 'steel production, electric, low-alloyed, from DRI-EAF',
        'market for reinforcing steel': 'steel production, electric, low-alloyed, from DRI-EAF'
    }
    steel_hot_rolled_list = {
        'market for steel, chromium steel 18/8, hot rolled': 'steel production, electric, chromium steel 18/8, from DRI-EAF',
        'market for steel, low-alloyed, hot rolled': 'steel production, electric, low-alloyed, from DRI-EAF',
    }
    for ex in act.technosphere():
        # substitute iron
        if ex.input['name'] == 'market for cast iron' or ex.input['name'] == 'cast iron production':
            ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                  ws.equals('name', 'iron production, from DRI'))
            ex.save()
        # substitute steel
        elif any(name == ex.input['name'] for name in steel_list.keys()):
            try:
                ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                      ws.equals('name', steel_list[ex.input['name']]),
                                      ws.equals('location', 'RER'))
                ex.save()
            except Exception:
                pass
            try:
                ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                      ws.equals('name', steel_list[ex.input['name']]),
                                      ws.equals('location', 'Europe without Switzerland and Austria'))
                ex.save()
            except Exception:
                pass
        # substitute steel, hot rolled
        elif any(name == ex.input['name'] for name in steel_hot_rolled_list.keys()):
            try:
                ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                      ws.equals('name', steel_hot_rolled_list[ex.input['name']]),
                                      ws.equals('location', 'RER'))
                ex.save()
                hot_rolling_act = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                             ws.equals('name', 'hot rolling, steel'),
                                             ws.equals('location', 'Europe without Austria'))
                new_ex = act.new_exchange(input=hot_rolling_act, type='technosphere', amount=ex['amount'])
                new_ex.save()
            except Exception:
                pass
            try:
                ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                      ws.equals('name', steel_hot_rolled_list[ex.input['name']]),
                                      ws.equals('location', 'Europe without Switzerland and Austria'))
                ex.save()
                hot_rolling_act = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                             ws.equals('name', 'hot rolling, steel'),
                                             ws.equals('location', 'Europe without Austria'))
                new_ex = act.new_exchange(input=hot_rolling_act, type='technosphere', amount=ex['amount'])
                new_ex.save()
            except Exception:
                pass

        # substitute concrete
        elif 'concrete' in act['name']:
            try:
                ex.input = ws.get_one(bd.Database('premise_auxiliary_for_infrastructure'),
                                      ws.equals('name', 'market for concrete, normal strength'),
                                      ws.equals('location', 'CH'))
                ex.save()
            except Exception:
                pass


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


def delete_infrastructure_main(
        file_path: str = r'C:\Users\mique\OneDrive - UAB\PhD_ICTA_Miquel\research stay Delft\technology_mapping_clean.xlsx',
        om_spheres_separation: bool = True
):
    """
    It takes all the activities in 'technology_map_clean.xlsx', finds the exact activity
    (or activities in plural if it has multiple options, i.e., those activities that exist for all
    locations in Calliope), and deletes the inputs that are infrastructure. Then, it creates a copy of the activity in
    additional_acts (adding ', biosphere' at the end of the name), and it removes the technosphere, handling the
    exceptions to hydrogen, diesel and kerosene. Moreover, it creates another copy of the activity in
    additional_acts (adding ', technosphere' at the end of the name), and it removes the biosphere.
    """
    # delete infrastructure
    df = pd.read_excel(file_path, sheet_name='Foreground')
    for name, location, database, reference_product in (
            zip(df['LCI_operation_and_maintenance'], df['prod_location'], df['prod_database'],
                df['reference product'])):
        print('NEXT ACTIVITY')
        # Skip if any of the following conditions are met
        if name == '-' or name == 'No activity found' or location == '-':
            continue
        print(f'Name: {name}')
        print(f'Location: {location}')
        print(f'Database: {database}')
        print(f'Reference product: {reference_product}')
        # Adjust the database name if needed
        if database == 'Ecoinvent':
            database = 'premise_base'

        # If the location is 'country', start checking for activities
        if location == 'country':
            for loc in consts.LOCATION_EQUIVALENCE.values():
                try:
                    act = ws.get_one(bd.Database(database), ws.contains('name', name),
                                     ws.exclude(ws.contains('name', 'renewable energy products')),
                                     ws.equals('location', loc),
                                     ws.contains('reference product', reference_product))
                    infrastructure = [e for e in act.technosphere() if e.input._data['unit'] == 'unit']
                    for e in infrastructure:
                        e.delete()
                    if om_spheres_separation:
                        om_biosphere(act)
                        om_technosphere(act)
                    print(f'Activity: {name}. Location: {loc}. Ref product: {reference_product}')
                    continue
                except Exception as e:
                    print(f'No activity ({name}) in ({loc})')
                    # there is no need to create copy after copy
                    continue

        else:
            # If location is not 'country', proceed with regular activity lookup
            try:
                if 'wind' in name:
                    act = ws.get_one(bd.Database(database), ws.contains('name', name),
                                     ws.exclude(ws.contains('name', 'renewable energy products')),
                                     ws.equals('location', location)
                                     )
                else:
                    act = ws.get_one(bd.Database(database), ws.contains('name', name),
                                     ws.exclude(ws.contains('name', 'renewable energy products')),
                                     ws.equals('location', location),
                                     ws.contains('reference product', reference_product))
                # we do not want to delete the maintenance of offshore and onshore wind (which have 'unit' as units),
                # and that is why we add this conditional.
                if not any(wind_name in act['name'] for wind_name in ['onshore', 'offshore']):
                    infrastructure = [e for e in act.technosphere() if e.input._data['unit'] == 'unit']
                    for e in infrastructure:
                        e.delete()
                if om_spheres_separation:
                    om_biosphere(act)
                    om_technosphere(act)
                print(f'Activity found for {name} in location: {location}')
            except Exception as e:
                print(f'No activity ({name}) in location: {location}.')
    # deal with fuels
    # dac
    dac_acts = ws.get_many(bd.Database('premise_base'),
                           ws.startswith('name', 'direct air capture system'))
    for act in dac_acts:
        act.upstream().delete()
    # RWGS tank
    rwgs_act = ws.get_one(bd.Database('premise_base'),
                          ws.startswith('name', 'RWGS tank'))
    rwgs_act.upstream().delete()
    # fixed bed reactor
    bed_reactor_act = ws.get_one(bd.Database('premise_base'),
                                 ws.contains('name', 'fixed bed reactor'))
    bed_reactor_act.upstream().delete()
    # syngas factory
    syngas_factory_act = ws.get_one(bd.Database('premise_base'),
                                    ws.equals('name', 'market for synthetic gas factory'))
    syngas_factory_act.upstream().delete()
    # methanol factory
    methanol_factory_act = ws.get_one(bd.Database('premise_base'),
                                      ws.equals('name', 'methanol production facility, construction'))
    methanol_factory_act.upstream().delete()
    # electrolyser
    electrolyzer_acts = ws.get_many(bd.Database('premise_base'),
                                    ws.startswith('name', 'electrolyzer production, 1MWe, PEM'))
    for act in electrolyzer_acts:
        for ex in act.upstream():
            if 'hydrogen' not in ex.output['name']:
                ex.delete()
    # liquid storage tank
    liquid_storage_act = ws.get_one(bd.Database('premise_base'),
                                    ws.equals('name', 'market for liquid storage tank, chemicals, organics'))
    for ex in liquid_storage_act.upstream():
        if any(fuel in ex.output['name'] for fuel in ['hydrogen', 'carbon dioxide', 'methanol']):
            ex.delete()


def om_biosphere(act):
    """
    Creates a copy of the activity and deletes the technosphere.
    """
    biosphere_act = act.copy(database='additional_acts')
    biosphere_act['name'] = f"{biosphere_act['name']}, biosphere"
    biosphere_act.save()
    biosphere_act.technosphere().delete()
    # handle special cases: hydrogen fleet
    if biosphere_act['name'] == 'hydrogen production, from electrolyser fleet, for enbios, biosphere':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=1,
            specific_inputs=['hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity',
                             'hydrogen production, gaseous, 1 bar, from SOEC electrolysis, from grid electricity',
                             'hydrogen production, gaseous, 20 bar, from AEC electrolysis, from grid electricity'
                             ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        biosphere_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = biosphere_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()


def om_technosphere(act):
    """
    Creates a copy of the activity and deletes the biosphere.
    """
    biosphere_act = act.copy(database='additional_acts')
    biosphere_act['name'] = f"{biosphere_act['name']}, technosphere"
    biosphere_act.save()
    biosphere_act.biosphere().delete()
    if biosphere_act['name'] == 'hydrogen production, from electrolyser fleet, for enbios, biosphere':
        for ex in biosphere_act.technsophere:
            ex.input.biosphere().delete()


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
            waste_electricity_original = ws.get_one(bd.Database(db_waste_name),
                                                    ws.equals('name',
                                                              'treatment of municipal solid waste, incineration'),
                                                    ws.equals('location', location),
                                                    ws.contains('reference product', 'electricity')
                                                    )
            print(f'original_location: {location}, assigned location: {location}')
            waste_act = waste_electricity_original.copy(database='additional_acts')
            print(f'creating copy of {waste_act._data["name"]}')
            waste_act.technosphere().delete()
            print(f'deleting technosphere')
            waste_original_heat = ws.get_one(bd.Database(db_waste_name),
                                             ws.equals('name',
                                                       'treatment of municipal solid waste, incineration'),
                                             ws.equals('location', location),
                                             ws.contains('reference product', 'heat')
                                             )
            waste_heat_act = waste_original_heat.copy(database='additional_acts')
            print(f'creating copy of {waste_act._data["name"]}')
            waste_heat_act.technosphere().delete()
            print(f'deleting technosphere')
        # if we do not find the location, CH is chosen by default.
        except wurst.errors.NoResults:
            waste_electricity_original = ws.get_one(bd.Database(db_waste_name),
                                                    ws.equals('name',
                                                              'treatment of municipal solid waste, incineration'),
                                                    ws.equals('location', 'CH'),
                                                    ws.contains('reference product', 'electricity')
                                                    )
            print(f'original_location: {location}, assigned location: CH')
            waste_act = waste_electricity_original.copy(database='additional_acts')
            print(f'copy of {waste_act._data["name"]} created in "additional_acts"')
            print('changing location')
            waste_act['location'] = location
            waste_act['comment'] = waste_act['comment'] + '\n' + 'Taken dataset from CH'
            waste_act.save()
            waste_act.technosphere().delete()
            print(f'deleting technosphere')
            waste_heat_original = ws.get_one(bd.Database(db_waste_name),
                                             ws.equals('name',
                                                       'treatment of municipal solid waste, incineration'),
                                             ws.equals('location', 'CH'),
                                             ws.contains('reference product', 'electricity')
                                             )
            waste_heat_act = waste_heat_original.copy(database='additional_acts')
            waste_heat_act['location'] = location
            waste_heat_act['comment'] = waste_act['comment'] + '\n' + 'Taken dataset from CH'
            waste_heat_act.save()
            waste_heat_act.technosphere().delete()

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


def update_methanol_facility():
    """
    Re-scale methanol infrastructure. According to the original dataset (https://doi.org/10.1039/C9SE00658C) used to
    create the inventories in premise it consists of an adiabatic reactor of 12.6 m3 and an isothermal
    reactor of 8 m3. LT assumed 20 years. Production rate will be 44900000 kg / 20 years / 8000 h = 280 kg/h
    """
    methanol_facility_act_original = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'methanol production facility, construction'))
    methanol_facility_act = methanol_facility_act_original.copy(database='additional_acts')

    for ex in methanol_facility_act.technosphere():
        # chemical factory
        if 'chemical factory' in ex.input['name']:
            ex['amount'] = 0.00899 / 44900000 * 12.5  # data taken from https://doi.org/10.1039/C9SE00658C and premise
            ex.save()
        # air compressor
        elif 'air compressor' in ex.input['name']:
            ex['amount'] = 0.755 / 44900000 * 12.5  # data taken from https://doi.org/10.1039/C9SE00658C and premise
            ex.save()
        # build reactors and heat exchanger
        elif 'concrete' in ex.input['name']:
            ex['amount'] = ex['amount'] * 44900000 / (0.0333 * 12.5 * 2400)
            ex.save()
        elif 'flat glass' in ex.input['name']:
            ex['amount'] = 81  # own estimation
            ex.save()
        else:
            ex['amount'] = ex['amount'] * 44900000 / (0.0333 * 12.5)
            ex.save()


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


def trucks_and_bus_update(db_truck_name: str):
    """
    Creates a copy of 'light duty truck, fuel cell electric, 3.5t gross weight, long haul',
    'medium duty truck, fuel cell electric, 26t gross weight, long haul', and
    'passenger bus, battery electric - opportunity charging, LTO battery, 13m single deck urban bus' in the database
    'additional_acts_db' and deletes all inputs that are not in the keep_inputs list.
    The idea is to model the truck and bus as ONLY those inputs that make the vehicle electric instead of
    using an internal combustion engine.
    """
    # light truck
    light_truck_original = ws.get_one(bd.Database(db_truck_name),
                                      ws.equals('name',
                                                'light duty truck, battery electric, 3.5t gross weight, '
                                                'long haul'),
                                      )
    create_additional_acts_db()
    light_truck_act = light_truck_original.copy(database='additional_acts')
    light_truck_technosphere = list(light_truck_act.technosphere())
    keep_inputs = ['converter', 'inverter', 'fuel tank', 'power electronics', 'other components', 'electric motor',
                   'fuel cell system', 'battery capacity']
    for ex in light_truck_technosphere:
        if not any(input_name in ex.input['name'] for input_name in keep_inputs):
            ex.delete()

    # medium truck
    medium_truck_original = ws.get_one(bd.Database(db_truck_name),
                                       ws.equals('name',
                                                 'medium duty truck, battery electric, 26t gross weight, '
                                                 'long haul'),
                                       )
    medium_truck_act = medium_truck_original.copy(database='additional_acts')
    medium_truck_technosphere = list(medium_truck_act.technosphere())
    keep_inputs = ['power distribution', 'converter', 'inverter', 'fuel tank', 'power electronics', 'other components',
                   'electric motor', 'fuel cell system', 'battery capacity']
    for ex in medium_truck_technosphere:
        if not any(input_name in ex.input['name'] for input_name in keep_inputs):
            ex.delete()
    # bus
    bus_original = ws.get_one(bd.Database(db_truck_name),
                              ws.equals('name',
                                        'passenger bus, battery electric - opportunity charging, LTO battery, '
                                        '13m single deck urban bus'),
                              )
    bus_act = bus_original.copy(database='additional_acts')
    bus_technosphere = list(bus_act.technosphere())
    keep_inputs = ['converter', 'inverter', 'power electronics', 'other components', 'electric motor',
                   'battery capacity', 'power distribution']
    for ex in bus_technosphere:
        if not any(input_name in ex.input['name'] for input_name in keep_inputs):
            ex.delete()


def passenger_car_and_scooter_update(db_passenger_name: str):
    """
    Creates a copy of 'passenger car, battery electric, Medium' in the database 'additional_acts_db' and
    deletes glider inputs.
    """
    car_original = ws.get_one(bd.Database(db_passenger_name),
                              ws.equals('name', 'passenger car, battery electric, Medium'),
                              )
    create_additional_acts_db()
    car_act = car_original.copy(database='additional_acts')
    for ex in car_act.technosphere():
        if 'glider' in ex.input['name']:
            ex.delete()
    scooter_original = ws.get_one(bd.Database(db_passenger_name),
                                  ws.equals('name', 'scooter, battery electric, 4-11kW'),
                                  )
    scooter_act = scooter_original.copy(database='additional_acts')
    for ex in scooter_act.technosphere():
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
    new_ex = gas_to_liquid_act.new_exchange(input=cobalt_act, type='technosphere', amount=1250000)
    new_ex.save()


def hydro_reservoir_update(location: str, db_hydro_name: str):
    """
    :return: transfers land use and emissions from flooding operations to infrastructure instead of operation in
    run-of-river power plants.
    """
    electricity_reservoir = ws.get_one(
        bd.Database(db_hydro_name),
        ws.contains('name', 'electricity production, hydro, reservoir, non-alpine region'),
        ws.equals('location', location)
    )
    create_additional_acts_db()
    new_elec_act = electricity_reservoir.copy(database='additional_acts')
    infrastructure_act = [e.input for e in new_elec_act.technosphere() if e.input._data['unit'] == 'unit'][0]
    new_infrastructure_act = infrastructure_act.copy(database='additional_acts')
    infrastructure_amount = [e.amount for e in new_elec_act.technosphere() if e.input._data['unit'] == 'unit'][0]
    land = [e for e in new_elec_act.biosphere() if
            any(keyword in e.input._data['name'] for keyword in ['Occupation', 'occupied', 'Transformation'])]
    emissions = [e for e in new_elec_act.biosphere() if
                 any(keyword in e.input._data['name'] for keyword in ['Carbon dioxide', 'monoxide', 'Methane'])]
    for e in land:
        new_amount = e.amount / infrastructure_amount
        biosphere_act = e.input
        e.delete()
        new_ex = new_infrastructure_act.new_exchange(input=biosphere_act, type='biosphere', amount=new_amount)
        new_ex.save()
    for e in emissions:
        new_amount = e.amount / infrastructure_amount
        biosphere_act = e.input
        e.delete()
        new_ex = new_infrastructure_act.new_exchange(input=biosphere_act, type='biosphere', amount=new_amount)
        new_ex.save()
    infrastructure_ex = [e for e in new_elec_act.technosphere() if e.input._data['unit'] == 'unit'][0]
    infrastructure_ex.delete()


def hydro_run_of_river_update(db_hydro_name: str):
    """
    :return: transfers land use and emissions from flooding operations to infrastructure instead of operation in
    run-of-river power plants. Location always CA-QC, as it is the only one with clear info in MW of infrastructure.
    """
    electricity_run_of = ws.get_one(bd.Database(db_hydro_name),
                                    ws.contains('name', 'electricity production, hydro, run-of-river'),
                                    ws.equals('location', 'CA-QC'))
    create_additional_acts_db()
    new_elec_act = electricity_run_of.copy(database='additional_acts')
    infrastructure_act = [e.input for e in new_elec_act.technosphere() if e.input._data['unit'] == 'unit'][0]
    new_infrastructure_act = infrastructure_act.copy(database='additional_acts')
    infrastructure_amount = [e.amount for e in new_elec_act.technosphere() if e.input._data['unit'] == 'unit'][0]
    land = [e for e in new_elec_act.biosphere() if
            any(keyword in e.input._data['name'] for keyword in ['Occupation', 'occupied', 'Transformation'])]
    emissions = [e for e in new_elec_act.biosphere() if
                 any(keyword in e.input._data['name'] for keyword in ['Carbon dioxide', 'monoxide', 'Methane'])]
    for e in land:
        new_amount = e.amount / infrastructure_amount
        biosphere_act = e.input
        e.delete()
        new_ex = new_infrastructure_act.new_exchange(input=biosphere_act, type='biosphere', amount=new_amount)
        new_ex.save()
    for e in emissions:
        new_amount = e.amount / infrastructure_amount
        biosphere_act = e.input
        e.delete()
        new_ex = new_infrastructure_act.new_exchange(input=biosphere_act, type='biosphere', amount=new_amount)
        new_ex.save()
    infrastructure_ex = [e for e in new_elec_act.technosphere() if e.input._data['unit'] == 'unit'][0]
    infrastructure_ex.delete()


def fuels_combustion():
    # bus
    bus_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'transport, passenger bus, diesel, 13m single deck coach bus, EURO-VI'))
    # heavy transport
    heavy_transport_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'transport, freight, lorry, diesel, 26t gross weight, EURO-VI, long haul'))
    # light transport
    light_transport_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'transport, freight, lorry, diesel, 3.5t gross weight, EURO-VI, long haul'))
    # passenger car
    passenger_car_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'transport, passenger car, diesel, Medium, EURO-6'))
    # motorcycle
    motorcycle_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'transport, Motorbike, gasoline, 4-11kW, EURO-5'))
    # air transport
    air_transport_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'transport, freight, aircraft, belly-freight, medium haul'))
    # sea transport
    sea_transport_act = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'transport, freight, sea, container ship'),
        ws.equals('location', 'RER')
    )
    for act in [bus_act, heavy_transport_act, light_transport_act, passenger_car_act,
                motorcycle_act, air_transport_act, sea_transport_act]:
        act_copy = act.copy(database='additional_acts')
        act_copy.technosphere().delete()


def biofuel_to_methane_infrastructure(db_syn_gas_name: str):
    """
    It creates a biomethane factory that includes 1 synthetic gas factory and 7.11 industrial furnaces.
    """
    syn_gas_factory = ws.get_one(bd.Database(db_syn_gas_name),
                                 ws.equals('name', 'synthetic gas factory construction'),
                                 ws.equals('location', 'CH'))
    furnace = ws.get_one(bd.Database(db_syn_gas_name),
                         ws.equals('name', 'industrial furnace production, natural gas'),
                         ws.equals('location', 'RER'))
    create_additional_acts_db()
    new_act = bd.Database('additional_acts').new_activity(name='biomethane factory', code='biomethane factory',
                                                          location='RER', unit='unit')
    new_act['reference product'] = 'biomethane factory'
    new_act.save()
    new_ex = new_act.new_exchange(input=new_act.key, type='production', amount=1)
    new_ex.save()
    new_ex = new_act.new_exchange(input=furnace, type='technosphere', amount=7.11)
    new_ex.save()
    new_ex = new_act.new_exchange(input=syn_gas_factory, type='technosphere', amount=1)
    new_ex.save()


def hp_update(db_hp_name: str):
    heat_exchanger = ws.get_one(bd.Database(db_hp_name),
                                ws.equals('name', 'market for borehole heat exchanger, 150m'),
                                ws.equals('location', 'GLO'))
    heat_pump = ws.get_one(bd.Database(db_hp_name),
                           ws.equals('name', 'market for heat pump, brine-water, 10kW'),
                           ws.equals('location', 'GLO'))
    create_additional_acts_db()
    new_act = bd.Database('additional_acts').new_activity(name='heat pump with heat exchanger, brine-water, 10kW',
                                                          code='heat pump with heat exchanger, brine-water, 10kW',
                                                          location='RER', unit='unit')
    new_act['reference product'] = 'heat pump with heat exchanger, brine-water, 10kW'
    new_act.save()
    new_ex = new_act.new_exchange(input=new_act.key, type='production', amount=1)
    new_ex.save()
    new_ex = new_act.new_exchange(input=heat_exchanger, type='technosphere', amount=0.401)
    new_ex.save()
    new_ex = new_act.new_exchange(input=heat_pump, type='technosphere', amount=1)
    new_ex.save()


def airborne_wind_lci(bd_airborne_name: str):
    """
    Based on Wilhelm, (2015). Rigid. Yo-yo. Total power: 328 MW. Number of systems: 182. Rated power: 1.8MW.
    Annual electricity production: 6142 MWh/y.
    Lifetime: 20 years.
    Parts of the system: wing system, tethering, ground station, landing system, launcher (rail track).
    Life-cycle phases: materials, manufacturing, installation. No EoL. No transport (it can't be as much
    as reported in the paper!). Maintenance in a different inventory.
    """
    input_data = {
        'steel': [{'market for steel, low-alloyed': 'GLO', 'hot rolling, steel': 'Europe without Austria'}, 73455],
        'iron': [{'cast iron production': 'RER', 'hot rolling, steel': 'Europe without Austria'}, 21165],
        'aluminium': [{'market for aluminium, wrought alloy': 'GLO', 'sheet rolling, aluminium': 'RER'}, 6225],
        'copper': [{'market for copper, cathode': 'GLO', 'sheet rolling, copper': 'RER'}, 3984],
        'pe': [{'polyethylene production, low density, granulate': 'RER', 'extrusion, plastic film': 'RER'}, 13446],
        'concrete': [{'market group for concrete, normal strength': 'GLO'}, 29631 / 2400],
        'gravel': [{'gravel production, crushed': 'CH'}, 78933],
        'cfrp': [{'carbon fibre reinforced plastic, injection moulded': 'GLO'}, 2241],
        'plywood': [{'plywood production': 'RER'}, 17430 / 700]  # plywood density around 700 kg/m3
    }
    create_additional_acts_db()
    new_act = bd.Database('additional_acts').new_activity(
        name='airborne wind system, 328MW', code='airborne wind system, 328MW', location='RER', unit='unit',
        comment='Rigid. Yo-yo. Total power: 328 MW. Number of systems: 182. Rated power: 1.8MW. '
                'Annual electricity production: 6142 MWh/y.'
                'Lifetime: 20 years.  Based on Wilhelm, (2015).')
    new_act['reference product'] = 'airborne wind system, 328MW'
    new_act.save()

    new_ex = new_act.new_exchange(input=new_act.key, type='production', amount=1)
    new_ex.save()

    for material_info in input_data.values():
        material_act_name_location = material_info[0]
        input_amount = material_info[1]
        for act_name, location in material_act_name_location.items():
            if act_name == 'polyethylene production, low density, granulate':
                act = ws.get_one(bd.Database(bd_airborne_name),
                                 ws.equals('name', act_name),
                                 ws.equals('location', location),
                                 ws.contains('reference product', 'polyethylene'))
            elif act_name == 'plywood production':
                act = ws.get_one(bd.Database(bd_airborne_name),
                                 ws.equals('name', act_name),
                                 ws.equals('location', location),
                                 ws.equals('reference product', 'plywood'))
            else:
                act = ws.get_one(bd.Database(bd_airborne_name),
                                 ws.equals('name', act_name),
                                 ws.equals('location', location))

            new_ex = new_act.new_exchange(input=act, type='technosphere', amount=input_amount)
            new_ex.save()

    # add installation (0.48 m2 excavation for cabling)
    excavation = ws.get_one(
        bd.Database(bd_airborne_name),
        ws.equals('name', 'excavation, hydraulic digger'),
        ws.equals('location', 'RER'))
    new_ex = new_act.new_exchange(input=excavation, type='technosphere', amount=182 * 400 * 0.48)
    new_ex.save()

    # no eol

    # create maintenance (273000 kg lubricating oil, considering 3 changes, 521400 km by car for inspections)
    new_act = bd.Database('additional_acts').new_activity(
        name='airborne wind system, 328MW, maintenance', code='airborne wind system, 328MW, maintenance',
        location='RER', unit='unit',
        comment='Rigid. Yo-yo. Total power: 328 MW. Number of systems: 182. Rated power: 1.8MW. '
                'Annual electricity production: 6142 MWh/y.'
                'Lifetime: 20 years.  Based on Wilhelm, (2015).')
    new_act['reference product'] = 'airborne wind system, 328MW, maintenance'
    new_act.save()

    new_ex = new_act.new_exchange(input=new_act.key, type='production', amount=1)
    new_ex.save()

    car_transport = ws.get_one(
        bd.Database(bd_airborne_name),
        ws.equals('name', 'transport, passenger car'),
        ws.equals('location', 'RER'))
    new_ex = new_act.new_exchange(input=car_transport, type='technosphere', amount=521400)
    new_ex.save()
    oil = ws.get_one(
        bd.Database(bd_airborne_name),
        ws.equals('name', 'lubricating oil production'),
        ws.equals('location', 'RER'))
    new_ex = new_act.new_exchange(input=oil, type='technosphere', amount=273000)
    new_ex.save()


##### create fleets #####
# wind_onshore
def wind_onshore_fleet(db_wind_name: str, location: str,
                       fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                       ):
    """
    ´´fleet_turbines_definition´´ structure:
    {'turbine_1': [
    {
    'power': , 'manufacturer': , 'rotor_diameter': , 'hub_height': , 'commissioning_year': ,
    'generator_type': , 'recycled_share_steel': , 'lifetime': , 'eol_scenario':
    },
    0.5], # where this 0.5 is the share of turbine_1
    'turbine_2': [
    {
    'power': , 'manufacturer': , 'rotor_diameter': , 'hub_height': , 'commissioning_year': ,
    'generator_type': , 'recycled_share_steel': , 'lifetime': , 'eol_scenario':
    },
    0.5], # where this 0.5 is the share of turbine_2
    }
    """
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
        WindTrace.WindTrace_onshore.lci_wind_turbine(
            new_db=bd.Database('additional_acts'), cutoff391=bd.Database(db_wind_name),
            park_name=park_name, park_power=turbine_parameters['power'], number_of_turbines=1,
            park_location=location, park_coordinates=(51.181, 13.655),
            manufacturer=turbine_parameters['manufacturer'], rotor_diameter=turbine_parameters['rotor_diameter'],
            turbine_power=turbine_parameters['power'], hub_height=turbine_parameters['hub_height'],
            commissioning_year=turbine_parameters['commissioning_year'],
            generator_type=turbine_parameters['generator_type'],
            recycled_share_steel=turbine_parameters['recycled_share_steel'],
            lifetime=turbine_parameters['lifetime'], eol_scenario=turbine_parameters['eol_scenario']
        )
        maintenance_activity = bd.Database('additional_acts').get(park_name + '_maintenance')
        ex = list(maintenance_activity.upstream())
        for e in ex:
            e.delete()

    # create fleet activity
    fleet_activity = bd.Database('additional_acts').new_activity(
        name=f'onshore wind turbine fleet, 1 MW, for enbios, {location}',
        code=f'onshore wind turbine fleet, 1 MW, for enbios, {location}',
        unit='unit',
        location=location
    )
    fleet_activity['reference product'] = 'onshore wind turbine fleet, 1 MW'
    fleet_activity.save()
    new_ex = fleet_activity.new_exchange(input=fleet_activity.key, type='production', amount=1)
    new_ex.save()
    # add inputs
    for turbine, info in fleet_turbines_definition.items():
        share = info[1]
        turbine_parameters = info[0]
        park_name = f'{turbine}_{turbine_parameters["power"]}_{location}'
        single_turbine_activity = bd.Database('additional_acts').get(park_name + '_single_turbine')
        # to fleet activity (infrastructure)
        new_ex = fleet_activity.new_exchange(input=single_turbine_activity, type='technosphere',
                                             amount=share / turbine_parameters["power"])
        new_ex.save()

    # create wind fleet maintenance (per 1 MW)
    fleet_activity = bd.Database('additional_acts').new_activity(
        name=f'onshore wind turbine fleet, 1 MW, maintenance, for enbios, {location}',
        code=f'onshore wind turbine fleet, 1 MW, maintenance, for enbios, {location}',
        unit='unit',
        location=location
    )
    fleet_activity['reference product'] = 'onshore wind turbine fleet maintenance, 1 MW'
    fleet_activity.save()
    new_ex = fleet_activity.new_exchange(input=fleet_activity.key, type='production', amount=1)
    new_ex.save()
    # add inputs
    for turbine, info in fleet_turbines_definition.items():
        share = info[1]
        turbine_parameters = info[0]
        park_name = f'{turbine}_{turbine_parameters["power"]}_{location}'
        maintenance_activity = bd.Database('additional_acts').get(park_name + '_maintenance')
        # to fleet activity (infrastructure)
        new_ex = fleet_activity.new_exchange(input=maintenance_activity, type='technosphere',
                                             amount=share / turbine_parameters["power"])
        new_ex.save()

    return park_names


def wind_offshore_fleet(db_wind_name: str, location: str,
                        fleet_turbines_definition: Dict[str, List[Union[Dict[str, Any], float]]],
                        ):
    """
    ´´fleet_turbines_definition´´ structure:
    {'turbine_1': [
    {
    'power': , 'manufacturer': , 'rotor_diameter': , 'hub_height': , 'commissioning_year': ,
    'generator_type': , 'recycled_share_steel': , 'lifetime': , 'eol_scenario': , 'offshore_type': ,
    'floating_platform': , 'sea_depth': , 'distance_to_shore':
    },
    0.5], # where this 0.5 is the share of turbine_1
    'turbine_2': [
    {
    'power': , 'manufacturer': , 'rotor_diameter': , 'hub_height': , 'commissioning_year': ,
    'generator_type': , 'recycled_share_steel': , 'lifetime': , 'eol_scenario': , 'offshore_type': ,
    'floating_platform': , 'sea_depth': , 'distance_to_shore':
    },
    0.5], # where this 0.5 is the share of turbine_2
    }
    """
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
        WindTrace.WindTrace_offshore.lci_offshore_turbine(
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

    # create fleet activity
    fleet_activity = bd.Database('additional_acts').new_activity(
        name=f'offshore wind turbine fleet, 1 MW, for enbios, {location}',
        code=f'offshore wind turbine fleet, 1 MW, for enbios, {location}',
        unit='unit',
        location=location
    )
    fleet_activity['reference product'] = 'offshore wind turbine fleet, 1 MW'
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
        single_turbine_activity = bd.Database('additional_acts').get(park_name + '_offshore_turbine')
        # to fleet activity (infrastructure)
        new_ex = fleet_activity.new_exchange(input=single_turbine_activity, type='technosphere',
                                             amount=share / turbine_parameters["power"])
        new_ex.save()
        # delete maintenance
        maintenance_activity = bd.Database('additional_acts').get(park_name + '_offshore_maintenance')
        ex = list(maintenance_activity.upstream())
        for e in ex:
            e.delete()

    # create wind fleet maintenance (per 1 MW)
    fleet_activity = bd.Database('additional_acts').new_activity(
        name=f'offshore wind turbine fleet, 1 MW, maintenance, for enbios, {location}',
        code=f'offshore wind turbine fleet, 1 MW, maintenance, for enbios, {location}',
        unit='unit',
        location=location
    )
    fleet_activity['reference product'] = 'offshore wind turbine fleet maintenance, 1 MW'
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
        maintenance_activity = bd.Database('additional_acts').get(park_name + '_offshore_maintenance')
        # to fleet activity (infrastructure)
        new_ex = fleet_activity.new_exchange(input=maintenance_activity, type='technosphere',
                                             amount=share / turbine_parameters["power"])
        new_ex.save()

    return park_names


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
        battery_fleet_original = ws.get_one(bd.Database(db_batteries_name),
                                            ws.equals('name',
                                                      'market for battery capacity, stationary (CONT scenario)'))
        battery_fleet = battery_fleet_original.copy(database='additional_acts')
    elif scenario == 'tc':
        battery_fleet_original = ws.get_one(bd.Database(db_batteries_name),
                                            ws.equals('name', 'market for battery capacity, stationary (TC scenario)'))
        battery_fleet = battery_fleet_original.copy(database='additional_acts')
    else:
        print(f'Manual battery scenario with the following technology shares: {technology_share}')
        # Runtime check to enforce battery types as keys
        expected_keys = {'LFP', 'NMC111', 'NMC523', 'NMC622', 'NMC811', 'NMC955', 'SiB',
                         'Vanadium', 'lead', 'Sodium-Nickel'}
        if set(technology_share.keys()) != expected_keys:
            raise ValueError(f"'technology_share' must contain exactly the keys {expected_keys}")
        if sum(technology_share.values()) != 1:
            raise ValueError(f"'technology_share' shares must sum 1")

        # create manual battery scenario
        create_additional_acts_db()
        battery_original = ws.get_one(bd.Database(db_batteries_name),
                                      ws.equals('name', 'market for battery capacity, stationary (TC scenario)'))
        battery_fleet = battery_original.copy(database='additional_acts')
        battery_fleet['name'] = 'market for battery capacity, stationary (manual scenario), for enbios'
        battery_fleet.save()

        battery_type_to_exchange = {battery_type: ex for ex in battery_fleet.technosphere() for battery_type in
                                    technology_share.keys() if battery_type in ex.input['name']}

        for battery_type, share in technology_share.items():
            ex = battery_type_to_exchange.get(battery_type)
            if ex:
                ex._data['amount'] = share
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

    # create markets
    electrolysers_market_act = bd.Database('additional_acts').new_activity(
        name='electrolyser, fleet, 1 MWh/h, for enbios',
        code='electrolyser, fleet, 1 MWh/h, for enbios',
        location='RER',
        unit='kilowatt',
        comment=f'aec: {aec_share * 100}%, '
                f'pem: {pem_share * 100}%,'
                f'soec: {soec_share * 100}%'
    )
    electrolysers_market_act['reference product'] = 'electrolyser, fleet, 1 MWh/h'
    electrolysers_market_act.save()
    production_exchange = electrolysers_market_act.new_exchange(input=electrolysers_market_act.key, type='production',
                                                                amount=1)
    production_exchange.save()
    for electrolyser_type in ['AEC', 'SOEC', 'PEM']:
        # create individual hydrogen production activities per MWh/h of H2
        electrolyser_act = hydrogen_production_act_in_mwh_per_hour(electrolyser_name=electrolyser_type,
                                                                   db_hydrogen_name=db_hydrogen_name)
        if electrolyser_type == 'AEC':
            share = aec_share
        elif electrolyser_type == 'SOEC':
            share = soec_share
        else:
            share = pem_share
        new_ex = electrolysers_market_act.new_exchange(input=electrolyser_act, type='technosphere', amount=share)
        new_ex.save()
    hydrogen_production_update(db_hydrogen_production_name=db_hydrogen_name,
                               soec_share=soec_share, aec_share=aec_share, pem_share=pem_share)


def hydrogen_production_act_in_mwh_per_hour(electrolyser_name: str, db_hydrogen_name: str):
    """
    :return: it creates individual hydrogen production activities per MWh/h of H2 instead of per kg of H2, using LHV
    it also creates a fleet for hydrogen production per MWh/h
    """
    # electrolysers (infrastructure)
    electrolyser_act = bd.Database('additional_acts').new_activity(
        name=f'{electrolyser_name} electrolyser, production capacity 1 MWh/h',
        code=f'{electrolyser_name} electrolyser, production capacity 1 MWh/h',
        location='RER',
        unit='megawatt',
    )
    electrolyser_act['reference product'] = f'{electrolyser_name} electrolyser, production capacity 1 MWh/h'
    electrolyser_act.save()
    production_exchange = electrolyser_act.new_exchange(input=electrolyser_act.key, type='production',
                                                        amount=1)
    production_exchange.save()

    # add technosphere exchanges
    electrolysers_acts = ws.get_many(bd.Database(db_hydrogen_name),
                                     ws.contains('name', electrolyser_name),
                                     ws.contains('name', '1MWe'),
                                     ws.equals('location', 'RER')
                                     )
    lifetime_production = {'AEC': 3085961, 'SOEC': 3779894, 'PEM': 2964315}  # in kg, through the 20 years lifetime
    for act in electrolysers_acts:
        if 'treatment' not in act._data['name']:
            if 'Stack' in act._data['name']:
                new_ex = electrolyser_act.new_exchange(
                    input=act, type='technosphere',
                    amount=1 / ((33.3 / 8000 / 1000) * lifetime_production[electrolyser_name])  # in MWh/h
                )
                new_ex.save()
            else:
                new_ex = electrolyser_act.new_exchange(
                    input=act, type='technosphere',
                    amount=1 / ((33.3 / 8000 / 1000) * lifetime_production[electrolyser_name]) * 4
                )  # because BoP's lifetime is 4 times shorter than the total lifetime of the system
                new_ex.save()
        else:
            if 'stack' in act._data['name']:
                new_ex = electrolyser_act.new_exchange(
                    input=act, type='technosphere',
                    amount=-1 / ((33.3 / 8000 / 1000) * lifetime_production[electrolyser_name])
                )
                new_ex.save()
            else:
                new_ex = electrolyser_act.new_exchange(
                    input=act, type='technosphere',
                    amount=-1 / ((33.3 / 8000 / 1000) * lifetime_production[electrolyser_name]) * 4
                )
                new_ex.save()
    # add biosphere exchanges
    transformation_acts = ws.get_many(bd.Database('biosphere3'),
                                      ws.contains('name', 'Transformation,'),
                                      ws.contains('name', 'industrial area')
                                      )
    occupation_act = ws.get_one(bd.Database('biosphere3'),
                                ws.equals('name', 'Occupation, industrial area'))
    for act in transformation_acts:
        new_ex = electrolyser_act.new_exchange(input=act, type='biosphere', amount=1 / 120)
        new_ex.save()
    new_ex = electrolyser_act.new_exchange(
        input=occupation_act, type='biosphere',
        amount=1 / (120 * 27.5))  # although the original dataset assumes 20 years, PREMISE assumes 27.5
    new_ex.save()

    return electrolyser_act


def hydrogen_production_update(db_hydrogen_production_name: str, soec_share: float, aec_share: float, pem_share: float):
    """
    hydrogen production activity (in kg) with electrolyser fleet
    """
    # create copies of hydrogen production and eliminate infrastructure inputs
    create_additional_acts_db()
    tech_acts = {}
    for tech in ['AEC', 'SOEC', 'PEM']:
        hydrogen_act = ws.get_one(bd.Database(db_hydrogen_production_name),
                                  ws.contains('name', 'hydrogen production, gaseous'),
                                  ws.contains('name', tech),
                                  ws.exclude(ws.contains('name', 'steam')))
        new_act = hydrogen_act.copy(database='additional_acts')
        new_act.save()
        land_use = [e for e in new_act.biosphere() if
                    any(keyword in e.input._data['name'] for keyword in ['Transformation', 'Occupation'])]
        for e in land_use:
            e.delete()
        if tech == 'AEC':
            tech_acts[new_act] = aec_share
        elif tech == 'SOEC':
            tech_acts[new_act] = soec_share
        elif tech == 'PEM':
            tech_acts[new_act] = pem_share

    # create hydrogen production with fleet
    new_act = bd.Database('additional_acts').new_activity(
        name='hydrogen production, from electrolyser fleet, for enbios',
        code='hydrogen production, from electrolyser fleet, for enbios',
        location='RER',
        unit='kilogram',
        comment=f'aec: {aec_share * 100}%, '
                f'pem: {pem_share * 100}%,'
                f'soec: {soec_share * 100}%'
    )
    new_act['reference product'] = 'hydrogen, gaseous'
    new_act.save()

    new_ex = new_act.new_exchange(input=new_act.key, type='production', amount=1)
    new_ex.save()
    for act, share in tech_acts.items():
        new_ex = new_act.new_exchange(input=act, type='technosphere', amount=share)
        new_ex.save()


def collect_biosphere_flows(activity, scaling_factor=1, specific_inputs=None, current_tier=0, tier_limit=2):
    """
    Collects biosphere flows and biosphere exchanges up to a specified tier depth.
    Parameters:
    - activity: Brightway activity whose flows are analyzed.
    - scaling_factor: Float, used to scale amounts of flows.
    - specific_inputs: List of names of technosphere inputs to include.
    - current_tier: Int, depth of recursion.
    - tier_limit: Int, maximum depth for recursive search.
    Returns:
    - biosphere_flows: List of tuples, each containing (biosphere flow, scaled amount).
    - biosphere_exchanges: List of biosphere exchanges collected.
    """
    # Check if the current tier exceeds the limit
    if current_tier > tier_limit:
        return [], []
    biosphere_flows = []
    biosphere_exchanges = []
    for exc in activity.exchanges():
        if exc['type'] == 'biosphere':
            # Collect biosphere flow with scaling
            biosphere_flows.append((exc.input, exc.amount * scaling_factor))
            biosphere_exchanges.append(exc)
        elif specific_inputs and exc.input['name'] in specific_inputs and exc['type'] == 'technosphere':
            # If input is in specific_inputs and within tier limit, collect its contributions
            new_scaling_factor = exc.amount * scaling_factor
            nested_flows, nested_exchanges = collect_biosphere_flows(
                exc.input,
                new_scaling_factor,
                specific_inputs,
                current_tier + 1,
                tier_limit
            )
            biosphere_flows.extend(nested_flows)
            biosphere_exchanges.extend(nested_exchanges)
    return biosphere_flows, biosphere_exchanges


def aggregate_flows(biosphere_flows):
    # Use defaultdict to sum amounts for each unique biosphere flow
    aggregated_flows = defaultdict(float)
    for flow, amount in biosphere_flows:
        aggregated_flows[flow] += amount
    # Convert back to list of tuples for final result
    return list(aggregated_flows.items())


def aggregate_technosphere_inputs(activity, scaling_factor=1, specific_inputs=None, current_tier=0, tier_limit=2):
    """
    Aggregates technosphere flows up to a specified tier depth, ensuring all inputs
    from the deepest tier are included, and filtering intermediate tiers by `specific_inputs`.
    Some inputs (e.g., `always_include`) are added directly to the results if not in tier 0.
    Parameters:
    - activity: Brightway activity to analyze.
    - scaling_factor: Float, scales the amount of flows.
    - specific_inputs: List of names of technosphere inputs to explore specifically.
    - current_tier: Int, current depth of recursion.
    - tier_limit: Int, maximum depth for recursive search.
    Returns:
    - aggregated_flows: Dictionary with aggregated inputs and their scaled amounts.
    """
    if current_tier > tier_limit:
        return {}
    aggregated_flows = {}
    # List of inputs that must be directly included in the results
    always_include = [
        "hydrogen production, gaseous, 30 bar, from PEM electrolysis, from grid electricity",
        "market group for electricity, low voltage",
        "hydrogen production, gaseous, 25 bar, from gasification of woody biomass in entrained flow gasifier, "
        "at gasification plant"
    ]
    for exc in activity.exchanges():
        if exc['type'] == 'technosphere':
            # Scale the current input amount
            scaled_amount = exc.amount * scaling_factor
            if exc.input['name'] in always_include and current_tier != 0:
                # Add `always_include` inputs directly to results if not in tier 0
                if exc.input in aggregated_flows:
                    aggregated_flows[exc.input] += scaled_amount
                else:
                    aggregated_flows[exc.input] = scaled_amount
                continue  # Do not explore these inputs further
            if current_tier == tier_limit:
                # Add inputs directly at the last tier
                if exc.input in aggregated_flows:
                    aggregated_flows[exc.input] += scaled_amount
                else:
                    aggregated_flows[exc.input] = scaled_amount
            else:
                # Check if input should be included based on specific_inputs
                if specific_inputs and exc.input['name'] not in specific_inputs:
                    print(f"Skipping {exc.input['name']} at tier {current_tier}")
                    continue
                print(f"Exploring {exc.input['name']} at tier {current_tier}")
                # Recursively explore deeper tiers
                nested_flows = aggregate_technosphere_inputs(
                    exc.input,
                    scaling_factor=scaled_amount,
                    specific_inputs=specific_inputs,
                    current_tier=current_tier + 1,
                    tier_limit=tier_limit
                )
                # Merge nested flows into aggregated flows
                for key, value in nested_flows.items():
                    if key in aggregated_flows:
                        aggregated_flows[key] += value
                    else:
                        aggregated_flows[key] = value
    return aggregated_flows


def rebuild_acts(act, specific_inputs=None, tier_limit=3):
    """
    It reconstructs the diesel and kerosene acts, so it includes what previously was in all tiers of
    fuel production into tier 0.
    """
    # deal with the technosphere
    updated_act = act.copy(database='additional_acts')
    print(f'{updated_act["name"]}copy created in additional_acts')
    for ex in updated_act.technosphere():
        if ex.input['name'] == specific_inputs[0]:
            ex.delete()
            print(f'deleted {ex.input["name"]} input')
    aggregated_flows = aggregate_technosphere_inputs(
        activity=act, specific_inputs=specific_inputs, tier_limit=tier_limit)
    for activity, amount in aggregated_flows.items():
        new_ex = updated_act.new_exchange(input=activity, amount=amount, type='technosphere')
        new_ex.save()
        print(f'adding act: {activity}, amount: {amount}')
    # deal with the biosphere
    if act[
        'name'] == 'diesel production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, energy allocation, at fuelling station':
        print('dealing with the biosphere')
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=3,
            specific_inputs=[
                'diesel production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from wood gasification',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from wood gasification'
            ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        print('biosphere deleted')
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()
            print(f'adding flow: {flow[0]["name"]}, amount: {flow[1]}')
    elif act[
        'name'] == 'diesel production, synthetic, from Fischer Tropsch process, hydrogen from electrolysis, energy allocation, at fuelling station':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=1,
            specific_inputs=[
                'diesel production, synthetic, Fischer Tropsch process, hydrogen from electrolysis, energy allocation',
            ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()
    elif act[
        'name'] == 'kerosene production, synthetic, from Fischer Tropsch process, hydrogen from wood gasification, energy allocation, at fuelling station':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=3,
            specific_inputs=[
                'kerosene production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from wood gasification',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from wood gasification'
            ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()
    elif act[
        'name'] == 'kerosene production, synthetic, from Fischer Tropsch process, hydrogen from electrolysis, energy allocation, at fuelling station':
        bioflows, bioexchanges = collect_biosphere_flows(
            activity=act,
            tier_limit=1,
            specific_inputs=[
                'kerosene production, synthetic, Fischer Tropsch process, hydrogen from electrolysis, energy allocation',
            ]
        )
        bioflows_gruped = aggregate_flows(bioflows)
        updated_act.biosphere().delete()
        for flow in bioflows_gruped:
            new_ex = updated_act.new_exchange(input=flow[0], type='biosphere', amount=flow[1])
            new_ex.save()


def rebuild_kerosene_and_diesel_acts():
    """
    diesel and kerosene at fuelling station is produced from hydrogen, which is synthesised either via wood gasification
    or electrolysis. The process involves several steps, separated into different activities in premise_base. These
    include carbon monoxide production, followed by syngas production, followed by kerosene production and finally
    kerosene at fuelling station. This function puts all the value chain together (both technosphere and biosphere) in
    one single activity, which will be located in additional_acts.
    """
    diesel_wood = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'diesel production, synthetic, from Fischer Tropsch process, '
                          'hydrogen from wood gasification, energy allocation, at fuelling station')
    )
    diesel_electrolysis = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'diesel production, synthetic, from Fischer Tropsch process, '
                          'hydrogen from electrolysis, energy allocation, at fuelling station')
    )
    kerosene_wood = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'kerosene production, synthetic, from Fischer Tropsch process, '
                          'hydrogen from wood gasification, energy allocation, at fuelling station')
    )
    kerosene_electrolysis = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'kerosene production, synthetic, from Fischer Tropsch process, '
                          'hydrogen from electrolysis, energy allocation, at fuelling station')
    )
    for act in [diesel_wood, diesel_electrolysis, kerosene_wood, kerosene_electrolysis]:
        if act['name'] == ('diesel production, synthetic, from Fischer Tropsch process, '
                           'hydrogen from wood gasification, energy allocation, at fuelling station'):
            rebuild_acts(act, [
                'diesel production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from wood gasification',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from wood gasification'
            ])
        elif act['name'] == ('diesel production, synthetic, from Fischer Tropsch process, '
                             'hydrogen from electrolysis, energy allocation, at fuelling station'):
            rebuild_acts(act, [
                'diesel production, synthetic, Fischer Tropsch process, hydrogen from electrolysis, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from electrolysis',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from electrolysis'
            ])
        elif act['name'] == ('kerosene production, synthetic, from Fischer Tropsch process, '
                             'hydrogen from wood gasification, energy allocation, at fuelling station'):
            rebuild_acts(act, [
                'kerosene production, synthetic, Fischer Tropsch process, hydrogen from wood gasification, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from wood gasification',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from wood gasification'
            ])
        elif act['name'] == ('kerosene production, synthetic, from Fischer Tropsch process, '
                             'hydrogen from electrolysis, energy allocation, at fuelling station'):
            rebuild_acts(act, [
                'kerosene production, synthetic, Fischer Tropsch process, hydrogen from electrolysis, energy allocation',
                'syngas, RWGS, Production, for Fischer Tropsch process, hydrogen from electrolysis',
                'carbon monoxide, from RWGS, for Fischer Tropsch process, hydrogen from electrolysis'
            ])


def rebuild_methanol_act():
    """
    methanol production in premise_base consists of methanol synthesis (which produces unpurified methanol), followed by
    methanol synthesis, which purifies this methanol. This function puts these to activities together in a single one
    and leaves it in 'additional_acts'.
    """
    inner_technosphere = []
    methanol_act = ws.get_one(bd.Database('additional_acts'),
                              ws.equals('name', 'methanol distillation, from wood, without CCS'))
    for ex in methanol_act.technosphere():
        if ex.input['name'] == 'methanol synthesis, from wood, without CCS':
            inner_technosphere = ex.input.technosphere()
            ex.delete()
    if inner_technosphere:
        for ex in inner_technosphere:
            ex.output = methanol_act
            ex.save()
    methanol_act_electrolysis = ws.get_one(
        bd.Database('premise_base'),
        ws.equals('name', 'methanol distillation, hydrogen from electrolysis, CO2 from DAC'))
    methanol_act_2 = methanol_act_electrolysis.copy(database='additional_acts')
    for ex in methanol_act_2.technosphere():
        if ex.input['name'] == 'methanol synthesis, hydrogen from electrolysis, CO2 from DAC':
            inner_technosphere = ex.input.technosphere()
            ex.delete()
    if inner_technosphere:
        for ex in inner_technosphere:
            ex.output = methanol_act_2
            ex.save()


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
