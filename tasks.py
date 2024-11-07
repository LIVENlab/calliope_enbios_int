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


# TODO:
#  1. Formalise general workflow
#  2. Background
#  3. Because infrastructure is mainly GLOBAL, we cannot relink much of the European steel and iron. We need to do it
#  AFTER the forground has been modified: 1. Create a copy of each infrastructure in technology_mapping, 2. Change
#  location to RER, 3. Do the same process as in steel_update, 4. apply special function to wind infrastructure.

#### BACKGROUND #####
# 1.1. Unlink carrier activities
# 1.1.1 Electricity
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


# 1.1.2 Heat
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
        for act in heat_production_acts:
            for e in act.upstream():
                e.delete()


# 1.1.3 CO2
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


# 1.1.4 Hydrogen
def unlink_hydrogen(db_name: str = 'premise_base'):
    """
    It deletes input for syngas, carbon monoxide production, methanol production, kerosene, diesel, gasoline, and
    ammonia production
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


# 1.1.5 Waste
# In cutoff it comes without any environmental burdens, so there is no need to apply any unlinks

# 1.1.6 Biomass
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


# 1.1.7 Methane
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


# 1.1.8 Methanol
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


# 1.1.9 Kerosene
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


# 1.1.10
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
    NOTE: premise_cement database is a premise database (imagage_rcp19, 2050) with ndb.update('cement'), copy
    """
    cement_ccs_original = ws.get_one(
        bd.Database('premise_cement'),
        ws.equals('name', 'clinker production, efficient, with on-site CCS'),
        ws.equals('location', 'WEU')
    )
    # create a copy of the clinker production with CCS
    cement_ccs = cement_ccs_original.copy(database='premise_base')
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
    # delete premise_cement, as we won't need it anymore
    del bd.databases['premise_cement']
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
                    'Outputs (propylene: 46.4 MJ7kg, ethylene: 47.2 MJ/kg, butane: 45.7 MJ/kg)'
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
                         ws.startswith('name', f'market for {chemical}'),
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
    """
    ammonia_act = ws.get_one(bd.Database('premise_base'),
                             ws.equals('name', 'market for ammonia, anhydrous, liquid'),
                             ws.equals('location', 'RER'))
    for ex in ammonia_act.upstream():
        ex.input = ws.get_one(bd.Database('premise_base'),
                              ws.equals('name', 'ammonia production, hydrogen from electrolysis'))
        ex.save()


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


def deletable_exchanges(act):
    """
    finds infrastructure, electricity, and heat inputs and deletes them.
    """
    infrastructure = [e for e in act.technosphere() if e.input._data['unit'] == 'unit']
    print(infrastructure)
    electricity = [e for e in act.technosphere() if
                   ('market for electricity' in e.input._data['name'] and e.input._data['unit'] == 'kilowatt hour')
                   or ('market group for electricity' in e.input._data['name'] and e.input._data[
                       'unit'] == 'kilowatt hour')]
    print(electricity)
    heat = [e for e in act.technosphere() if
            ('market for electricity' in e.input._data['name'] and e.input._data['unit'] == 'megajoule')
            or ('market group for heat' in e.input._data['name'] and e.input._data['unit'] == 'megajoule')]
    print(heat)

    return infrastructure, electricity, heat


def delete_methanol_infrastructure(biosphere_technosphere_disaggregation: bool):
    """
        It creates a copy of the methanol syntheses activities and deletes its infrastructure (tier 1 and 2)
    """
    create_additional_acts_db()
    is_in_additional_acts = ws.get_many(bd.Database('additional_acts'),
                                        ws.equals('name',
                                                  'methanol distillation, hydrogen from electrolysis, CO2 from DAC'))
    if len(list(is_in_additional_acts)) == 0:
        from_wood_act = ws.get_one(bd.Database('additional_acts'),
                                   ws.equals('name', 'methanol distillation, from wood, without CCS')
                                   )
        original_from_hydrogen_act = ws.get_one(
            bd.Database('premise_base'),
            ws.equals('name', 'methanol distillation, hydrogen from electrolysis, CO2 from DAC')
        )
        from_hydrogen_act = original_from_hydrogen_act.copy(database='additional_acts')
        for act in [from_wood_act, from_hydrogen_act]:
            infrastructure_ex = [e for e in act.technosphere() if e.input._data['unit'] == 'unit']
            methanol_synthesis_act = \
                [e.input for e in act.technosphere() if e.input._data['reference product'] == 'methanol, unpurified'][0]
            methanol_synthesis_ex = [e for e in methanol_synthesis_act.technosphere() if
                                     e.input._data['unit'] == 'unit']
            for e in infrastructure_ex:
                e.delete()
            for e in methanol_synthesis_ex:
                e.delete()
            if biosphere_technosphere_disaggregation:
                technosphere_biosphere_disaggregation(activity=act)


def delete_kerosene_infrastructure(biosphere_technosphere_disaggregation: bool):
    """
    It creates a copy of the kerosene production and diesel production activities and deletes
    its infrastructure (tier 2)
    """
    create_additional_acts_db()
    kerosene_already_in_additional_acts = ws.get_many(
        bd.Database('additional_acts'),
        ws.contains('name',
                    'kerosene production, synthetic, from Fischer Tropsch process, '),
        ws.contains('name', 'energy allocation'),
        ws.exclude(ws.contains('name', 'coal')),
        ws.exclude(ws.contains('name', 'CCS')))
    if len(list(kerosene_already_in_additional_acts)) == 0:
        original_kerosene = ws.get_many(bd.Database('premise_base'),
                                        ws.contains('name',
                                                    'kerosene production, synthetic, from Fischer Tropsch process, '),
                                        ws.contains('name', 'energy allocation'),
                                        ws.exclude(ws.contains('name', 'coal')),
                                        ws.exclude(ws.contains('name', 'CCS')))
        for act in original_kerosene:
            kerosene = act.copy(database='additional_acts')
            kerosene_synthetic_act = \
                [e.input for e in kerosene.technosphere() if
                 e.input._data['reference product'] == 'kerosene, synthetic'][0]
            infrastructure_ex = [e for e in kerosene_synthetic_act.technosphere() if e.input._data['unit'] == 'unit']
            for e in infrastructure_ex:
                e.delete()
            if biosphere_technosphere_disaggregation:
                technosphere_biosphere_disaggregation(activity=act)


def delete_diesel_infrastructure(biosphere_technosphere_disaggregation: bool):
    """
    It creates a copy of the kerosene production and diesel production activities and deletes
    its infrastructure (tier 2)
    """
    create_additional_acts_db()
    # check if we already created the copies in 'additional_acts'
    diesel_already_in_additional_acts = ws.get_many(
        bd.Database('additional_acts'),
        ws.contains('name',
                    'diesel production, synthetic, from Fischer Tropsch process, '),
        ws.contains('name', 'energy allocation'),
        ws.exclude(ws.contains('name', 'coal')),
        ws.exclude(ws.contains('name', 'CCS')))
    if len(list(diesel_already_in_additional_acts)) == 0:
        original_diesel = ws.get_many(bd.Database('premise_base'),
                                      ws.contains('name',
                                                  'diesel production, synthetic, from Fischer Tropsch process, '),
                                      ws.contains('name', 'energy allocation'),
                                      ws.exclude(ws.contains('name', 'coal')),
                                      ws.exclude(ws.contains('name', 'CCS')))

        for act in original_diesel:
            diesel = act.copy(database='additional_acts')
            diesel_synthetic_act = \
                [e.input for e in diesel.technosphere() if e.input._data['reference product'] == 'diesel, synthetic'][0]
            infrastructure_ex = [e for e in diesel_synthetic_act.technosphere() if e.input._data['unit'] == 'unit']
            for e in infrastructure_ex:
                e.delete()
            if biosphere_technosphere_disaggregation:
                technosphere_biosphere_disaggregation(activity=act)


def delete_infrastructure_main(
        file_path: str = r'C:\Users\mique\OneDrive - UAB\PhD_ICTA_Miquel\research stay Delft\technology_mapping_clean.xlsx',
        biosphere_technosphere_disaggregation: bool = True
):
    """
    It takes all the activities in 'technology_map_clean.xlsx', finds the exact activity
    (or activities in plural if it has multiple options, i.e., those activities that exist for all
    locations in Calliope), creates a copy of them in 'additional_acts'
    (only if the activities were not already in 'additional_acts'), and deletes the inputs that are infrastructure,
    heat or electricity to avoid double accounting.
    """
    # delete infrastructure
    df = pd.read_excel(file_path, sheet_name='Foreground')
    for name, location, database, reference_product in (
            zip(df['LCI_operation_and_maintenance'], df['prod_location'], df['initial_database'],
                df['reference product'])):
        print('NEXT ACTIVITY')
        # Skip if any of the following conditions are met
        if name == '-' or name == 'No activity found' or location == '-' or location == 'FR, DE':
            continue
        print(f'Name: {name}')
        print(f'Location: {location}')
        print(f'Database: {database}')
        print(f'Reference product: {reference_product}')
        # Adjust the database name if needed
        if database == 'Ecoinvent':
            database = 'cutoff391'

        # delete infrastructure for methanol, diesel and kerosene production (special cases)
        if 'methanol distillation' in name:
            delete_methanol_infrastructure(biosphere_technosphere_disaggregation)
            continue
        if 'diesel production' in name:
            delete_diesel_infrastructure(biosphere_technosphere_disaggregation)
            continue
        if 'kerosene production' in name:
            delete_kerosene_infrastructure(biosphere_technosphere_disaggregation)
            continue

        # If the location is 'country', start checking for activities
        if location == 'country':
            for loc in consts.LOCATION_EQUIVALENCE.values():
                found_activity = False
                try:
                    act = ws.get_one(bd.Database(database), ws.contains('name', name),
                                     ws.exclude(ws.contains('name', 'renewable energy products')),
                                     ws.equals('location', loc),
                                     ws.contains('reference product', reference_product))
                    if database != 'additional_acts':
                        try:
                            new_act = act.copy(database='additional_acts')
                            infrastructure, electricity, heat = deletable_exchanges(new_act)
                            for e in infrastructure:
                                e.delete()
                            for e in electricity:
                                e.delete()
                            for e in heat:
                                e.delete()
                            technosphere_biosphere_disaggregation(new_act)
                        # if the act has already been copied to the database
                        except Exception as e:
                            print(f"The was previously copied in the 'additional_acts' database: {act._data['name']}")
                    else:
                        infrastructure, electricity, heat = deletable_exchanges(new_act)
                        for e in infrastructure:
                            e.delete()
                        for e in electricity:
                            e.delete()
                        for e in heat:
                            e.delete()
                        technosphere_biosphere_disaggregation(new_act)
                    print(f'Activity: {name}. Location: {loc}. Ref product: {reference_product}')
                    found_activity = True
                    continue
                except Exception as e:
                    print(f'No activity ({name}) in ({loc}). '
                          f'Starting ["CH", "FR", "DE"]')

                # If no activity found in the equivalence list, check fallback locations
                for fallback_loc in ['CH', 'FR', 'DE']:
                    try:
                        # Try to find activities in the fallback location
                        act = ws.get_one(bd.Database(database), ws.contains('name', name),
                                         ws.exclude(ws.contains('name', 'renewable energy products')),
                                         ws.equals('location', fallback_loc),
                                         ws.contains('reference product', reference_product))
                        if act:  # If any activities are found in the fallback location
                            if database != 'additional_acts':
                                try:
                                    new_act = act.copy(database='additional_acts')
                                    infrastructure, electricity, heat = deletable_exchanges(new_act)
                                    for e in infrastructure:
                                        e.delete()
                                    for e in electricity:
                                        e.delete()
                                    for e in heat:
                                        e.delete()
                                    technosphere_biosphere_disaggregation(new_act)
                                # if the act has already been copied to the database
                                except Exception as e:
                                    print(act._data['name'], e)
                            else:
                                infrastructure, electricity, heat = deletable_exchanges(new_act)
                                for e in infrastructure:
                                    e.delete()
                                for e in electricity:
                                    e.delete()
                                for e in heat:
                                    e.delete()
                                technosphere_biosphere_disaggregation(new_act)
                            print(f'Activity: {name}. Location: {fallback_loc}. Ref product: {reference_product}. ')
                            found_activity = True
                            break
                    except Exception as e:
                        print(f'No activity ({name}) in ({fallback_loc}).')
                if not found_activity:
                    print(f'No activity found for {name}. Quitting')
                    sys.exit()

                # Continue the outer loop as soon as we find a valid activity
                if found_activity:
                    continue
        else:
            # If location is not 'country', proceed with regular activity lookup
            try:
                act = ws.get_one(bd.Database(database), ws.contains('name', name),
                                 ws.exclude(ws.contains('name', 'renewable energy products')),
                                 ws.equals('location', location))
                if database != 'additional_acts':
                    try:
                        new_act = act.copy(database='additional_acts')
                        infrastructure, electricity, heat = deletable_exchanges(new_act)
                        for e in infrastructure:
                            e.delete()
                        for e in electricity:
                            e.delete()
                        for e in heat:
                            e.delete()
                        technosphere_biosphere_disaggregation(new_act)
                    # if the act has already been copied to the database
                    except Exception as e:
                        print(act._data['name'], e)
                else:
                    infrastructure, electricity, heat = deletable_exchanges(new_act)
                    for e in infrastructure:
                        e.delete()
                    for e in electricity:
                        e.delete()
                    for e in heat:
                        e.delete()
                    technosphere_biosphere_disaggregation(act)
                print(f'Activity found for {name} in location: {location}')
            except Exception as e:
                print(f'No activity ({name}) in location: {location}.')


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
    # bus
    bus_original = ws.get_one(bd.Database(db_truck_name),
                              ws.equals('name',
                                        'passenger bus, battery electric - opportunity charging, LTO battery, '
                                        '13m single deck urban bus'),
                              )
    bus_act = bus_original.copy(database='additional_acts')
    bus_technosphere = list(bus_act.technosphere())
    keep_inputs = ['converter', 'inverter', 'power electronics', 'other components', 'electric motor',
                   'electricity storage capacity', 'power distribution']
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
    electricity_reservoir = ws.get_many(
        bd.Database(db_hydro_name),
        ws.contains('name', 'electricity production, hydro, reservoir'),
        ws.equals('location', location)
    )
    create_additional_acts_db()
    for act in electricity_reservoir:
        new_elec_act = act.copy(database='additional_acts')
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
        technosphere_biosphere_disaggregation(new_elec_act)


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
        electricity = [e for e in new_act.technosphere() if 'electricity, low voltage' in e.input._data['name']]
        infrastructure = [e for e in new_act.technosphere() if e.input._data['unit'] == 'unit']
        for e in land_use:
            e.delete()
        for e in electricity:
            e.delete()
        for e in infrastructure:
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


def technosphere_biosphere_disaggregation(activity):
    create_additional_acts_db()
    for sphere in ['biosphere', 'technosphere']:
        # biosphere
        act = activity.copy(database='additional_acts')
        if sphere == 'biosphere':
            act.technosphere().delete()
        else:
            act.biosphere().delete()
        act['name'] = f'{act["name"]}, {sphere}'
        act['reference product'] = f'{act["reference product"]}, {sphere}'
        act.save()


##### substitute and unlink #####
# Note: eliminate downstream connections of all hydrogen activities (not only those from electrolysers)
# (biofuel_to_liquids).
# Note: maybe relink_technosphere_exchanges() from wurst?


##### materials as indicator #####
# 1. foreground
def foreground_materials(act):
    # TODO:
    # 1. CRMs; 2. list of heavily used materials: steel, iron, copper, aluminium, cement, concrete, water, plastics;
    # 3. group materials per ISIC classification.
    metals_cpc = ['41114: Ferro-nickel', '41422: Unwrought nickel',
                  '41310: Silver (including silver plated with gold or platinum), unwrought or in semi-manufactured forms, or in powder […]',
                  '41601: Tungsten, molybdenum, tantalum, magnesium, cobalt, cadmium, titanium, zirconium, beryllium, gallium, hafnium, […]',
                  '41320: Gold (including gold plated with platinum), unwrought or in semi-manufactured forms, or in powder form',
                  '41116: Ferrous products obtained by direct reduction of iron ore and other spongy ferrous products, in lumps, pellets[…]',
                  '41603: Bismuth, antimony, manganese, chromium and articles thereof; including waste and scrap of bismuth or manganese',
                  '412: Products of iron or steel',
                  '4160: Other non-ferrous metals and articles thereof (including waste and scrap of some metals); cermets and articles […]',
                  '41122: Alloy steel in ingots or other primary forms and semi-finished products of alloy steel',
                  '41115: Other ferro-alloys',
                  '41431: Unwrought aluminium',
                  '41413: Refined copper and copper alloys, unwrought; master alloys of copper',
                  '41330: Platinum, unwrought or in semi-manufactured forms, or in powder form',
                  '41432: Alumina (aluminium oxide), except artificial corundum',
                  '41: Basic metals',
                  '41513: Wire of copper',
                  '41111: Pig iron and spiegeleisen in pigs, blocks or other primary forms',
                  '4153: Semi-finished products of aluminium or aluminium alloys',
                  '41443: Tin, unwrought',
                  '4151: Semi-finished products of copper or copper alloys',
                  '4128: Tubes, pipes and hollow profiles, of steel',
                  '413: Basic precious metals and metals clad with precious metals',
                  '41412: Unrefined copper; copper anodes for electrolytic refining',
                  '41112: Ferro-manganese',
                  '4111: Primary materials of the iron and steel industry',
                  '41121: Non-alloy steel in ingots or other primary forms, and semi-finished products of non-alloy steel',
                  '4132: Gold (including gold plated with platinum), unwrought or in semi-manufactured forms, or in powder form',
                  '41411: Copper mattes; cement copper',
                  '41113: Ferro-chromium',
                  '41117: Granules and powders, of pig iron and spiegeleisen, or steel',
                  '4124: Bars and rods, hot-rolled, of iron or steel',
                  '41442: Zinc, unwrought',
                  '4121: Flat-rolled products of steel, not further worked than hot-rolled',
                  '41441: Lead, unwrought',
                  '41531: Powders and flakes of aluminium',
                  '4133: Platinum, unwrought or in semi-manufactured forms, or in powder form']
    cement_cpc = ['374: Plaster, lime and cement',
                  '37440: Portland cement, aluminous cement, slag cement and similar hydraulic cements, except in the form of clinkers',
                  '37410: Plasters', '37430: Cement clinkers', '3741: Plasters',
                  '37420: Quicklime, slaked lime and hydraulic lime',
                  '3744: Portland cement, aluminous cement, slag cement and similar hydraulic cements, except in the form of clinkers']
    concrete_cpc = ['37510: Non-refractory mortars and concretes',
                    '37530: Articles of plaster or of compositions based on plaster',
                    '37540: Tiles, flagstones, bricks and similar articles, of cement, concrete or artificial stone',
                    '37570: Articles of asbestos-cement, cellulose fibre-cement or the like',
                    '375: Articles of concrete, cement and plaster']
    plastics_cpc = ['347: Plastics in primary forms', '34710: Polymers of ethylene, in primary forms',
                    '34740: Polyacetals, other polyethers and epoxide resins, in primary forms; polycarbonates, alkyd resins, polyallyl es[…]',
                    '34730: Polymers of vinyl chloride or other halogenated olefins, in primary forms',
                    '34790: Other plastics in primary forms; ion exchangers',
                    '34720: Polymers of styrene, in primary forms']
    metals = []
    cement = []
    concrete = []
    plastics = []
    for ex in act.technosphere():
        try:
            for classification in ex.input['classifications']:
                if classification[0] == 'CPC':
                    if classification[1] in metals_cpc:
                        metals.append(ex['amount'])
                    elif classification[1] in cement_cpc:
                        cement.append(ex['amount'])
                    elif classification[1] in concrete_cpc:
                        print(ex.input['name'], 'concrete')
                        concrete.append(ex['amount'])
                    elif classification[1] in plastics_cpc:
                        plastics.append(ex['amount'])
        except Exception as e:
            pass
    return sum(metals), sum(cement), sum(concrete), sum(plastics)


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
