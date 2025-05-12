import bw2io as bi
import wurst.errors
from premise import *
from config_parameters import *
from functions import *
import bw2data as bd


def main(ccs: bool = False, vehicles_as_batteries: bool = True,
         soec_electrolyser_share: float = 0.3, aec_electrolyser_share: float = 0.4,
         pem_electrolyser_share: float = 0.3,  # electrolyser variables
         battery_current_share: bool = False,
         battery_technology_share: Optional[Dict[str, float]] = config_parameters.EMERGING_TECH_MODERATE,
         # battery variables
         open_technology_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND['openground'],
         roof_technology_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_power_share"],
         roof_3kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_3kw"],
         roof_93kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_93kw"],
         roof_156kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_156kw"],
         roof_280kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_280kw"],
         # solar pv variables
         onshore_wind_fleet: Dict = config_parameters.BALANCED_ON_WIND_FLEET,  # onshore wind variables
         offshore_wind_fleet: Dict = config_parameters.OFF_WIND_FLEET,  # offshore wind variables

         infrastructure_production_in_europe: bool = True,
         mapping_file_path: str = r'C:\Users\1361185\OneDrive - UAB\Documentos\GitHub\calliope_enbios_int\data\tech_mapping.xlsx'
         ):
    """
    Databases:
    1. 'original_cutoff391'
    2. 'apos391'
    3. 'premise_original'
    4. 'premise_base': will contain the updated background and foreground. Infrastructure deletion,
    and fuel input deletion (double accounting avoided).
    5. 'additional_acts':
    6. Auxiliary databases:
        - 'premise_auxiliary_for_infrastructure' will be the base for 'infrastructure (with European steel and concrete)'
    7. Final databases (all of them come WITH background changes):
        - 'infrastructure (with European steel and concrete)': infrastructure activities WITH European markets for
          steel and concrete.
    # TODO: maybe after all the treatments, we can create a database 'O&M biosphere' with all the corresponding activities. Same for 'O&M technosphere'. In this way, we would have all needed activities well classified in three separated databases.
    """
    # setup_databases
    bd.projects.set_current(PROJECT_NAME)
    bi.bw2setup()

    # Ecoinvent v3.9.1 cutoff and apos
    if 'original_cutoff391' not in bd.databases:
        ei = bi.SingleOutputEcospold2Importer(SPOLDS_CUTOFF, "original_cutoff391", use_mp=False)
        ei.apply_strategies()
        ei.write_database()
    if 'apos391' not in bd.databases:
        ei = bi.SingleOutputEcospold2Importer(SPOLDS_APOS, "apos391", use_mp=False)
        ei.apply_strategies()
        ei.write_database()

    # TODO: fix premise version so it does not break
    # premise, without updates (only imported inventories)
    ndb = NewDatabase(
        scenarios=[
            {"model": "image", "pathway": "SSP2-RCP19", "year": 2020},
        ],
        source_db="cutoff391",
        source_version="3.9.1",
        key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo='
    )
    ndb.write_db_to_brightway(name='premise_original')

    # premise with cement and biomass update
    ndb = NewDatabase(
        scenarios=[
            {"model": "image", "pathway": "SSP2-RCP19", "year": 2020},
        ],
        source_db="cutoff391",
        source_version="3.9.1",
        key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo='
    )
    ndb.update('cement')
    ndb.update('biomass')
    ndb.write_db_to_brightway(name='premise_cement')

    # create premise_original copy named 'premise_base)
    bd.Database('premise_original').copy(name="premise_base")

    # background changes
    update_background()
    # TODO: allow to have shares of today's and future's industry
    # TODO: allow the rest of the world to also update their industries (according to IAMs?)

    # create a copy for each of the databases that we will have in the project.
    premise_base_auxiliary()

    # foreground changes
    update_foreground(ccs=ccs, vehicles_as_batteries=vehicles_as_batteries,
                      soec_electrolyser_share=soec_electrolyser_share, aec_electrolyser_share=aec_electrolyser_share,
                      pem_electrolyser_share=pem_electrolyser_share,
                      battery_current_share=battery_current_share,
                      battery_technology_share=battery_technology_share,
                      open_technology_share=open_technology_share,
                      roof_technology_share=roof_technology_share,
                      roof_3kw_share=roof_3kw_share,
                      roof_93kw_share=roof_93kw_share,
                      roof_156kw_share=roof_156kw_share,
                      roof_280kw_share=roof_280kw_share,
                      onshore_wind_fleet=onshore_wind_fleet,
                      offshore_wind_fleet=offshore_wind_fleet)
    # TODO: continue with the next function
    # TODO: add variables in install_and_update_databases() so we can select if we want to
    #  update cement and iron foreground, delete infrastructure, avoid double accounting, etc.

    # 'infrastructure (with European steel and concrete)' operating.
    if infrastructure_production_in_europe:
        update_cement_iron_foreground(file_path=mapping_file_path)

    # O&M activities in premise_base and additional_acts do not have infrastructure inputs after running this function.
    # Moreover, now we have activities (with ', biosphere' and ', technosphere' at the end of the name indicated in the
    # mapping file) in additional_acts.
    delete_infrastructure_main(
        file_path=r'C:\Users\mique\OneDrive - UAB\PhD_ICTA_Miquel\research stay Delft\technology_mapping_clean.xlsx')

    # avoid double accounting
    avoid_double_accounting()


# 1. set the background
# 1.1. Unlink carrier activities
def avoid_double_accounting():
    """
    We use the polluter pays principle to avoid double accounting. There are two possible sources of double accounting.
    Let's break them down using electricity production as an example:
    1. Calliope calculates the demand for electricity in Europe. We will calculate the impacts of the technologies that
    produce electricity to satisfy this demand. Thus, when calculating the impacts of other technologies within the
    electricity system (e.g., electricity used in electrolysers for hydrogen production), we should not count the
    impacts of this electricity again. (delete internal links)
    2. Calliope assumes electricity is produced with certain technologies. However, in Ecoinvent, other technologies
    might be producing electricity in the background (e.g., coal is not used in Calliope, but is in the background of
    Ecoinvent). Thus, they should not be accounted either. (delete links from shifted demand from Ecoinvent to Calliope)
    """
    for name in ['premise_base', 'additional_acts',
                 'premise_auxiliary_for_infrastructure', 'infrastructure (with European steel and concrete)']:
        try:
            # 1.1.1 Electricity
            unlink_electricity(db_name=name)
        except wurst.errors.NoResults:
            print(f'electricity not available in {name}')
        try:
            # 1.1.2 Heat
            unlink_heat(db_name=name)
        except wurst.errors.NoResults:
            print(f'heat not available in {name}')
        try:
            # 1.1.3 CO2
            unlink_co2(db_name=name)
        except wurst.errors.NoResults:
            print(f'co2 not available in {name}')
        try:
            # 1.1.4 Hydrogen
            unlink_hydrogen(db_name=name)
        except wurst.errors.NoResults:
            print(f'hydrogen not available in {name}')
        # 1.1.5 Waste
        # In cutoff it comes without any environmental burdens, so there is no need to apply any unlinks
        try:
            # 1.1.6 Biomass
            unlink_biomass(db_name=name)
        except wurst.errors.NoResults:
            print(f'biomass not available in {name}')
        try:
            # 1.1.7 Methane
            unlink_methane(db_name=name)
        except wurst.errors.NoResults:
            print(f'methane not available in {name}')
        try:
            # 1.1.8 Methanol
            unlink_methanol(db_name=name)
        except wurst.errors.NoResults:
            print(f'methanol not available in {name}')
        try:
            # 1.1.9 Kerosene
            unlink_kerosene(db_name=name)
        except wurst.errors.NoResults:
            print(f'kerosene not available in {name}')
        try:
            # 1.1.10 Diesel
            unlink_diesel(db_name=name)
        except wurst.errors.NoResults:
            print(f'diesel not available in {name}')


def update_background(
        ccs_clinker: bool = True,
        train_electrification: bool = True,
        biomass_from_residues: bool = True,
        biomass_from_residues_share: float = 1.0,
        h2_iron_and_steel: bool = True,
        olefins_from_methanol: bool = True,
        methanol_from_electrolysis: bool = True,
        ammonia_from_hydrogen: bool = True,
        trucks_electrification: bool = True,
        trucks_electrification_share: float = 0.5,
        sea_transport_syn_diesel: bool = True
):
    """
    This function allows to adapt certain industries according to Calliope's assumptions:
    - Cement: clinker production with Carbon Capture and Storage
    - Train: 100% electrification
    - Biomass: specified share (biomass_from_residues_share) coming from residues
    - Iron and steel: from hydrogen - direct reduction iron - 50% electric arc furnace synthetic route
    - Plastics: olefins produced by methanol. Methanol from H2 and CO2 (from Direct Air Capture)
    - Methanol: Feedstock methanol from electrolysis (Note: aromatics follow today synthetic route due to lack of data.
                Calliope's assumptions on recycling and improved circular economies could not be matched.)
    - Ammonia: Feedstock ammonia from hydrogen
    - Transport: (1) Trucks improved efficiency to EURO6
                 (2) Trucks fleet share electrified as specified (trucks_electrification_share)
                 (3) Sea transport using synthetic diesel instead of heavy fuel oil
    IMPORTANT: Note that the changes are only made in Europe but the rest of the world keeps functioning with the same
    production structure as today's.
    """
    if ccs_clinker:
        cement_update()
    if train_electrification:
        train_update()
    if biomass_from_residues:
        biomass_update(biomass_from_residues_share)
    if h2_iron_and_steel:
        steel_update()
    if olefins_from_methanol:
        plastics_update()
    if methanol_from_electrolysis:
        methanol_update()
    if ammonia_from_hydrogen:
        ammonia_update()
    # in case of trucks_electrification=True, it takes 15-20 min!
    transport_update(trucks_electrification=trucks_electrification,
                     fleet_electrification_share=trucks_electrification_share,
                     sea_transport_syn_diesel=sea_transport_syn_diesel)


def update_foreground(ccs: bool = False, vehicles_as_batteries: bool = True,
                      soec_electrolyser_share: float = 0.3, aec_electrolyser_share: float = 0.4,
                      pem_electrolyser_share: float = 0.3,  # electrolyser variables
                      battery_current_share: bool = False,
                      battery_technology_share: Optional[Dict[str, float]] = config_parameters.EMERGING_TECH_MODERATE,
                      # battery variables
                      open_technology_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND['openground'],
                      roof_technology_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND[
                          "rooftop_power_share"],
                      roof_3kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_3kw"],
                      roof_93kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_93kw"],
                      roof_156kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_156kw"],
                      roof_280kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_280kw"],
                      # solar pv variables
                      onshore_wind_fleet: Dict = config_parameters.BALANCED_ON_WIND_FLEET,  # onshore wind variables
                      offshore_wind_fleet: Dict = config_parameters.OFF_WIND_FLEET  # offshore wind variables
                      ):
    """
    Adapt the foreground activities as follows:
    - Fixes from premise inventories:
        (1) re-scale methanol infrastructure (update_methanol_facility())
        (2) include the cobalt catalyst in gas_to_liquid infrastructure (gas_to_liquid_update())
    - Fixes from Ecoinvent:
        (1) hydro reservoir: transfers land use and emissions from flooding operations to infrastructure instead of
            operation (hydro_reservoir_update()).
        (2) hydro run-of-river: transfers land use to infrastructure instead of operation (hydro_run_of_river_update()).
    - Group infrastructure facilities in a single inventory, when needed:
        (1) biomethane
        (2) waste
        (3) heat pumps
        (4) hydrogen cells
    - Add the necessary inventories:
        (1) add direct emissions inventory for incinerator
        (2) add airborne wind inventory
        (3) add fuel combustion inventories (deleting technosphere)
    - De-nest inventories for methanol, kerosene and diesel.
    - VARIABLE DEPENDANT UPDATES:
        (1) Use of Carbon Capture and Storage in hydrogen for biofuel-to-methanol (default: False)
        (2) Model vehicles as only the electric and electronic parts (battery, etc) (default: True)
        (3) Create fleets. Scenarios described in create_fleets().
    """
    create_additional_acts_db()

    # fixes from premise
    update_methanol_facility()
    gas_to_liquid_update(db_cobalt_name='premise_base', db_gas_to_liquid_name='premise_base')

    # fixes from Ecoinvent
    hydro_run_of_river_update(db_hydro_name='premise_base')
    hydro_reservoir_update(location='ES', db_hydro_name='premise_base')

    # group infrastructure in a single inventory, when needed
    biofuel_to_methane_infrastructure(db_syn_gas_name='premise_base')
    hp_update(db_hp_name='premise_base')
    update_chp_hydrogen()
    chp_waste_update(db_waste_name='apos391', db_original_name='premise_base',
                     locations=['CH'])

    # add necessary inventories
    airborne_wind_lci(bd_airborne_name='premise_base')
    fuels_combustion()

    # de-nest (restructure) inventories for methanol, kerosene and diesel
    rebuild_methanol_act()
    rebuild_kerosene_and_diesel_acts()

    # variable dependant updates
    if not ccs:
        biofuel_to_methanol_update(db_methanol_name='premise_base')
    if vehicles_as_batteries:
        trucks_and_bus_update(db_truck_name='premise_base')
        passenger_car_and_scooter_update(db_passenger_name='premise_base')

    # create fleets
    create_fleets(soec_electrolyser_share=soec_electrolyser_share, aec_electrolyser_share=aec_electrolyser_share,
                  pem_electrolyser_share=pem_electrolyser_share,
                  battery_current_share=battery_current_share,
                  battery_technology_share=battery_technology_share,
                  open_technology_share=open_technology_share,
                  roof_technology_share=roof_technology_share,
                  roof_3kw_share=roof_3kw_share,
                  roof_93kw_share=roof_93kw_share,
                  roof_156kw_share=roof_156kw_share,
                  roof_280kw_share=roof_280kw_share,
                  onshore_wind_fleet=onshore_wind_fleet,
                  offshore_wind_fleet=offshore_wind_fleet)


def create_fleets(
        soec_electrolyser_share: float = 0.3, aec_electrolyser_share: float = 0.4,
        pem_electrolyser_share: float = 0.3,  # electrolyser variables
        battery_current_share: bool = False,
        battery_technology_share: Optional[Dict[str, float]] = config_parameters.EMERGING_TECH_MODERATE,
        # battery variables
        open_technology_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND['openground'],
        roof_technology_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_power_share"],
        roof_3kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_3kw"],
        roof_93kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_93kw"],
        roof_156kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_156kw"],
        roof_280kw_share: Dict[str, float] = config_parameters.PV_CURRENT_TREND["rooftop_280kw"],  # solar pv variables
        onshore_wind_fleet: Dict = config_parameters.BALANCED_ON_WIND_FLEET,  # onshore wind variables
        offshore_wind_fleet: Dict = config_parameters.OFF_WIND_FLEET  # offshore wind variables
):
    """
    Electrolysers:
    SOEC - Solid Oxide Electrolysis Cell - High efficiency. Not commercially available.
    AEC - Alkaline Electrolysis Cell - Low current density. Most stablished technology. Commercially mature.
    PEM - Proton Exchange Membrane - High current density. More expensive. Commercially available (not mature).
        - REQUIREMENT: (soec_electrolyser_share + aec_electrolyser_share + pem_electrolyser_share) = 1.

    Batteries:
    - It allows for either the current share or custom-made scenarios:
        · Current share (from Schlichenmaier & Naegler (2022) https://doi.org/10.1016/j.egyr.2022.11.025):
            [LFP (40%), NMC111 (24%), NMC622 (13%), Lead-acid (10%), ...]
        · Manual. Examples of application in consts.py:
            - EMERGING_TECH_OPTIMISTIC
            - EMERGING_TECH_CURRENT
            - EMERGING_TECH_MODERATE
    - By default we are choosing the EMERGING_TECH_MODERATE scenario.

    Solar pv:
    - Default scenario based on the current share [Fraunhofer Institute (2024)] defined in consts.py

    Onshore wind:
    - Scenarios proposed in consts.py (but we could add other hub heights or rotor diameters!):
        · BIG_ON_WIND_FLEET: 4 MW, 6 MW and 8 MW, equally distributed.
        · BALANCED_ON_WIND_FLEET (default!): 4 MW, 6 MW and 8 MW, with 30%, 65%, 5% shares, respectively.

    Offshore wind:
    - One scenario proposed in consts.py:
        · 14 MW (based on the SG 14-222 DD) and 10 MW (based on the V164-10MW) ->
          gravity: 5%, monopile: 20%, tripod: 10%, floating (spar-buoy): 15%
    """
    # Hydrogen
    hydrogen_from_electrolysis_market(db_hydrogen_name='premise_base',
                                      soec_share=soec_electrolyser_share,
                                      aec_share=aec_electrolyser_share, pem_share=pem_electrolyser_share)
    # Batteries
    batteries_fleet(db_batteries_name='premise_base', current_share=battery_current_share,
                    technology_share=battery_technology_share)
    # Solar photovoltaics
    solar_pv_fleet(db_solar_name='premise_base', open_technology_share=open_technology_share,
                   roof_technology_share=roof_technology_share, roof_3kw_share=roof_3kw_share,
                   roof_93kw_share=roof_93kw_share, roof_156kw_share=roof_156kw_share, roof_280kw_share=roof_280kw_share
                   )

    # Onshore wind fleets
    wind_onshore_fleet(db_wind_name='original_cutoff391', location='DE', fleet_turbines_definition=onshore_wind_fleet)

    # Offshore wind fleets
    wind_offshore_fleet(db_wind_name='original_cutoff391', location='DE', fleet_turbines_definition=offshore_wind_fleet)
