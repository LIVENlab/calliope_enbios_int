import bw2io as bi
import wurst.errors
from premise import *
from config_parameters import *
from functions import *
import bw2data as bd
import shutil

# TODO: next seps in the workflow: run sparks: guardat al servidor a l'arxiu testttttt.py.
# TODO: check options to include Regioinvent

def run(ccs_clinker: bool = True,
        train_electrification: bool = True,
        biomass_from_residues: bool = True,
        biomass_from_residues_share: float = 1.0,
        h2_iron_and_steel: bool = True,
        olefins_from_methanol: bool = True,
        methanol_from_electrolysis: bool = True,
        ammonia_from_hydrogen: bool = True,
        trucks_electrification: bool = True,
        trucks_electrification_share: float = 0.5,
        sea_transport_syn_diesel: bool = True,

        ccs: bool = False, vehicles_as_batteries: bool = True,
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
        mapping_file_path: str = r'C:\Users\1361185\OneDrive - UAB\Documentos\GitHub\calliope_enbios_int\data\input\tech_mapping_in.xlsx',

        delete_infrastructure: bool = True,
        om_spheres_separation: bool = True,
        avoid_double_counting: bool = True,
        file_out_path: str = r'C:\Users\1361185\OneDrive - UAB\Documentos\GitHub\calliope_enbios_int\data\output\tech_mapping_out.xlsx',

        avoid_electricity: bool = True,
        avoid_heat: bool = True,
        avoid_co2: bool = True,
        avoid_hydrogen: bool = True,
        avoid_biomass: bool = True,
        avoid_methane: bool = True,
        avoid_methanol: bool = True,
        avoid_kerosene: bool = True,
        avoid_diesel: bool = True,
        avoid_countries_list: Optional[List[str]] = None,
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

    if 'premise_original' not in bd.databases:
        # premise, without updates (only imported inventories)
        ndb = NewDatabase(
            scenarios=[
                {"model": "image", "pathway": "SSP2-RCP19", "year": 2020},
            ],
            source_db="original_cutoff391",
            source_version="3.9.1",
            key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo='
        )
        ndb.write_db_to_brightway(name='premise_original')

    if 'premise_cement' not in bd.databases:
        # premise with cement and biomass update
        ndb = NewDatabase(
            scenarios=[
                {"model": "image", "pathway": "SSP2-RCP19", "year": 2020},
            ],
            source_db="original_cutoff391",
            source_version="3.9.1",
            key='tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo='
        )
        ndb.update('cement')
        ndb.update('biomass')
        ndb.write_db_to_brightway(name='premise_cement')

    # create a premise_original copy named 'premise_base'
    if 'premise_base' not in bd.databases:
        bd.Database('premise_original').copy(name="premise_base")

    # background changes
    update_background(ccs_clinker=ccs_clinker,
                      train_electrification=train_electrification,
                      biomass_from_residues=biomass_from_residues,
                      biomass_from_residues_share=biomass_from_residues_share,
                      h2_iron_and_steel=h2_iron_and_steel,
                      olefins_from_methanol=olefins_from_methanol,
                      methanol_from_electrolysis=methanol_from_electrolysis,
                      ammonia_from_hydrogen=ammonia_from_hydrogen,
                      trucks_electrification=trucks_electrification,
                      trucks_electrification_share=trucks_electrification_share,
                      sea_transport_syn_diesel=sea_transport_syn_diesel)
    # TODO: allow to have shares of today's and future's industry!!!!
    # TODO: allow the rest of the world to also update their industries (according to IAMs?)
    # TODO: allow to change Europe's electricity mix in case we apply the code to only one country

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

    # 'infrastructure (with European steel and concrete)' operating.
    if infrastructure_production_in_europe:
        update_cement_iron_foreground(file_path=mapping_file_path)

    # O&M activities in premise_base and additional_acts do not have infrastructure inputs after running this function.
    # Moreover, now we have activities (with ', biosphere' and ', technosphere' at the end of the name indicated in the
    # mapping file) in additional_acts.
    if delete_infrastructure:
        delete_infrastructure_main(
            file_path=mapping_file_path, om_spheres_separation=om_spheres_separation)

    # avoid double accounting
    if avoid_double_counting:
        avoid_double_accounting(electricity=avoid_electricity, heat=avoid_heat, co2=avoid_co2,
                                hydrogen=avoid_hydrogen, biomass=avoid_biomass, methane=avoid_methane,
                                methanol=avoid_methanol, kerosene=avoid_kerosene, diesel=avoid_diesel,
                                avoid_countries_list=avoid_countries_list)

    # save the output file
    if om_spheres_separation:
        create_output_file(file_in=mapping_file_path, file_out=file_out_path)
    else:
        shutil.copy(mapping_file_path, file_out_path)


def avoid_double_accounting(electricity: bool, heat: bool, co2: bool, hydrogen: bool, biomass: bool,
                            methane: bool, methanol: bool, kerosene: bool, diesel: bool,
                            avoid_countries_list: Optional[List[str]] = None,
                            ):
    """
    We use the polluter pays principle to avoid double accounting. There are two possible sources of double accounting.
    Let's break them down using electricity production as an example:
    1. Calliope calculates the demand for electricity in Europe. We will calculate the impacts of the technologies that
    produce electricity to satisfy this demand. Thus, when calculating the impacts of other technologies within the
    energy system (e.g., electricity used in electrolysers for hydrogen production), we should not count the
    impacts of this electricity again. (delete internal links)
    2. Calliope assumes electricity is produced with certain technologies. However, in Ecoinvent, other technologies
    might be producing electricity in the background (e.g., coal is not used in Calliope but is in the background of
    Ecoinvent). Thus, they should not be accounted for either. (delete links from shifted demand from Ecoinvent
    to Calliope)
    The following energy carriers are dealt with: electricity, heat, CO2, hydrogen, waste, biomass, methane, methanol,
    kerosene, diesel.
    """
    print(f'Starting avoiding double accounting protocol. Applied to: electricity: {electricity}, heat: {heat}, '
          f'CO2: {co2}, hydrogen: {hydrogen}, biomass: {biomass}, methane: {methane}, methanol: {methanol}, '
          f'kerosene: {kerosene}, diesel: {diesel}')
    for name in ['premise_base', 'additional_acts',
                 'premise_auxiliary_for_infrastructure', 'infrastructure (with European steel and concrete)']:
        if electricity:
            try:
                # Electricity
                unlink_electricity(db_name=name, country_codes_list=avoid_countries_list)
            except wurst.errors.NoResults:
                print(f'electricity not available in {name}')
        if heat:
            try:
                # Heat
                unlink_heat(db_name=name)
            except wurst.errors.NoResults:
                print(f'heat not available in {name}')
        if co2:
            try:
                # CO2
                unlink_co2(db_name=name)
            except wurst.errors.NoResults:
                print(f'co2 not available in {name}')
        if hydrogen:
            try:
                # Hydrogen
                unlink_hydrogen(db_name=name)
            except wurst.errors.NoResults:
                print(f'hydrogen not available in {name}')
        # Waste
        # In cutoff it comes without any environmental burdens, so there is no need to apply any unlinking
        if biomass:
            try:
                # Biomass
                unlink_biomass(db_name=name)
            except wurst.errors.NoResults:
                print(f'biomass not available in {name}')
        if methane:
            try:
                # Methane
                unlink_methane(db_name=name)
            except wurst.errors.NoResults:
                print(f'methane not available in {name}')
        if methanol:
            try:
                # Methanol
                unlink_methanol(db_name=name)
            except wurst.errors.NoResults:
                print(f'methanol not available in {name}')
        if kerosene:
            try:
                # Kerosene
                unlink_kerosene(db_name=name)
            except wurst.errors.NoResults:
                print(f'kerosene not available in {name}')
        if diesel:
            try:
                # Diesel
                unlink_diesel(db_name=name)
            except wurst.errors.NoResults:
                print(f'diesel not available in {name}')

    print('Double accounting protocol successfully completed')


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
    print(
        f"Updating background. The following sectors are going to be updated:\n"
        f"  - Cement with Carbon Capture and Storage = {ccs_clinker}\n"
        f"  - Train electrification = {train_electrification}\n"
        f"  - Biomass from residues = {biomass_from_residues} "
        f"(with a residues share of: {biomass_from_residues_share})\n"
        f"  - Iron and steel from hydrogen = {h2_iron_and_steel}\n"
        f"  - Olefins (plastics) from methanol = {olefins_from_methanol}\n"
        f"  - Methanol from electrolysis = {methanol_from_electrolysis}\n"
        f"  - Ammonia from hydrogen = {ammonia_from_hydrogen}\n"
        f"  - Trucks electrification = {trucks_electrification} "
        f"(with an electrification share of: {trucks_electrification_share})\n"
        f"  - Sea transport using synthetic diesel = {sea_transport_syn_diesel}"
    )

    if ccs_clinker:
        print('Updating cement')
        cement_update()
    if train_electrification:
        print('Updating train')
        train_update()
    if biomass_from_residues:
        print('Updating biomass')
        biomass_update(biomass_from_residues_share)
    if h2_iron_and_steel:
        print('Updating steel and iron')
        steel_update()
    if olefins_from_methanol:
        print('updating plastics')
        plastics_update()
    if methanol_from_electrolysis:
        print('updating methanol')
        methanol_update()
    if ammonia_from_hydrogen:
        print('updating ammonia')
        ammonia_update()
    # in the case of trucks_electrification=True, it takes 15-20 min!
    transport_update(trucks_electrification=trucks_electrification,
                     fleet_electrification_share=trucks_electrification_share,
                     sea_transport_syn_diesel=sea_transport_syn_diesel)
    print('Background update finished.')


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
        (2) Model vehicles as only the electric and electronic parts (battery, etc.) (default: True)
        (3) Create fleets. Scenarios described in create_fleets().
    """
    print('Updating foreground.')
    create_additional_acts_db()

    # fixes from premise
    update_methanol_facility()
    gas_to_liquid_update(db_cobalt_name='premise_base', db_gas_to_liquid_name='premise_base')

    # fixes from Ecoinvent
    hydro_run_of_river_update(db_hydro_name='premise_base')
    hydro_reservoir_update(location='ES', db_hydro_name='premise_base')
    pumped_hydro_update(location='ES', db_pump_name='premise_base')

    # group infrastructure in a single inventory, when needed
    biofuel_to_methane_infrastructure(db_syn_gas_name='premise_base')
    hp_update(db_hp_name='premise_base')
    update_chp_hydrogen()
    chp_waste_update(db_waste_name='apos391', db_original_name='premise_base',
                     locations=['ES'])
    biogas_update(db_biogas_name='premise_base')  # necessary if we need biogas infrastructure like in Spain

    # add the necessary inventories
    airborne_wind_lci(bd_airborne_name='premise_base')
    fuels_combustion()

    if not ccs:
        biofuel_to_methanol_update(db_methanol_name='premise_base')
        # de-nest (restructure) inventories for methanol, kerosene and diesel
        print('De-nesting methanol, kerosene and diesel.')
        rebuild_methanol_act()
        # make steam input for methanol European
        methanol_distillation_update()
        rebuild_kerosene_and_diesel_acts()
        # TODO: rewrite rebuild_methanol_act() for when ccs = True

    # variable dependant updates
    if vehicles_as_batteries:
        print('modelling vehicles as batteries')
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

    print('Foreground updated successfully.')


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
    - By default, we are choosing the EMERGING_TECH_MODERATE scenario.

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
    wind_onshore_fleet(db_wind_name='original_cutoff391', location='ES', fleet_turbines_definition=onshore_wind_fleet)

    # Offshore wind fleets
    # TODO: 1. offshore per kWh, 2. maintenance emissions are onsite!
    wind_offshore_fleet(db_wind_name='original_cutoff391', location='ES', fleet_turbines_definition=offshore_wind_fleet)


def create_output_file(file_in: str, file_out: str):
    print('Creating output file')
    # Load the Excel file with both sheets
    sheets = pd.read_excel(file_in, sheet_name=None)

    # Extract the sheets
    o_m_df = sheets['o&m']
    infrastructure_df = sheets['infrastructure']

    # Create two versions for 'life_cycle_inventory_name'
    biosphere_df = o_m_df.copy()
    biosphere_df['life_cycle_inventory_name'] = biosphere_df['life_cycle_inventory_name'] + ', biosphere'

    technosphere_df = o_m_df.copy()
    technosphere_df['life_cycle_inventory_name'] = technosphere_df['life_cycle_inventory_name'] + ', technosphere'

    # Add the new column right after 'id'
    biosphere_df.insert(biosphere_df.columns.get_loc('id') + 1, 'geographical_scope', 'onsite')
    technosphere_df.insert(technosphere_df.columns.get_loc('id') + 1, 'geographical_scope', 'offsite')

    # Concatenate the two dataframes
    new_o_m_df = pd.concat([biosphere_df, technosphere_df], ignore_index=True)
    # All activities from 'o&m' should be taken from 'additional_acts'
    new_o_m_df['prod_database'] = 'additional_acts'

    # Save to a new Excel file with both sheets
    with pd.ExcelWriter(file_out, engine='xlsxwriter') as writer:
        new_o_m_df.to_excel(writer, sheet_name='o&m', index=False)
        infrastructure_df.to_excel(writer, sheet_name='infrastructure', index=False)

    print(f"File '{file_out}' created successfully.")


"""###### run ######
run()
# create backup
bi.backup_project_directory(config_parameters.PROJECT_NAME)"""

"""## change venv to bw25!
# migrate to bw25 (NOTE: using a venv with bw25)
bd.projects.set_current(config_parameters.PROJECT_NAME)
bd.projects.migrate_project_25()
# create bw25 backup to share project
bi.backup_project_directory(config_parameters.PROJECT_NAME)
# restore bw25 project (change dbfile)
bi.restore_project_directory('dbfile')"""
