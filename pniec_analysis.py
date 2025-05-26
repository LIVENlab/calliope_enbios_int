from main import *

run(ccs_clinker=False, train_electrification=False, biomass_from_residues=False, h2_iron_and_steel=False,
    olefins_from_methanol=False, methanol_from_electrolysis=False, ammonia_from_hydrogen=False,
    trucks_electrification=False, sea_transport_syn_diesel=False,

    ccs=False, vehicles_as_batteries=False,
    # current data (2023). World. Source: https://www.iea.org/energy-system/low-emission-fuels/electrolysers
    soec_electrolyser_share=0.18, aec_electrolyser_share=0.6, pem_electrolyser_share=0.22,
    # current battery share (World)
    battery_current_share=True,
    # current PV share. Spanish data: Informe Anual UNEF 2024 (https://www.unef.es/es/recursos-informes?idMultimediaCategoria=18&)
    open_technology_share=config_parameters.PV_CURRENT_TREND['openground'],
    roof_technology_share=config_parameters.PV_CURRENT_TREND["rooftop_power_share"],
    roof_3kw_share=config_parameters.PV_CURRENT_TREND["rooftop_3kw"],
    roof_93kw_share=config_parameters.PV_CURRENT_TREND["rooftop_93kw"],
    roof_156kw_share=config_parameters.PV_CURRENT_TREND["rooftop_156kw"],
    roof_280kw_share=config_parameters.PV_CURRENT_TREND["rooftop_280kw"],
    # onshore wind. Prospect based on WindEurope outlook (2024).
    onshore_wind_fleet=config_parameters.SPAIN_2030_ON_WIND_FLEET,
    # offshore wind. Based on Parc Tramuntana and PlOCAN data
    offshore_wind_fleet=config_parameters.SPAIN_2030_OFF_WIND_FLEET,

    infrastructure_production_in_europe=False,
    delete_infrastructure=True,

    avoid_electricity=True,
    avoid_countries_list=['ES'],
    avoid_heat=False,
    avoid_co2=False,
    avoid_hydrogen=False,
    avoid_biomass=False,
    avoid_methane=False,
    avoid_methanol=False,
    avoid_kerosene=False,
    avoid_diesel=False,

    mapping_file_path=r'C:\Users\mique\Documents\GitHub\calliope_enbios_int\data\input\tech_mapping_spain.xlsx',
    file_out_path=r'C:\Users\mique\Documents\GitHub\calliope_enbios_int\data\output\tech_mapping_out.xlsx'
    )

# TODO: change WindTrace onshore to new version
