import os

###### ----- MODELLING CONFIG ----- ######

# vestas_file path
cwd = os.getcwd()
windtrace_folder = os.path.join(cwd, 'WindTrace')
VESTAS_FILE = os.path.join(windtrace_folder, 'clean_data.xlsx')

# variables to be set by the user
PROJECT_NAME = 'enbios_prep_test'
NEW_DB_NAME = 'additional_acts'
SPOLDS_CUTOFF = r"C:\ecoinvent_data\3.9.1\cutoff\datasets"
SPOLDS_APOS = r"C:\ecoinvent_data\3.9.1\apos\datasets"

###### ----- SCENARIOS CONFIG ----- ######

# battery predefined scenarios
EMERGING_TECH_OPTIMISTIC = {'LFP': 0.25, 'NMC111': 0, 'NMC523': 0, 'NMC622': 0.05, 'NMC811': 0.05, 'NMC955': 0.05,
                            'SiB': 0.20, 'Vanadium': 0.15, 'lead': 0.05, 'Sodium-Nickel': 0.20}
EMERGING_TECH_MODERATE = {'LFP': 0.35, 'NMC111': 0.04, 'NMC523': 0.04, 'NMC622': 0.04, 'NMC811': 0.04, 'NMC955': 0.04,
                            'SiB': 0.1, 'Vanadium': 0.15, 'lead': 0.05, 'Sodium-Nickel': 0.15}
EMERGING_TECH_CURRENT = {'LFP': 0.4, 'NMC111': 0.066, 'NMC523': 0.067, 'NMC622': 0.067, 'NMC811': 0.0, 'NMC955': 0.0,
                            'SiB': 0.05, 'Vanadium': 0.1, 'lead': 0.1, 'Sodium-Nickel': 0.15}

# solar photovoltaic predefined scenarios
# (1) Follow today's trend. Data from Photovoltaics Report (2024) - Fraunhofer Institute
PV_CURRENT_TREND = {
    'openground': {'CdTe': 0.02, 'CIS': 0, 'micro-Si': 0, 'multi-Si': 0.01, 'single-Si': 0.97},
    "rooftop_3kw": {'single-Si': 0.97, 'CIS': 0, 'a-Si': 0, 'multi-Si': 0.01, 'CdTe': 0.02, 'ribbon-Si': 0},
    "rooftop_93kw": {'multi-Si': 0.02, 'single-Si': 0.98},
    "rooftop_156kw": {'multi-Si': 0.02, 'single-Si': 0.98},
    "rooftop_280kw": {'multi-Si': 0.02, 'single-Si': 0.98},
    "rooftop_power_share": {'3kWp': 0.22, '93kWp': 0.09, '156kWp': 0.09, '280kWp': 0.6}  # data for Spain (2023): industrial=60% (280kW), residential=22% (3kW), commercial=18% (93kW, 156kW). Source: Informe Anual UNEF 2024 (https://www.unef.es/es/recursos-informes?idMultimediaCategoria=18&)
}

# onshore wind predefined scenarios
# (1) 4 MW, 6 MW and 8 MW, equally distributed.
BIG_ON_WIND_FLEET = \
    {'turbine_1': [
        {
            'power': 4.0, 'manufacturer': "Vestas", 'rotor_diameter': 125, 'hub_height': 100,
            'commissioning_year': 2030,
            'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
        }, 0.333],
        'turbine_2': [
            {
                'power': 6.0, 'manufacturer': 'Vestas', 'rotor_diameter': 145, 'hub_height': 120,
                'commissioning_year': 2030,
                'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
            },
            0.333],
        'turbine_3': [
            {
                'power': 8.0, 'manufacturer': 'Vestas', 'rotor_diameter': 160, 'hub_height': 145,
                'commissioning_year': 2030,
                'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
            },
            0.333]}
# (2) 4 MW, 6 MW and 8 MW, with 30%, 65%, 5% shares, respectively.
BALANCED_ON_WIND_FLEET = \
    {'turbine_1': [
        {
            'power': 4.0, 'manufacturer': "Vestas", 'rotor_diameter': 125, 'hub_height': 100,
            'commissioning_year': 2030,
            'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
        }, 0.3],
        'turbine_2': [
            {
                'power': 6.0, 'manufacturer': 'Vestas', 'rotor_diameter': 145, 'hub_height': 120,
                'commissioning_year': 2030,
                'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
            },
            0.65],
        'turbine_3': [
            {
                'power': 8.0, 'manufacturer': 'Vestas', 'rotor_diameter': 160, 'hub_height': 145,
                'commissioning_year': 2030,
                'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
            },
            0.05]}

# (3) 4 MW, 6 MW with 30%, 70%, respectively. Based on the 5.7 MW mean size of turbines ordered in Europe in 2024 (Wind energy in Europe. 2024 Statistics and the outlook for 2025-2030. WindEurope. p.33)
SPAIN_2030_ON_WIND_FLEET = \
    {'turbine_1': [
        {
            'power': 4.0, 'manufacturer': "Vestas", 'rotor_diameter': 125, 'hub_height': 100,
            'commissioning_year': 2030,
            'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
        }, 0.3],
        'turbine_2': [
            {
                'power': 6.0, 'manufacturer': 'Vestas', 'rotor_diameter': 145, 'hub_height': 120,
                'commissioning_year': 2030,
                'generator_type': "gb_dfig", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4
            },
            0.7],
        }

# offshore wind predefined scenarios
# (1) 14 MW (based on the SG 14-222 DD) and 10 MW (based on the V164-10MW)
#       gravity: 5%, monopile: 20%, tripod: 10%, floating (spar-buoy): 15%
OFF_WIND_FLEET = \
    {'turbine_1': [
        {
            'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'gravity', 'floating_platform': None, 'sea_depth': 5, 'distance_to_shore': 30
        }, 0.05],  # based on the SG 14-222 DD
        'turbine_2': [
            {
                'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
                'commissioning_year': 2030,
                'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'gravity', 'floating_platform': None, 'sea_depth': 5, 'distance_to_shore': 30
            }, 0.05],  # based on the V164-10MW
        'turbine_3': [
            {
                'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
                'commissioning_year': 2030,
                'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'monopile', 'floating_platform': None, 'sea_depth': 30, 'distance_to_shore': 30
            }, 0.2],
        'turbine_4': [
            {
                'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
                'commissioning_year': 2030,
                'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'monopile', 'floating_platform': None, 'sea_depth': 30, 'distance_to_shore': 30
            }, 0.2],
        'turbine_5': [
            {
                'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
                'commissioning_year': 2030,
                'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'tripod', 'floating_platform': None, 'sea_depth': 45, 'distance_to_shore': 30
            }, 0.1],
        'turbine_6': [
            {
                'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
                'commissioning_year': 2030,
                'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'tripod', 'floating_platform': None, 'sea_depth': 45, 'distance_to_shore': 30
            }, 0.1],
        'turbine_7': [
            {
                'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
                'commissioning_year': 2030,
                'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'floating', 'floating_platform': 'spar_buoy_steel', 'sea_depth': 45,
                'distance_to_shore': 30
            }, 0.15],
        'turbine_8': [
            {
                'power': 10.0, 'manufacturer': "Vestas", 'rotor_diameter': 164, 'hub_height': 138,
                'commissioning_year': 2030,
                'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'floating', 'floating_platform': 'spar_buoy_steel', 'sea_depth': 45,
                'distance_to_shore': 30
            }, 0.15]
    }

# (2) Spain: 14 MW (based on the SG 14-222 DD), 15 MW (based on Vestas V236-15.0 MW. Likely to be the turbine used in Parc Tramuntana)
#       floating (spar-buoy): 50% [Parc Tramuntana], floating (tension-leg-platform): 50% [PLOCAN: X1]
SPAIN_2030_OFF_WIND_FLEET = \
    {'turbine_1': [
        {
            'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'floating', 'floating_platform': 'spar_buoy_steel', 'sea_depth': 100, 'distance_to_shore': 24  # based on Parc Tramuntana
        }, 0.25],  # based on the SG 14-222 DD
        'turbine_2': [
            {
                'power': 15.0, 'manufacturer': "Vestas", 'rotor_diameter': 236, 'hub_height': 150,
                'commissioning_year': 2030,
                'generator_type': "gb_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'floating', 'floating_platform': 'spar_buoy_steel', 'sea_depth': 100, 'distance_to_shore': 24
            }, 0.25],  # based on the V236-15.0 MW
        'turbine_3': [
        {
            'power': 14.0, 'manufacturer': "Siemens Gamesa", 'rotor_diameter': 222, 'hub_height': 125,
            'commissioning_year': 2030,
            'generator_type': "dd_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
            'offshore_type': 'floating', 'floating_platform': 'tension_leg', 'sea_depth': 100, 'distance_to_shore': 24  # based on Parc Tramuntana
        }, 0.25],  # based on the SG 14-222 DD
        'turbine_24': [
            {
                'power': 15.0, 'manufacturer': "Vestas", 'rotor_diameter': 236, 'hub_height': 150,
                'commissioning_year': 2030,
                'generator_type': "gb_pmsg", 'recycled_share_steel': 0.5, 'lifetime': 25, 'eol_scenario': 4,
                'offshore_type': 'floating', 'floating_platform': 'tension_leg', 'sea_depth': 100, 'distance_to_shore': 24
            }, 0.25],  # based on the V236-15.0 MW
    }

