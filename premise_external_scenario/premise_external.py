from premise import NewDatabase
from datapackage import Package
import bw2data as bd
import pickle

bd.projects.set_current('fossil_free_ecoinvent')

pkg = Package(r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\datapackage.json")

external_scenario = [
    {"scenario": "Business As Usual", "data": pkg},
]
ndb = NewDatabase(
    scenarios=[
        {
            "model": "image",
            "pathway": "SSP2-Base",
            "year": 2050,
            "external scenarios": external_scenario,
        }
    ],
    source_db="ecoinvent-3.9.1-cutoff",   # change to what you actually use
    source_version="3.9.1",
    key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
)
ndb.update("external")

# electricity mixes substitutions
try:
    with open("file.pkl", "rb") as f:
        data = pickle.load(f)
except:
    print("There is no pickled database")

new_high_voltage = [a for a in data if a['name'] == 'market for electricity, high voltage (new)'
                    and a['reference product'] == 'electricity, high voltage (new)' and a['location'] != 'World']

new_medium_voltage = [a for a in data if a['name'] == 'market for electricity, medium voltage (new)'
                      and a['reference product'] == 'electricity, medium voltage (new)' and a['location'] != 'World']

new_low_voltage = [a for a in data if a['name'] == 'market for electricity, low voltage (new)'
                   and a['reference product'] == 'electricity, low voltage (new)' and a['location'] != 'World']


ndb.write_db_to_brightway('test_2')
