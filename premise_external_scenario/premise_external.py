from premise import NewDatabase
from datapackage import Package
import bw2data as bd

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
ndb.write_db_to_brightway('test_2')
