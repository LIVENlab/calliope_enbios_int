from premise import NewDatabase
from datapackage import Package
import bw2data as bd
import pickle
import wurst.searching as ws
import bw2io as bi

bd.projects.set_current('bw25_matrix')

def import_ei_12():
    bi.import_ecoinvent_release(
        version="3.12",
        system_model="cutoff",
        username="USERNAME",
        password="PASS",
        lci=True,
        lcia=True,
        # optional:
        biosphere_name='ecoinvent-3.12-biosphere',         # Default name like "ecoinvent-3.10-biosphere"
    )

pkg = Package(r"C:\Users\mique\Documents\GitHub\calliope_enbios_int\premise_external_scenario\datapackage.json")

external_scenario = [
    {"scenario": "Business As Usual", "data": pkg},
]
ndb = NewDatabase(
    scenarios=[
        {
            "model": "image",
            "pathway": "SSP2-L",
            "year": 2020,
            "external scenarios": external_scenario,
        }
    ],
    source_db="cutoff391",   # change to what you actually use
    source_version="3.9.1",
    key="tUePmX_S5B8ieZkkM7WUU2CnO8SmShwmAeWK9x2rTFo=",
)
ndb.update("external")

# electricity mixes substitutions
print('Starting electricity mix substitutions')
pickle_path = ndb.scenarios[0]['database filepath']
try:
    with open(pickle_path, "rb") as f:
        data = pickle.load(f)
except:
    print("There is no pickled database")

# delete "World" markets
data = [a for a in data if a['location'] != 'World']

voltages = ["high", "medium", "low"]

all_voltages = [
    a for a in data
    if any(
        a["name"] == f"market for electricity, {v} voltage (new)"
        and a["reference product"] == f"electricity, {v} voltage (new)"
        for v in voltages
    )
]

acts_to_be_replaced = []
for act in all_voltages:
    act_to_be_replaced = [a for a in data if a['name'] == act['name'][:-6] and a['reference product'] == act['reference product'][:-6] and a['location'] == act['location']]
    acts_to_be_replaced.append(act_to_be_replaced[0])

# TODO: create a function
counter = 0
for replaceable_act in acts_to_be_replaced:
    replacing_act = all_voltages[counter]
    for act in data:
        for exchange in ws.technosphere(act):
            if exchange['product'] == replaceable_act['reference product'] and exchange['name'] == replaceable_act['name'] and exchange['unit'] == replaceable_act['unit'] and exchange['location'] == replaceable_act['location']:
                if not exchange['name'] == act['name']:
                    exchange['product'] = replacing_act['reference product']
                    exchange['name'] = replacing_act['name']
    counter += 1

# natural gas substitutions
print('Starting natural gas substitutions')
locations = [
    "AT", "BE", "CH", "CZ", "DE", "DK", "ES", "FI", "FR", "GB", "GR", "HU", "IE", "IT",
    "NL", "NO", "PL", "RO", "SE", "SK", "RoE"
]
acts_to_be_replaced = [a for a in data if a['name'] == 'market for natural gas, high pressure' and a['location'] in locations]
counter = 0
for replaceable_act in acts_to_be_replaced:
    for act in data:
        for exchange in ws.technosphere(act):
            if (exchange['product'] == replaceable_act['reference product'] and
                    exchange['name'] == replaceable_act['name'] and
                    exchange['unit'] == replaceable_act['unit'] and
                    exchange['location'] == replaceable_act['location']):
                if not exchange['name'] == act['name']:
                    exchange['product'] = 'methane, high pressure (new)'
                    exchange['name'] = 'market for methane, high pressure (new)'
    counter += 1
locations = [
    "DE", "GB", "NL", "NO", "RO"
]
acts_to_be_replaced = [a for a in data if a['name'] == 'petroleum and gas production, offshore' and a['reference product'] == 'natural gas, high pressure' and a['location'] in locations]
counter = 0
for replaceable_act in acts_to_be_replaced:
    for act in data:
        for exchange in ws.technosphere(act):
            if (exchange['product'] == replaceable_act['reference product'] and
                    exchange['name'] == replaceable_act['name'] and
                    exchange['unit'] == replaceable_act['unit'] and
                    exchange['location'] == replaceable_act['location']):
                if not exchange['name'] == act['name']:
                    exchange['product'] = 'methane, high pressure (new)'
                    exchange['name'] = 'market for methane, high pressure (new)'
    counter += 1
locations = [
    "NL", "RO", "GB", "DE"
]
acts_to_be_replaced = [a for a in data if a['name'] == 'petroleum and gas production, onshore' and a['reference product'] == 'natural gas, high pressure' and a['location'] in locations]
counter = 0
for replaceable_act in acts_to_be_replaced:
    for act in data:
        for exchange in ws.technosphere(act):
            if (exchange['product'] == replaceable_act['reference product'] and
                    exchange['name'] == replaceable_act['name'] and
                    exchange['unit'] == replaceable_act['unit'] and
                    exchange['location'] == replaceable_act['location']):
                if not exchange['name'] == act['name']:
                    exchange['product'] = 'methane, high pressure (new)'
                    exchange['name'] = 'market for methane, high pressure (new)'
    counter += 1


with open(pickle_path, "wb") as f:
    pickle.dump(data, f)

ndb.write_db_to_brightway('test_2')
