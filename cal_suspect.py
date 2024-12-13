import json
import math
import time

import torch

import service
from data_parser import EntityType
from model import ShipRoutePredictor, get_ship_embedding

vocab_size = 25
embed_dim = 64
num_heads = 4
num_layers = 2
num_ships = 296
dropout = 0.1
max_length = 127

suspect_ship = [
    'plaiceplundererba1',
    'europeanseabassbuccaneer777',
    'pompanoplunderere5d',
    'roachrobberdb6',
    'whitemarlinwranglerbac',
    'huron1b3',
    'opheliacac',
    'wavewranglerc2d',
    'bigeyetunabanditb73',
    'arcticgraylingangler094',
    'channelcatfishcapturer175',
    'snappersnatcher7be'
]


def cal_suspect_ratio(target_id):
    target_embedding = get_ship_embedding(m, target_id)
    sim_with_suspect = suspect_ship_embedding @ target_embedding
    sim_with_suspect = sim_with_suspect.softmax(dim=-1)

    suspect_blend_embedding = sim_with_suspect @ suspect_ship_embedding
    sim = torch.cosine_similarity(target_embedding, suspect_blend_embedding, dim=0)
    sim = (torch.exp(sim + 1) - 1) / (math.e ** 2 - 1)
    return sim


def cal_suspect():
    result = {}
    for vessel in service.id2vessel.values():
        if vessel.type == EntityType.Vessel_FishingVessel:
            company = vessel.company
            v_id = vessel.id
            if company == 'SouthSeafood Express Corp':
                continue
            if company not in result.keys():
                result[company] = {'suspect_ratio': 0, 'vessels': {}}
            suspect_ratio = cal_suspect_ratio(ship_mapping[v_id]).item()
            result[company]['suspect_ratio'] = max(result[company]['suspect_ratio'], suspect_ratio)
            result[company]['vessels'][v_id] = suspect_ratio
    with open('suspect.json', 'w') as json_file:
        json.dump(result, json_file, indent=4)


if __name__ == '__main__':
    m = ShipRoutePredictor(vocab_size=vocab_size, embed_dim=embed_dim, num_heads=num_heads, num_layers=num_layers,
                           num_ships=num_ships, dropout=dropout, max_seq_len=max_length)
    m.load_state_dict(torch.load('model.pth'))
    t1 = time.time()
    service.initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson')
    t2 = time.time()
    print('Cost ', t2 - t1, 's')
    ship_mapping = {k: v for v, k in enumerate(service.id2vessel.keys())}
    suspect_ship_id = [ship_mapping[i] for i in suspect_ship]

    suspect_ship_embedding = get_ship_embedding(m, suspect_ship_id)
    cal_suspect()