import json
import time
from concurrent.futures import ProcessPoolExecutor

from data_parser import EntityType, EventType
from data_parser import parse_edge
from data_parser import parse_geo_object
from data_parser.node_parser import *
import numpy as np

node_list = []
edge_list = []

# 保存vessel id->vessel
id2vessel = {}
id2location = {}
# Location类型通过name查找geo feature
name2geo = {}


def process_nodes(nodes):
    local_node_list = []
    local_id2vessel = {}
    local_id2location = {}
    for node in nodes:
        n = parse_node(node)
        if isinstance(n, Vessel):
            local_id2vessel[n.id] = n
        elif isinstance(n, Location):
            local_id2location[n.id] = n
        local_node_list.append(n)

    return local_node_list, local_id2vessel, local_id2location


def process_edges(links):
    local_edge_list = []
    for edge in links:
        e = parse_edge(edge)
        local_edge_list.append(e)

    return local_edge_list


def initialize(data_file_path, geo_file_path, num_workers=8):
    global node_list, edge_list, id2vessel, id2location, name2geo

    # 读取 JSON 文件
    with open(data_file_path, 'r') as f:
        data = json.load(f)

    # 使用 ProcessPoolExecutor 进行并行处理
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # 分割节点
        num_nodes = len(data['nodes'])
        chunk_size_nodes = max(1, num_nodes // num_workers)
        node_chunks = [data['nodes'][i:i + chunk_size_nodes] for i in range(0, num_nodes, chunk_size_nodes)]

        # 提交节点处理任务并收集结果
        futures_node = [executor.submit(process_nodes, chunk) for chunk in node_chunks]
        results_node = [future.result() for future in futures_node]

        # 合并节点处理结果
        for local_node_list, local_id2vessel, local_id2location in results_node:
            node_list.extend(local_node_list)
            id2vessel.update(local_id2vessel)
            id2location.update(local_id2location)

        # 分割边
        num_edges = len(data['links'])
        chunk_size_edges = max(1, num_edges // num_workers)
        edge_chunks = [data['links'][i:i + chunk_size_edges] for i in range(0, num_edges, chunk_size_edges)]

        # 提交边处理任务并收集结果
        futures_edge = [executor.submit(process_edges, chunk) for chunk in edge_chunks]
        results_edge = [future.result() for future in futures_edge]

        # 合并边处理结果
        for local_edge_list in results_edge:
            edge_list.extend(local_edge_list)

    # 读取并处理地理信息文件
    name2geo.clear()
    with open(geo_file_path, 'r') as f:
        geo_data = json.load(f)
        for node in geo_data['features']:
            n = parse_geo_object(node)
            name2geo[n.id] = n


def select_nodes(func):
    results = []
    for node in node_list:
        if func(node):
            results.append(node)
    return results


def select_edge(func):
    results = []
    for edge in edge_list:
        if func(edge):
            results.append(edge)
    return results


def select_entity_attribute(entity_type, attribute, func):
    results = []
    for node in node_list:
        if node.type == entity_type:
            if func(node.__dict__[attribute]):
                results.append(node)
    return results


def select_edge_attribute(edge_type, attribute, func):
    results = []
    for edge in edge_list:
        if edge.type == edge_type:
            if func(edge.__dict__[attribute]):
                results.append(edge)
    return results


def select_vessel_by_id(vessel_id):
    return id2vessel[vessel_id]


def select_location_by_id(location_id):
    return id2location[location_id]


def select_geo_by_id(location_id):
    location = id2location[location_id]
    return name2geo[location.name]


def select_fishing_vessel_by_company(company):
    results = []
    for vessel in id2vessel.values():
        if vessel.type == EntityType.Vessel_FishingVessel:

            if vessel.company == company:
                results.append(vessel)
    return results


def normalize(v):
    norm = np.linalg.norm(v)
    if norm == 0:
        return v
    return v / norm


def select_preserve():
    result = []
    for name, l in id2location.items():
        if l.kind == "Ecological Preserve":
            result.append(name)
    return result


def select_dwell_vector(vessel_id, norm=False, location_list=None, weight_mapping=None):
    locations = id2location.keys() if location_list is None else location_list
    location_idx_mapping = {l: idx for idx, l in enumerate(locations)}
    location_vector = np.zeros(len(locations))
    for edge in edge_list:
        if edge.type == EventType.TransportEvent_TransponderPing:
            if edge.target == vessel_id:
                location_name = edge.source
                if location_name not in location_idx_mapping.keys():
                    continue
                dwell = edge.dwell
                location_vector[location_idx_mapping[location_name]] += dwell * weight_mapping.get(location_name, 1)
    if norm:
        location_vector = normalize(location_vector)
    return location_vector


def select_transponder_ping():
    vessel_transponderping = {}
    for edge in edge_list:
        if edge.type == EventType.TransportEvent_TransponderPing:
            if edge.target not in vessel_transponderping.keys():
                vessel_transponderping[edge.target] = []
            vessel_transponderping[edge.target].append({
                'time': edge.time,
                'dwell': edge.dwell,
                'source': edge.source
            })
    return vessel_transponderping

def calculate_suspect():
    preserve_list = select_preserve()
    weight_mapping = {l: 10 for l in preserve_list}
    x1 = select_dwell_vector('snappersnatcher7be', norm=False, weight_mapping=weight_mapping)
    x1 += 50000
    for idx, l in enumerate(id2location.keys()):
        if l in preserve_list:
            x1[idx] += 500000
    x1 = normalize(x1)
    result = {}
    for vessel in id2vessel.values():
        if vessel.type == EntityType.Vessel_FishingVessel:
            company = vessel.company
            v_id = vessel.id
            d_vector = select_dwell_vector(v_id, norm=True, weight_mapping=weight_mapping)
            if company == 'SouthSeafood Express Corp':
                continue
            if company not in result.keys():
                result[company] = {'suspect_ratio': 0, 'vessels': {}}
            suspect_ratio = x1 @ d_vector
            result[company]['suspect_ratio'] = max(result[company]['suspect_ratio'], suspect_ratio)
            result[company]['vessels'][v_id] = suspect_ratio
    with open('suspect.json', 'w') as json_file:
        json.dump(result, json_file, indent=4)


# if __name__ == '__main__':
#     t1 = time.time()
#     initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson')
#     t2 = time.time()
#     print('Cost ', t2 - t1)
# print(id2vessel.items())
# print(select_fishing_vessel_by_company('WestRiver Shipping KgaA'))
# calculate_suspect()
