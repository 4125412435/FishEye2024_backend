import datetime
import json
import time
from concurrent.futures import ProcessPoolExecutor

import data_parser.type_parser
from data_parser import parse_node
from data_parser import parse_edge
from data_parser import parse_geo_object
from data_parser.node_parser import *
from data_parser import EntityType, EventType

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


if __name__ == '__main__':
    t1 = time.time()
    initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson')
    t2 = time.time()
    print('Cost ', t2 - t1)
    result = []
    for edge in edge_list:
        if edge.type == data_parser.type_parser.EventType.TransportEvent_TransponderPing:
            result.append({
                'start_time': edge.time.isoformat(),
                'end_time': (edge.time + datetime.timedelta(seconds=edge.dwell)).isoformat(),
                'vessel_id': edge.target,
                'location_id': edge.source
            })
    with open("data_zs.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)
