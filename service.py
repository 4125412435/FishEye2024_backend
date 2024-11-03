import json
import time
from concurrent.futures import ProcessPoolExecutor

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
    with ProcessPoolExecutor(max_workers=8) as executor:
        # 分割节点
        num_nodes = len(data['nodes'])
        chunk_size_nodes = max(1, num_nodes // 8)
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
        chunk_size_edges = max(1, num_edges // 6)
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


# def initialize(data_file_path, geo_file_path):
#     node_list.clear()
#     edge_list.clear()
#     id2vessel.clear()
#     with open(data_file_path, 'r') as f:
#         data = json.load(f)
#         for node in data['nodes']:
#             n = parse_node(node)
#             # 如果是船
#             if isinstance(n, Vessel):
#                 id2vessel[n.id] = n
#             elif isinstance(n, Location):
#                 id2location[n.id] = n
#             node_list.append(n)
#         for edge in data['links']:
#             e = parse_edge(edge)
#             edge_list.append(e)
#
#     name2geo.clear()
#     with open(geo_file_path, 'r') as f:
#         data = json.load(f)
#         for node in data['features']:
#             n = parse_geo_object(node)
#             name2geo[n.id] = n


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

# if __name__ == '__main__':
# t1 = time.time()
# initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson')
# t2 = time.time()
# print('Cost ', t2 - t1)
# print(len(node_list))
# print(len(edge_list))

#
# start_time = '2035-05-06'
# end_time = '2035-08-29'
# results = select_edge_attribute(EventType.TransportEvent_TransponderPing, attribute='time',
#                                 func=lambda x: start_time <= x <= end_time
#                                 )
# print(len(results))
# print(results[0].__dict__)
# start_time = '2035-06-05'
# print(start_time <= results[0].time <= end_time)
