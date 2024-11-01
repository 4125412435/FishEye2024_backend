import json
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


def initialize(data_file_path, geo_file_path):
    node_list.clear()
    edge_list.clear()
    id2vessel.clear()
    with open(data_file_path, 'r') as f:
        data = json.load(f)
        for node in data['nodes']:
            n = parse_node(node)
            # 如果是船
            if isinstance(n, Vessel):
                id2vessel[n.id] = n
            elif isinstance(n, Location):
                id2location[n.id] = n
            node_list.append(n)
        for edge in data['links']:
            e = parse_edge(edge)
            edge_list.append(e)

    name2geo.clear()
    with open(geo_file_path, 'r') as f:
        data = json.load(f)
        for node in data['features']:
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

# initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson')
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
