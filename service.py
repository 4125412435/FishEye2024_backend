import json
from data_parser import parse_node
from data_parser import parse_edge

node_list = []
edge_list = []


def initialize(data_file_path):
    node_list.clear()
    edge_list.clear()
    with open(data_file_path, 'r') as f:
        data = json.load(f)
        for node in data['nodes']:
            node_list.append(parse_node(node))
        for edge in data['links']:
            edge_list.append(parse_edge(edge))
