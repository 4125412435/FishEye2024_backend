from data_parser.type_parser import EventType, parse_type
from data_parser.metadata_parser import parse_metadata


class Event:
    def __init__(self, event_type, metadata, source, target, key):
        self.event_type = event_type
        self.metadata = metadata
        self.source = source
        self.target = target
        self.key = key


class Transaction(Event):
    def __init__(self, metadata, date, source, target, key):
        super(Transaction, self).__init__(EventType.Transaction, metadata, source, target, key)
        self.date = date


class HarborReport(Event):
    def __init__(self, metadata, date, data_author, aphorism, holiday_greeting, wisdom, saying, source, target, key):
        super(HarborReport, self).__init__(EventType.HarborReport, metadata, source, target, key)
        self.date = date
        self.data_author = data_author
        self.aphorism = aphorism
        self.holiday_greeting = holiday_greeting
        self.wisdom = wisdom
        self.saying = saying


class TransponderPing(Event):
    def __init__(self, metadata, time, dwell, source, target, key):
        super(TransponderPing, self).__init__(EventType.TransportEvent_TransponderPing, metadata, source, target, key)
        self.time = time
        self.dwell = dwell


def parse_edge(json_node):
    edge_type = parse_type(json_node['type'])
    metadata = parse_metadata(json_node)
    date = json_node.get('date', None)
    time = json_node.get('time', None)
    dwell = json_node.get('dwell', None)
    data_author = json_node.get('data_author', None)
    aphorism = json_node.get('aphorism', None)
    holiday_greeting = json_node.get('holiday_greeting', None)
    wisdom = json_node.get('wisdom', None)
    saying = json_node.get('saying of the sea', None)
    source = json_node.get('source', None)
    target = json_node.get('target', None)
    key = json_node.get('key', None)

    if edge_type is None:
        raise 'The edge has wrong type'
    if edge_type == EventType.HarborReport:
        return HarborReport(metadata, date, data_author, aphorism, holiday_greeting, wisdom, saying, source, target,
                            key)
    elif edge_type == EventType.Transaction:
        return Transaction(metadata, date, source, target, key)
    elif edge_type == EventType.TransportEvent_TransponderPing:
        return TransponderPing(metadata, time, dwell, source, target, key)
    else:
        raise 'Error parsing edge'
