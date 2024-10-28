from data_parser.type_parser import EntityType, parse_type
from data_parser.metadata_parser import parse_metadata


class Entity:
    def __init__(self, entity_type, metadata):
        self.entity_type = entity_type
        self.metadata = metadata


class CargoVessel(Entity):
    def __init__(self, metadata, flag_country, tonnage, name, length_overall, vessel_id):
        super(CargoVessel, self).__init__(EntityType.Vessel_CargoVessel, metadata)
        self.flag_country = flag_country
        self.tonnage = tonnage
        self.name = name
        self.length_overall = length_overall
        self.id = vessel_id


class FishingVessel(Entity):
    def __init__(self, metadata, flag_country, company, tonnage, name, length_overall, vessel_id):
        super(FishingVessel, self).__init__(EntityType.Vessel_FishingVessel, metadata)
        self.flag_country = flag_country
        self.company = company
        self.tonnage = tonnage
        self.name = name
        self.length_overall = length_overall
        self.id = vessel_id


class OtherVessel(Entity):
    def __init__(self, metadata, flag_country, name, length_overall, vessel_id):
        super(OtherVessel, self).__init__(EntityType.Vessel_Other, metadata)
        self.flag_country = flag_country
        self.name = name
        self.length_overall = length_overall
        self.id = vessel_id


class FerryPassengerVessel(Entity):
    def __init__(self, metadata, flag_country, name, vessel_id):
        super(FerryPassengerVessel, self).__init__(EntityType.Vessel_Ferry_Passenger, metadata)
        self.flag_country = flag_country
        self.name = name
        self.id = vessel_id


class FerryCargoVessel(Entity):
    def __init__(self, metadata, flag_country, name, vessel_id):
        super(FerryCargoVessel, self).__init__(EntityType.Vessel_Ferry_Cargo, metadata)
        self.flag_country = flag_country
        self.name = name
        self.id = vessel_id


class ResearchVessel(Entity):
    def __init__(self, metadata, flag_country, name, vessel_id):
        super(ResearchVessel, self).__init__(EntityType.Vessel_Research, metadata)
        self.flag_country = flag_country
        self.name = name
        self.id = vessel_id


class TourVessel(Entity):
    def __init__(self, metadata, flag_country, name, vessel_id):
        super(TourVessel, self).__init__(EntityType.Vessel_Tour, metadata)
        self.flag_country = flag_country
        self.name = name
        self.id = vessel_id


class Fish(Entity):
    def __init__(self, metadata, name, fish_id):
        super(Fish, self).__init__(EntityType.Commodity_Fish, metadata)
        self.name = name
        self.id = fish_id


class Point(Entity):
    def __init__(self, metadata, name, description, activity_list, kind, point_id):
        super(Point, self).__init__(EntityType.Location_Point, metadata)
        self.name = name
        self.description = description
        self.activity_list = activity_list
        self.kind = kind
        self.id = point_id


class City(Entity):
    def __init__(self, metadata, name, description, activity_list, kind, city_id):
        super(City, self).__init__(EntityType.Location_City, metadata)
        self.name = name
        self.description = description
        self.activity_list = activity_list
        self.kind = kind
        self.id = city_id


class Region(Entity):
    def __init__(self, metadata, name, description, fish_species_present, activity_list, kind, region_id):
        super(Region, self).__init__(EntityType.Location_Region, metadata)
        self.name = name
        self.description = description
        self.fish_species_present = fish_species_present
        self.activity_list = activity_list
        self.kind = kind
        self.id = region_id


class DeliveryReport(Entity):
    def __init__(self, metadata, qty_tons, date, report_id):
        super(DeliveryReport, self).__init__(EntityType.Location_Region, metadata)
        self.qty_tons = qty_tons
        self.date = date
        self.id = report_id


def parse_node(json_node):
    node_type = parse_type(json_node['type'])
    metadata = parse_metadata(json_node)
    name = json_node.get('Name', None)
    description = json_node.get('Description', None)
    activity_list = json_node.get('Activities', None)
    kind = json_node.get('kind', None)
    id_ = json_node.get('id', None)
    fish_species_present = json_node.get('fish_species_present', None)
    flag_country = json_node.get('flag_country', None)
    company = json_node.get('company', None)
    length_overall = json_node.get('length_overall', None)
    tonnage = json_node.get('tonnage', None)
    qty_tons = json_node.get('qty_tons', None)
    date = json_node.get('date', None)
    if node_type is None:
        raise 'The node has wrong type'
    if node_type == EntityType.Commodity_Fish:
        return Fish(metadata, name, id_)
    elif node_type == EntityType.Location_Point:
        return Point(metadata, name, description, activity_list, kind, id_)
    elif node_type == EntityType.Location_City:
        return City(metadata, name, description, activity_list, kind, id_)
    elif node_type == EntityType.Location_Region:
        return Region(metadata, name, description, fish_species_present, activity_list, kind, id_)
    elif node_type == EntityType.Vessel_Ferry_Cargo:
        return FerryCargoVessel(metadata, flag_country, name, id_)
    elif node_type == EntityType.Vessel_Ferry_Passenger:
        return FerryPassengerVessel(metadata, flag_country, name, id_)
    elif node_type == EntityType.Vessel_FishingVessel:
        return FishingVessel(metadata, flag_country, company, tonnage, name, length_overall, id_)
    elif node_type == EntityType.Vessel_CargoVessel:
        return CargoVessel(metadata, flag_country, tonnage, name, length_overall, id_)
    elif node_type == EntityType.Vessel_Tour:
        return TourVessel(metadata, flag_country, name, id_)
    elif node_type == EntityType.Vessel_Research:
        return ResearchVessel(metadata, flag_country, name, id_)
    elif node_type == EntityType.Vessel_Other:
        return OtherVessel(metadata, flag_country, name, length_overall, id_)
    elif node_type == EntityType.Document_DeliveryReport:
        return DeliveryReport(metadata, qty_tons, date, id_)
    else:
        raise 'Error parsing node'
