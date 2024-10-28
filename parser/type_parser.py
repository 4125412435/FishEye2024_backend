from enum import Enum


class EntityType(Enum):
    Vessel_CargoVessel = 'Entity.Vessel.CargoVessel'
    Vessel_Ferry_Passenger = 'Entity.Vessel.Ferry.Passenger'
    Vessel_Ferry_Cargo = 'Entity.Vessel.Ferry.Cargo'
    Vessel_FishingVessel = 'Entity.Vessel.FishingVessel'
    Vessel_Other = 'Entity.Vessel.Other'
    Vessel_Research = 'Entity.Vessel.Research'
    Vessel_Tour = 'Entity.Vessel.Tour'

    Commodity_Fish = 'Entity.Commodity.Fish'

    Location_Point = 'Entity.Location.Point'
    Location_City = 'Entity.Location.City'
    Location_Region = 'Entity.Location.Region'

    Document_DeliveryReport = 'Entity.Document.DeliveryReport'


class EventType(Enum):
    Transaction = 'Event.Transaction'
    HarborReport = 'Event.HarborReport'
    TransportEvent_TransponderPing = 'Event.TransportEvent.TransponderPing'


def parse_type(typ):
    for e in EventType:
        if e.value == typ:
            return e
    for e in EntityType:
        if e.value == typ:
            return e
    return None
