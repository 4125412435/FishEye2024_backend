class GeoFeature:
    def __init__(self, geo_name, polygon):
        self.id = geo_name
        self.polygon = polygon

    def center(self):
        x, y = 0, 0
        for p in self.polygon:
            x += p[0]
            y += p[1]
        return x / len(self.polygon), y / len(self.polygon)


def parse_geo_object(json):
    # 有些地区在geojson里面没有id,只能用name去对应
    geo_name = json['properties']['Name']
    geo_type = json['geometry']['type']
    # 如果是point就只有一个值 polygon就有多个值
    polygon = json['geometry']['coordinates']
    return GeoFeature(geo_name, polygon)
