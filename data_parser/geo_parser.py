class GeoFeature:
    def __init__(self, geo_name, geo_type, polygon):
        self.id = geo_name
        self.type = geo_type
        self.polygon = polygon

    def center(self):
        x, y = 0, 0
        if self.type == 'Polygon':
            for p in self.polygon[0]:
                x += p[0]
                y += p[1]
            x, y = x / len(self.polygon[0]), y / len(self.polygon[0])
        elif self.type == 'Point':
            x += self.polygon[0]
            y += self.polygon[1]
        return x, y


def parse_geo_object(json):
    # 有些地区在geojson里面没有id,只能用name去对应
    geo_name = json['properties']['Name']
    geo_type = json['geometry']['type']
    # 如果是point就只有一个值 polygon就有多个值
    polygon = json['geometry']['coordinates']
    return GeoFeature(geo_name, geo_type, polygon)

