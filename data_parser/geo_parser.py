class GeoFeature:
    def __init__(self, geo_name, geo_type, polygon):
        self.id = geo_name
        self.type = geo_type
        self.polygon = polygon

    def center(self):
        centroid_x, centroid_y = 0, 0
        if self.type == 'Polygon':
            area = 0
            centroid_x = 0
            centroid_y = 0
            num_points = len(self.polygon[0])

            for i in range(num_points):
                x1, y1 = self.polygon[0][i]
                x2, y2 = self.polygon[0][(i + 1) % num_points]
                partial_area = x1 * y2 - x2 * y1
                area += partial_area
                centroid_x += (x1 + x2) * partial_area
                centroid_y += (y1 + y2) * partial_area

            area /= 2
            centroid_x /= (6 * area)
            centroid_y /= (6 * area)


        elif self.type == 'Point':
            centroid_x += self.polygon[0]
            centroid_y += self.polygon[1]
        return centroid_x, centroid_y


def parse_geo_object(json):
    # 有些地区在geojson里面没有id,只能用name去对应
    geo_name = json['properties']['Name']
    geo_type = json['geometry']['type']
    # 如果是point就只有一个值 polygon就有多个值
    polygon = json['geometry']['coordinates']
    return GeoFeature(geo_name, geo_type, polygon)

