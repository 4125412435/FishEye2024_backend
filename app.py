import time

from flask import Flask, request, jsonify
import service
from data_parser import EntityType, EventType
from flask_cors import CORS, cross_origin
from dateutil import parser

if __name__ == 'app':
    time1 = time.time()
    service.initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson',
                       num_workers=10)
    time2 = time.time()
    print('load node:', len(service.node_list))
    print('load edge:', len(service.edge_list))
    print('cost time:', time2 - time1, 's')

app = Flask(__name__)
CORS(app)

@app.route('/mc2/select_transponder_ping', methods=['POST'])
def select_transponder_ping():  # put application's code here
    data = request.get_json()
    # data = {'startTime': '2035-09-15', 'endTime': '2035-9-29', 'queryType': '1', 'selectedCompany': 'WestRiver Shipping KgaA', 'selectedBoat': 'perchplundererbc0'}

    start_time = parser.parse(data['startTime'])
    end_time = parser.parse(data['endTime'])
    vessel_id = data['selectedBoat']
    company = data['selectedCompany']

    results = service.select_edge_attribute(EventType.TransportEvent_TransponderPing, attribute='time',
                                            func=lambda x: start_time <= x <= end_time
                                            )

    vessel_id_list = [vessel_id] if vessel_id is not '' else [vessel.id for vessel in
                                                                service.select_fishing_vessel_by_company(company)]

    result = []
    for vessel_id in vessel_id_list:
        path = []
        for ping in results:
            if ping.target == vessel_id:
                location_id = ping.source
                geo_feature = service.select_geo_by_id(location_id)
                time = ping.time
                x, y = geo_feature.center()
                path.append({'time': time, 'point': [x, y]})
        result.append({
            'vessel': vessel_id,
            'path': path
        })
    for r in result:
        r['path'] = sorted(r['path'], key=lambda t: t['time'])
    return jsonify(result)


@app.route('/mc2/select_dwell', methods=['POST'])
def select_dwell():  # put application's code here
    data = request.get_json()
    # data = {'startTime': '2035-09-15', 'endTime': '2035-9-29', 'queryType': '1', 'selectedCompany': 'WestRiver Shipping KgaA', 'selectedBoat': 'perchplundererbc0'}

    start_time = parser.parse(data['startTime'])
    end_time = parser.parse(data['endTime'])
    vessel_id = data['selectedBoat']
    company = data['selectedCompany']

    results = service.select_edge_attribute(EventType.TransportEvent_TransponderPing, attribute='time',
                                            func=lambda x: start_time <= x <= end_time
                                            )

    vessel_id_list = [vessel_id] if vessel_id is not '' else [vessel.id for vessel in
                                                                service.select_fishing_vessel_by_company(company)]

    result = []
    for vessel_id in vessel_id_list:
        dwell_time = {}
        for ping in results:
            if ping.target == vessel_id:
                location_id = ping.source
                dwell = ping.dwell
                geo_feature = service.select_geo_by_id(location_id)
                x, y = geo_feature.center()
                if location_id in dwell_time.keys():
                    dwell_time[location_id]['time'] += dwell
                else:
                    dwell_time[location_id] = {'time': 0, 'x': x, 'y': y}
        result.append({
            'vessel': vessel_id,
            'dwell': dwell_time
        })
    return jsonify(result)


if __name__ == '__main__':
    app.run()
