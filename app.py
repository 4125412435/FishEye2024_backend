from flask import Flask, request, jsonify
import service
from data_parser import EntityType, EventType
from flask_cors import CORS, cross_origin

app = Flask(__name__)
CORS(app)
service.initialize('./data/MC2/mc2.json', geo_file_path='./data/MC2/Oceanus Information/Oceanus Geography.geojson')


@app.route('/mc2/select_transponder_ping')
def select_transponder_ping():  # put application's code here
    data = request.get_json()
    start_time = data['startTime']
    end_time = data['endTime']
    vessel_id = data['selectedBoat']
    company = data['selectedCompany']

    results = service.select_edge_attribute(EventType.TransportEvent_TransponderPing, attribute='time',
                                            func=lambda x: start_time <= x <= end_time
                                            )

    path = []
    for ping in results:
        if ping.target == vessel_id:
            location_id = ping.source
            geo_feature = service.select_geo_by_id(location_id)
            time = ping.time
            x, y = geo_feature.center()
            path.append({'time': time, 'point': [x, y]})
    result = [{
        'vessel': vessel_id,
        'path': path
    }]
    return jsonify(result)


if __name__ == '__main__':
    app.run()
