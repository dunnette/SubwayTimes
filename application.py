import ST_Reader
import flask

r = ST_Reader.Reader()

application = flask.Flask(__name__)

@application.route('/routes', methods=['GET'])
def get_routes():
    return flask.jsonify({'data': r.get_routes(), 'update': r.get_last_update('trip_updates')})

@application.route('/<route_id>/stops', methods=['GET'])
def get_stops(route_id):
    return flask.jsonify({'data': r.get_stops([route_id]), 'update': r.get_last_update('trip_updates')})

@application.route('/<stop_id>/times', methods=['GET'])
def get_stop_times(stop_id):
	return flask.jsonify({'data': r.get_stop_times([stop_id]), 'update': r.get_last_update('trip_updates')})

@application.route('/<stop_id>/times/next', methods=['GET'])
def get_next_stop_time(stop_id):
	return flask.jsonify({'data': r.get_next_stop_time(stop_id), 'update': r.get_last_update('trip_updates')})

@application.route('/<stop_id>/name', methods=['GET'])
def get_stop_name(stop_id):
	return flask.jsonify({'data': r.get_stop_name(stop_id), 'update': r.get_last_update('stops')})

if __name__ == '__main__':
    application.run()
