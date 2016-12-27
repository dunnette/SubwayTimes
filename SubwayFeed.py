from google.transit import gtfs_realtime_pb2
import urllib
import datetime
import zipfile
import StringIO
import csv
import sqlite3

# http://datamine.mta.info/sites/all/files/pdfs/GTFS-Realtime-NYC-Subway%20version%201%20dated%207%20Sep.pdf
# http://datamine.mta.info/list-of-feeds

class SubwayFeed:
    endpoint_url = 'http://datamine.mta.info/mta_esi.php'
    static_data_cache = '/Users/dunnette/Downloads/google_transit.zip'
    static_data_url = 'http://web.mta.info/developers/data/nyct/subway/google_transit.zip'
    sqlite_db = 'subway_status.db'
    
    def __init__(self, key_str, feed_id_int):
        self.refresh_feed(key_str, feed_id_int)
        self.process_feed()
        self.process_stations()
    
    def get_stop_names(self, refresh = False):
        if refresh:
            url = urllib.urlopen(self.static_data_url)
            f = StringIO.StringIO(url.read())
        else:
            f = self.static_data_cache
        reader = csv.DictReader(zipfile.ZipFile(f).open('stops.txt'))
        stop_names = dict()
        for row in reader:
            stop_names[row['stop_id']] = row['stop_name']
        self.stop_names = stop_names
    
    def refresh_feed(self, key_str, feed_id_int):
        payload  = urllib.urlencode({'key': key_str, 'feed_id': feed_id_int})
        response = urllib.urlopen('{}?{}'.format(self.endpoint_url, payload))
        self.feed = gtfs_realtime_pb2.FeedMessage()
        self.feed.ParseFromString(response.read())
        
    def process_feed(self):
        self.trip_updates = [tu for tu in self.feed.entity if tu.HasField('trip_update')]
        self.vehicles = [tu for tu in self.feed.entity if tu.HasField('vehicle')]
        self.header = self.feed.header
        
    def process_stations(self):
        station_arrivals = dict()
        for entity in self.trip_updates:
            for stu in entity.trip_update.stop_time_update:
                route_id = entity.trip_update.trip.route_id
                direction_id = stu.stop_id[-1]
                stop_id = stu.stop_id[:-1]
                arrival_time = datetime.datetime.fromtimestamp(stu.departure.time) if stu.departure.time else datetime.datetime.fromtimestamp(stu.arrival.time)
                temp_stops = station_arrivals.get(route_id, dict())
                temp_directions = temp_stops.get(stop_id, dict())
                temp_arrivals = temp_directions.get(direction_id, list())
                temp_arrivals.append(arrival_time)
                temp_directions[direction_id] = temp_arrivals
                temp_stops[stop_id] = temp_directions
                station_arrivals[route_id] = temp_stops
        self.station_arrivals = station_arrivals
    
    def create_stops_table(self):
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()
        sql_command = 'DROP TABLE stop_status;'
        cursor.execute(sql_command)
        sql_command = """
        CREATE TABLE stop_status ( 
        entity_id INTEGER NOT NULL, 
        trip_id TEXT NOT NULL, 
        trip_start_date TEXT NOT NULL, 
        route_id TEXT NOT NULL, 
        stop_id TEXT NOT NULL,
        direction_id TEXT NOT NULL,
        schedule_relationship INTEGER NOT NULL,
        arrival_time INTEGER,
        departure_time INTEGER,
        load_ts TEXT NOT NULL,
        update_ts INTEGER NOT NULL);"""
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
    
    def process_stops(self):
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()

        for entity in self.trip_updates:
            for stu in entity.trip_update.stop_time_update:
                sql_command = """INSERT INTO stop_status (
                entity_id, 
                trip_id, 
                trip_start_date, 
                route_id, 
                stop_id, 
                direction_id, 
                schedule_relationship, 
                arrival_time, 
                departure_time, 
                load_ts, 
                update_ts
                ) VALUES ({}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, CURRENT_TIMESTAMP);""".format(
                    int(entity.id), 
                    entity.trip_update.trip.trip_id,
                    datetime.datetime.strptime(entity.trip_update.trip.start_date,'%Y%m%d'),
                    entity.trip_update.trip.route_id, 
                    stu.stop_id, 
                    stu.stop_id[-1], 
                    stu.schedule_relationship, 
                    stu.arrival.time if stu.arrival.time > 0 else 'NULL', 
                    stu.departure.time if stu.departure.time > 0 else 'NULL',
                    self.header.timestamp
                )
                cursor.execute(sql_command)

        connection.commit()
        connection.close()
    
    def print_time_to_station(self, route_id, stop_id, direction_id):
        for arrival_time in self.station_arrivals[route_id][stop_id][direction_id]:
            print '{0:.1f} minutes to arrival'.format((arrival_time-datetime.datetime.now()).total_seconds()/60)
