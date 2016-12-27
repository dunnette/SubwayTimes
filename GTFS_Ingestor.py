from google.transit import gtfs_realtime_pb2
import urllib
import datetime
import zipfile
import StringIO
import csv
import sqlite3

class GTFS_Ingestor:
    endpoint_url = 'http://datamine.mta.info/mta_esi.php'
    static_data_url = 'http://web.mta.info/developers/data/nyct/subway/google_transit.zip'
    sqlite_db = 'subway_status.db'
    
    def __init__(self, key_str):
        self.key_str = key_str
    
    def load_feed(self, feed_id_int):
        payload  = urllib.urlencode({'key': self.key_str, 'feed_id': feed_id_int})
        response = urllib.urlopen('{}?{}'.format(self.endpoint_url, payload))
        self.feed = gtfs_realtime_pb2.FeedMessage()
        self.feed.ParseFromString(response.read())
        
    def split_feed(self):
        self.trip_updates = [tu for tu in self.feed.entity if tu.HasField('trip_update')]
        self.vehicles = [tu for tu in self.feed.entity if tu.HasField('vehicle')]
        self.header = self.feed.header
        
    def create_stops_table(self):
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()
        sql_command = """
        CREATE TABLE stops ( 
        stop_id TEXT NOT NULL,
        stop_code TEXT,
        stop_name TEXT NOT NULL,
        stop_desc TEXT,
        stop_lat REAL,
        stop_lon REAL,
        zone_id TEXT,
        stop_url TEXT,
        location_type TEXT,
        parent_station TEXT,
        update_ts TEXT NOT NULL);"""
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
        
    def replace_stops_table(self):
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()
        sql_command = 'DROP TABLE stops;'
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
        self.create_stops_table()
        
    def populate_stops_table(self):
        url = urllib.urlopen(self.static_data_url)
        f = StringIO.StringIO(url.read())
        reader = csv.DictReader(zipfile.ZipFile(f).open('stops.txt'))
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()
        update_time = datetime.datetime.now()
        for row in reader:
            sql_command = """INSERT INTO stops (
            stop_id,
            stop_code,
            stop_name,
            stop_desc,
            stop_lat,
            stop_lon,
            zone_id,
            stop_url,
            location_type,
            parent_station,
            update_ts
            ) VALUES ('{}', '{}', "{}", '{}', {}, {}, '{}', '{}', '{}', '{}', '{}');""".format(
                row['stop_id'], 
                row['stop_code'],
                row['stop_name'],
                row['stop_desc'],
                row['stop_lat'],
                row['stop_lon'],
                row['zone_id'],
                row['stop_url'],
                row['location_type'],
                row['parent_station'],
                update_time
            )
            cursor.execute(sql_command)
        connection.commit()
        connection.close()
        
    def replace_trip_updates_table(self):
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()
        sql_command = 'DROP TABLE trip_updates;'
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
        self.create_trip_updates_table()
        
    def create_trip_updates_table(self):
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()
        sql_command = """
        CREATE TABLE trip_updates ( 
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
        update_ts TEXT NOT NULL);"""
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
        
    def populate_trip_updates_table(self):
        connection = sqlite3.connect(self.sqlite_db)
        cursor = connection.cursor()
        update_time = datetime.datetime.now()

        for entity in self.trip_updates:
            for stu in entity.trip_update.stop_time_update:
                sql_command = """INSERT INTO trip_updates (
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
                ) VALUES ({}, '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}');""".format(
                    int(entity.id), 
                    entity.trip_update.trip.trip_id,
                    datetime.datetime.strptime(entity.trip_update.trip.start_date,'%Y%m%d'),
                    entity.trip_update.trip.route_id, 
                    stu.stop_id, 
                    stu.stop_id[-1], 
                    stu.schedule_relationship, 
                    stu.arrival.time if stu.arrival.time > 0 else 'NULL', 
                    stu.departure.time if stu.departure.time > 0 else 'NULL',
                    self.header.timestamp,
                    update_time
                )
                cursor.execute(sql_command)

        connection.commit()
        connection.close()
