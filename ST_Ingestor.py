from google.transit import gtfs_realtime_pb2
import urllib
import datetime
import time
import zipfile
import StringIO
import csv
import sqlite3

class Ingestor:
    _endpoint_url = 'http://datamine.mta.info/mta_esi.php'
    _static_data_url = 'http://web.mta.info/developers/data/nyct/subway/google_transit.zip'
    _sqlite_db = 'subway_status.db'
    _feed_freq = 60
    _persist_limit = 5*60
    
    def __init__(self, key_str, regen_stops = False, regen_trip_updates = False, regen_vehicles = False):
        self._key_str = key_str
        if regen_stops:
            self._initialize_stops_table()
        if regen_trip_updates:
            self._initialize_trip_updates_table()
        if regen_vehicles:
            self._initialize_vehicles_table()
    
    def _initialize_feed(self, feed_id):
        self._load_feed(feed_id)
        self._split_feed()
    
    def _load_feed(self, feed_id_int):
        payload  = urllib.urlencode({'key': self._key_str, 'feed_id': feed_id_int})
        response = urllib.urlopen('{}?{}'.format(self._endpoint_url, payload))
        self._feed = gtfs_realtime_pb2.FeedMessage()
        self._feed.ParseFromString(response.read())
        
    def _split_feed(self):
        self._trip_updates = [tu for tu in self._feed.entity if tu.HasField('trip_update')]
        self._vehicles = [tu for tu in self._feed.entity if tu.HasField('vehicle')]
        self._header = self._feed.header
        
    def _drop_table(self, table_name):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = 'DROP TABLE {};'.format(table_name)
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
        
    def _initialize_stops_table(self):
        try:
            self._drop_table('stops')
        except:
            pass
        self._create_stops_table()
    
    def _create_stops_table(self):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = """
        CREATE TABLE stops ( 
        stop_id TEXT NOT NULL,
        stop_code TEXT,
        stop_name TEXT NOT NULL,
        stop_desc TEXT,
        stop_lat REAL NOT NULL,
        stop_lon REAL NOT NULL,
        zone_id TEXT,
        stop_url TEXT,
        location_type TEXT NOT NULL,
        parent_station TEXT,
        update_ts TEXT NOT NULL);"""
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
    
    def _populate_stops_table(self):
        url = urllib.urlopen(self._static_data_url)
        f = StringIO.StringIO(url.read())
        reader = csv.DictReader(zipfile.ZipFile(f).open('stops.txt'))
        self._stops_update_ts = datetime.datetime.now()
        def wrap_text(s): return s if s else None
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            args = (
                wrap_text(row['stop_id']),
                wrap_text(row['stop_code']),
                wrap_text(row['stop_name']),
                wrap_text(row['stop_desc']),
                wrap_text(row['stop_lat']),
                wrap_text(row['stop_lon']),
                wrap_text(row['zone_id']),
                wrap_text(row['stop_url']),
                wrap_text(row['location_type']),
                wrap_text(row['parent_station']),
                self._stops_update_ts)
            cursor.execute(sql_command, args)
        connection.commit()
        connection.close()
        
    def update_stops_table(self):
        self._initialize_stops_table()
        self._populate_stops_table()
    
    def _initialize_vehicles_table(self):
        try:
            self._drop_table('vehicles')
        except:
            pass
        self._create_vehicles_table()
        
    def _create_vehicles_table(self):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = """
        CREATE TABLE vehicles ( 
        entity_id INTEGER NOT NULL, 
        trip_id TEXT NOT NULL, 
        trip_start_date TEXT NOT NULL, 
        route_id TEXT NOT NULL, 
        current_stop_sequence INTEGER NOT NULL,
        current_status INTEGER NOT NULL,
        status_update_time INTEGER NOT NULL,
        load_ts INTEGER NOT NULL,
        update_ts TEXT NOT NULL);"""
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
        
    def _populate_vehicles_table(self):
        def wrap_text(s): return s if s else None
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        for entity in self._vehicles:
            sql_command = """INSERT INTO vehicles (
            entity_id, 
            trip_id, 
            trip_start_date, 
            route_id, 
            current_stop_sequence, 
            current_status, 
            status_update_time, 
            load_ts, 
            update_ts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            args = (
                wrap_text(entity.id), 
                wrap_text(entity.vehicle.trip.trip_id),
                wrap_text(datetime.datetime.strptime(entity.vehicle.trip.start_date,'%Y%m%d')),
                wrap_text(entity.vehicle.trip.route_id), 
                entity.vehicle.current_stop_sequence, 
                entity.vehicle.current_status, 
                wrap_text(entity.vehicle.timestamp),
                self._header.timestamp,
                self._feed_update_ts)
            cursor.execute(sql_command, args)
        connection.commit()
        connection.close()

    def _initialize_trip_updates_table(self):
        try:
            self._drop_table('trip_updates')
        except:
            pass
        self._create_trip_updates_table()
            
    def _create_trip_updates_table(self):
        connection = sqlite3.connect(self._sqlite_db)
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
        load_ts INTEGER NOT NULL,
        update_ts TEXT NOT NULL);"""
        cursor.execute(sql_command)
        connection.commit()
        connection.close()
        
    def _populate_trip_updates_table(self):
        def wrap_text(s): return s if s else None
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        for entity in self._trip_updates:
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                args = (
                    wrap_text(entity.id),
                    wrap_text(entity.trip_update.trip.trip_id),
                    wrap_text(datetime.datetime.strptime(entity.trip_update.trip.start_date,'%Y%m%d')),
                    wrap_text(entity.trip_update.trip.route_id), 
                    wrap_text(stu.stop_id),
                    wrap_text(stu.stop_id[-1]), 
                    stu.schedule_relationship, 
                    wrap_text(stu.arrival.time), 
                    wrap_text(stu.departure.time),
                    self._header.timestamp,
                    self._feed_update_ts)
                cursor.execute(sql_command, args)
        connection.commit()
        connection.close()

    def update_feed_tables(self, feed_ids, replace = False):
        if replace:
            del self._header
            self._initialize_vehicles_table()
            self._initialize_trip_updates_table()
        if self.is_feed_stale():
            pass
        else:
            self._feed_update_ts = datetime.datetime.now()
            for feed_id in feed_ids:
                self._initialize_feed(feed_id)
                self._populate_vehicles_table()
                self._populate_trip_updates_table()
            self._clean_feed_table()

    def is_feed_stale(self):
        return hasattr(self, '_header') and time.time() - self._header.timestamp < self._feed_freq

    def _clean_feed_table(self):
        oldest_record = time.time() - self._persist_limit
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        cursor.execute('DELETE FROM trip_updates WHERE load_ts < ?', (oldest_record,))
        cursor.execute('DELETE FROM vehicles WHERE load_ts < ?', (oldest_record,))
        connection.commit()
        connection.close()
