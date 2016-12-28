import sqlite3
import datetime

class MTA_Reader:
    _sqlite_db = 'subway_status.db'
    
    def get_closest_stations(self, lat_lon, n = 10):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = "select stop_id from stops where parent_station is null order by abs((stop_lat - {lat}) * (stop_lat - {lat}) + (stop_lon - {lon}) * (stop_lon - {lon})) limit {lim};".format(lat = lat_lon[0], lon=lat_lon[1], lim = n)
        cursor.execute(sql_command) 
        return cursor.fetchall()
    
    def time_to_next_arrival(self, stop_id):
        return (sorted(self.get_stop_times(stop_id))[0] - datetime.datetime.now()).total_seconds()
    
    def get_stop_times(self, stop_id, arrival_or_departure = 'arrival'):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = "select {}_time from trip_updates where stop_id = '{}';".format(arrival_or_departure, stop_id)
        cursor.execute(sql_command) 
        return [datetime.datetime.fromtimestamp(r[0]) for r in cursor.fetchall() if r[0] is not None]
        connection.close()
        
    def get_stop_name(self, stop_id):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = "select stop_name from stops where stop_id = '{}';".format(stop_id)
        cursor.execute(sql_command) 
        return [r[0] for r in cursor.fetchall()][0]
        connection.close()
        
    def get_stop_id(self, stop_name):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = "select stop_id from stops where stop_name = '{}';".format(stop_name)
        cursor.execute(sql_command) 
        return [r[0] for r in cursor.fetchall()]
        connection.close()
        
    def get_stops_by_route(self, route_id):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        sql_command = "select distinct stop_id from trip_updates where route_id = '{}';".format(route_id)
        cursor.execute(sql_command) 
        return [r[0] for r in cursor.fetchall()]
        connection.close()
