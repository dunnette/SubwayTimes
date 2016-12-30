import sqlite3
import datetime

class Reader:
    _sqlite_db = 'subway_status.db'
    
    def _query_dbase(self, q, a = ''):
        connection = sqlite3.connect(self._sqlite_db)
        cursor = connection.cursor()
        cursor.execute(q, a)
        result = cursor.fetchall()
        connection.close()
        return result
    
    def get_last_update(self, table):
        if table in ['stops', 'trip_updates', 'vehicles']:
            sql_command = "select max(update_ts) from {};".format(table)
            return [row[0] for row in self._query_dbase(sql_command)][0]
        return []
    
    def get_routes(self):
        sql_command = "select distinct route_id from trip_updates;"
        return [row[0] for row in self._query_dbase(sql_command)]
    
    def get_stops(self, routes):
        sql_command = "select distinct stop_id from trip_updates where route_id in ('{}');".format("','".join(routes))
        return [row[0] for row in self._query_dbase(sql_command)]
    
    def get_stop_times(self, stop_ids, arrival_or_departure = 'departure'):
        sql_command = "select {}_time from trip_updates where stop_id in ('{}') and update_ts = '{}';".format(arrival_or_departure, "','".join(stop_ids), self.get_last_update('trip_updates'))
        return [datetime.datetime.fromtimestamp(row[0]) for row in self._query_dbase(sql_command)]
    
    def get_next_stop_time(self, stop_id):
        return sorted(self.get_stop_times([stop_id]))[0]
    
    def get_closest_stations(self, lat_lon, n = 10):
        sql_command = "select distinct stop_id from stops where parent_station is null order by abs((stop_lat - ?) * (stop_lat - ?) + (stop_lon - ?) * (stop_lon - ?)) limit ?;"
        return [row[0] for row in self._query_dbase(sql_command, a = (lat_lon[0], lat_lon[0], lat_lon[1], lat_lon[1], n))]
    
    def get_stop_name(self, stop_id):
        sql_command = "select distinct stop_name from stops where stop_id = ?;"
        return [row[0] for row in self._query_dbase(sql_command, a = (stop_id,))]
        
    def get_stop_ids(self, stop_name):
        sql_command = "select distinct stop_id from stops where stop_name = ?;"
        return [row[0] for row in self._query_dbase(sql_command, a = (stop_name,))]
