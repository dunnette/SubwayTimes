# SubwayTimes

## Summary
A simple interface to cache MTA realtime results in a SQLite database.

## Sample

Generate SQLite database.
```python
g = GTFS_Ingestor(##API_KEY##, regen_stops = True, regen_trip_updates = True, regen_vehicles = True)
```

Update SQLite database
```python
g.update_trip_updates_table(replace = True)
g.update_vehicles_table(replace = True)
```

Initialize reader
```python
r = MTA_Reader()
```

Find three closest stations
```python
[r.get_stop_name(s[0]) for s in r.get_closest_stations((40.752320, -74.006836), n = 3)]
```

Find time of next arrival
```python
r.time_to_next_arrival('L01N')
```

## Reference
- [API Key Registration](http://datamine.mta.info/user/register)
- [GTFS Realtime Overview](https://developers.google.com/transit/gtfs-realtime/)
- [MTA Feed Documentation](http://datamine.mta.info/feed-documentation)
- [MTA List of Feeds](http://datamine.mta.info/list-of-feeds)
- [MTA GTFS Realtime Reference](http://datamine.mta.info/sites/all/files/pdfs/GTFS-Realtime-NYC-Subway%20version%201%20dated%207%20Sep.pdf)
