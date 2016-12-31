import ST_Ingestor
import st_api_cred
import time
import sched

def periodic():
	delay = 30 if g.is_feed_stale() else 2
	print(delay)
	g.update_feed_tables([1,2])
	s.enter(delay, 1, periodic, ())

if __name__ == '__main__':
	g = ST_Ingestor.Ingestor(st_api_cred.login['API_KEY'], regen_stops = True, regen_trip_updates = True, regen_vehicles = True)
	g.update_stops_table()

	s = sched.scheduler(time.time, time.sleep)

	s.enter(0, 1, periodic, ())
	s.run()
