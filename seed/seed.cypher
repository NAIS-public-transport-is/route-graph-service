// seed_no_apoc.cypher - radi bez APOC (ispravljena verzija)

// constraints
CREATE CONSTRAINT stop_id IF NOT EXISTS FOR (s:Stop) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT line_id IF NOT EXISTS FOR (l:Line) REQUIRE l.id IS UNIQUE;
CREATE CONSTRAINT vehicle_uuid IF NOT EXISTS FOR (v:Vehicle) REQUIRE v.vehicle_uuid IS UNIQUE;
CREATE CONSTRAINT depot_id IF NOT EXISTS FOR (d:Depot) REQUIRE d.id IS UNIQUE;

// depots
UNWIND [
  {id:'D1',name:'North Depot',lat:45.27,lon:19.83,capacity:50},
  {id:'D2',name:'West Depot',lat:45.25,lon:19.80,capacity:40},
  {id:'D3',name:'East Depot',lat:45.28,lon:19.88,capacity:30}
] AS d
CREATE (dep:Depot) SET dep = d;

// stops
UNWIND range(1,100) AS i
CREATE (:Stop {
  id: 'S'+toString(i),
  name:'Stop '+toString(i),
  lat:45.20 + rand()*0.2,
  lon:19.70 + rand()*0.3,
  zone: toString(1+toInteger(rand()*3)),
  shelter: rand() > 0.5
});

// lines
UNWIND range(1,8) AS i
CREATE (:Line {
  id:'L'+toString(i),
  name:'Line '+toString(i),
  mode: CASE WHEN i%3=0 THEN 'TRAM' ELSE 'BUS' END,
  frequency_mins: 5 + toInteger(rand()*20),
  active: true
});

// attach lines to stops with SERVES (with average_boardings), create NEXT between consecutive
MATCH (l:Line)
CALL {
  WITH l
  // pick a random subset of stops per line by ordering stops by rand() then collecting
  MATCH (s:Stop)
  WITH l, s
  ORDER BY rand()
  WITH l, collect(s) AS allStops
  // take first N stops for this line
  WITH l, allStops[0..toInteger(10 + rand()*5)] AS route
  UNWIND range(0, size(route)-1) AS idx
  WITH l, route[idx] AS s, idx
  CREATE (l)-[:SERVES {order: idx+1, average_boardings: toInteger(rand()*50)}]->(s)
}
IN TRANSACTIONS;

// create NEXT between consecutive SERVES per line
// we must preserve r and s across ORDER BY, so include them in WITH before ORDER BY
MATCH (l:Line)-[r:SERVES]->(s:Stop)
WITH l, r, s
ORDER BY l.id, r.order
WITH l, collect(s) AS stops
UNWIND range(0, size(stops)-2) AS i
WITH stops[i] AS a, stops[i+1] AS b
CREATE (a)-[:NEXT {travel_time: 60 + toInteger(rand()*180), distance: 100 + toInteger(rand()*900)}]->(b);

// vehicles (40) - vehicle_uuid is deterministic 'V'+i
UNWIND range(1,40) AS i
CREATE (:Vehicle {
  vehicle_uuid: 'V'+toString(i),
  id:'V'+toString(i),
  capacity: 40 + toInteger(rand()*60),
  status: CASE WHEN rand()<0.2 THEN 'MAINTENANCE' WHEN rand()<0.6 THEN 'IDLE' ELSE 'ACTIVE' END,
  last_seen_ts: timestamp()-toInteger(rand()*86400000),
  last_known_lat:45.20+rand()*0.2,
  last_known_lon:19.70+rand()*0.3
});

// assign active vehicles to lines
MATCH (v:Vehicle) WHERE v.status='ACTIVE'
WITH collect(v) AS vehicles
MATCH (l:Line)
WITH vehicles, collect(l) AS lines
UNWIND range(0, size(vehicles)-1) AS i
WITH vehicles[i] AS v, lines[toInteger(rand()*size(lines))] AS l
CREATE (v)-[:ASSIGNED_TO {since: timestamp()-toInteger(rand()*86400000)}]->(l);

// park idle vehicles at depots
MATCH (v:Vehicle) WHERE v.status='IDLE'
WITH collect(v) AS idle
MATCH (d:Depot)
WITH idle, collect(d) AS deps
UNWIND idle AS v
WITH v, deps[toInteger(rand()*size(deps))] AS d
CREATE (v)-[:PARKED_AT {since: timestamp()-toInteger(rand()*86400000)}]->(d);
