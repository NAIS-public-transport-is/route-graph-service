@echo off

set HOST=localhost:50052
set G="grpcurl.exe"
echo =====================================================
echo Starting demo of all CRUD operations and complex RPCs
echo =====================================================
echo.

REM ---------- STOPS ----------
echo --- STOPS: Create S900
%G% -plaintext -d "{\"id\":\"S900\",\"name\":\"Demo Stop 900\",\"lat\":45.33,\"lon\":19.85,\"zone\":\"1\",\"shelter\":true}" %HOST% routegraph.RouteGraph.CreateStop
echo.

echo --- STOPS: Get S900
%G% -plaintext -d "{\"id\":\"S900\"}" %HOST% routegraph.RouteGraph.GetStop
echo.

echo --- STOPS: Update S900 (change name)
%G% -plaintext -d "{\"id\":\"S900\",\"name\":\"Demo Stop 900 updated\",\"lat\":45.33,\"lon\":19.85,\"zone\":\"1\",\"shelter\":false}" %HOST% routegraph.RouteGraph.UpdateStop
echo.

echo --- STOPS: Get S900 after update
%G% -plaintext -d "{\"id\":\"S900\"}" %HOST% routegraph.RouteGraph.GetStop
echo.

echo --- STOPS: Delete S900
%G% -plaintext -d "{\"id\":\"S900\"}" %HOST% routegraph.RouteGraph.DeleteStop
echo.

echo --- STOPS: Get S900 (should error / not found)
%G% -plaintext -d "{\"id\":\"S900\"}" %HOST% routegraph.RouteGraph.GetStop
echo.
pause

REM ---------- LINES ----------
echo --- LINES: Create L900
%G% -plaintext -d "{\"id\":\"L900\",\"name\":\"Demo Line 900\",\"mode\":\"BUS\",\"frequency_mins\":15,\"active\":true}" %HOST% routegraph.RouteGraph.CreateLine
echo.

echo --- LINES: Get L900
%G% -plaintext -d "{\"id\":\"L900\"}" %HOST% routegraph.RouteGraph.GetLine
echo.

echo --- LINES: Update L900
%G% -plaintext -d "{\"id\":\"L900\",\"name\":\"Demo Line 900 v2\",\"mode\":\"TRAM\",\"frequency_mins\":10,\"active\":false}" %HOST% routegraph.RouteGraph.UpdateLine
echo.

echo --- LINES: Get L900 after update
%G% -plaintext -d "{\"id\":\"L900\"}" %HOST% routegraph.RouteGraph.GetLine
echo.

echo --- LINES: Delete L900
%G% -plaintext -d "{\"id\":\"L900\"}" %HOST% routegraph.RouteGraph.DeleteLine
echo.

echo --- LINES: Get L900 (should error / not found)
%G% -plaintext -d "{\"id\":\"L900\"}" %HOST% routegraph.RouteGraph.GetLine
echo.
pause

REM ---------- VEHICLES ----------
echo --- VEHICLES: Create V900
%G% -plaintext -d "{\"vehicle_uuid\":\"V900\",\"id\":\"V900\",\"capacity\":60,\"status\":\"IDLE\",\"last_seen_ts\":0,\"last_known_lat\":45.30,\"last_known_lon\":19.80}" %HOST% routegraph.RouteGraph.CreateVehicle
echo.

echo --- VEHICLES: Get V900
%G% -plaintext -d "{\"id\":\"V900\"}" %HOST% routegraph.RouteGraph.GetVehicle
echo.

echo --- VEHICLES: Update V900 (status ACTIVE)
%G% -plaintext -d "{\"vehicle_uuid\":\"V900\",\"id\":\"V900\",\"capacity\":60,\"status\":\"ACTIVE\",\"last_seen_ts\":0,\"last_known_lat\":45.30,\"last_known_lon\":19.80}" %HOST% routegraph.RouteGraph.UpdateVehicle
echo.

echo --- VEHICLES: Get V900 after update
%G% -plaintext -d "{\"id\":\"V900\"}" %HOST% routegraph.RouteGraph.GetVehicle
echo.

echo --- VEHICLES: Create V901 (for parked demo)
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"id\":\"V901\",\"capacity\":50,\"status\":\"IDLE\",\"last_seen_ts\":0,\"last_known_lat\":45.31,\"last_known_lon\":19.81}" %HOST% routegraph.RouteGraph.CreateVehicle
echo.

echo --- VEHICLES: Delete V900
%G% -plaintext -d "{\"id\":\"V900\"}" %HOST% routegraph.RouteGraph.DeleteVehicle
echo.

echo --- VEHICLES: Get V900 (should error)
%G% -plaintext -d "{\"id\":\"V900\"}" %HOST% routegraph.RouteGraph.GetVehicle
echo.
pause

REM ---------- DEPOTS ----------
echo --- DEPOTS: Create D900
%G% -plaintext -d "{\"id\":\"D900\",\"name\":\"Demo Depot 900\",\"lat\":45.40,\"lon\":19.90,\"capacity\":20}" %HOST% routegraph.RouteGraph.CreateDepot
echo.

echo --- DEPOTS: Get D900
%G% -plaintext -d "{\"id\":\"D900\"}" %HOST% routegraph.RouteGraph.GetDepot
echo.

echo --- DEPOTS: Update D900
%G% -plaintext -d "{\"id\":\"D900\",\"name\":\"Demo Depot 900 v2\",\"lat\":45.41,\"lon\":19.91,\"capacity\":25}" %HOST% routegraph.RouteGraph.UpdateDepot
echo.

echo --- DEPOTS: Get D900 after update
%G% -plaintext -d "{\"id\":\"D900\"}" %HOST% routegraph.RouteGraph.GetDepot
echo.

echo --- DEPOTS: Delete D900
%G% -plaintext -d "{\"id\":\"D900\"}" %HOST% routegraph.RouteGraph.DeleteDepot
echo.

echo --- DEPOTS: Get D900 (should error)
%G% -plaintext -d "{\"id\":\"D900\"}" %HOST% routegraph.RouteGraph.GetDepot
echo.
pause

REM ---------- EDGES: NEXT ----------
echo --- NEXT: Create edge S1 -> S2 1>&2
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\",\"travel_time\":120,\"distance\":500}" %HOST% routegraph.RouteGraph.CreateNextEdge
echo.

echo --- NEXT: Get edge S1 -> S2 1>&2
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\"}" %HOST% routegraph.RouteGraph.GetNextEdge
echo.

echo --- NEXT: Update edge S1 -> S2 (travel_time -> 150) 1>&2
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\",\"travel_time\":150,\"distance\":500}" %HOST% routegraph.RouteGraph.UpdateNextEdge
echo.

echo --- NEXT: Get edge S1 -> S2 after update 1>&2
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\"}" %HOST% routegraph.RouteGraph.GetNextEdge
echo.

echo --- NEXT: Delete edge S1 -> S2 1>&2
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\"}" %HOST% routegraph.RouteGraph.DeleteNextEdge
echo.

echo --- NEXT: Get edge S1 -> S2 (should error / not found) 1>&2
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\"}" %HOST% routegraph.RouteGraph.GetNextEdge
echo.
pause


REM ---------- EDGES: SERVES ----------
echo --- SERVES: Create edge L1 -> S3 (use existing line L1 and stop S3) 1>&2
%G% -plaintext -d "{\"line_id\":\"L1\",\"stop_id\":\"S3\",\"order\":1}" %HOST% routegraph.RouteGraph.CreateServesEdge
echo.

echo --- SERVES: Get L1 -> S3 1>&2
%G% -plaintext -d "{\"line_id\":\"L1\",\"stop_id\":\"S3\",\"order\":1}" %HOST% routegraph.RouteGraph.GetServesEdge
echo.

echo --- SERVES: Update L1 -> S3 (order -> 5) 1>&2
%G% -plaintext -d "{\"line_id\":\"L1\",\"stop_id\":\"S3\",\"order\":5}" %HOST% routegraph.RouteGraph.UpdateServesEdge
echo.

echo --- SERVES: Get L1 -> S3 after update 1>&2
%G% -plaintext -d "{\"line_id\":\"L1\",\"stop_id\":\"S3\",\"order\":5}" %HOST% routegraph.RouteGraph.GetServesEdge
echo.

echo --- SERVES: Delete L1 -> S3 1>&2
%G% -plaintext -d "{\"line_id\":\"L1\",\"stop_id\":\"S3\",\"order\":0}" %HOST% routegraph.RouteGraph.DeleteServesEdge
echo.

echo --- SERVES: Try Get L1 -> S3 (should error / not foundd) 1>&2
%G% -plaintext -d "{\"line_id\":\"L1\",\"stop_id\":\"S3\",\"order\":0}" %HOST% routegraph.RouteGraph.GetServesEdge
echo.
pause

REM ---------- EDGES: ASSIGNED_TO ----------
echo --- ASSIGNED_TO: Create assignment V901 -> L1 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"line_id\":\"L1\",\"since\": 0}" %HOST% routegraph.RouteGraph.CreateAssignedTo
echo.

echo --- ASSIGNED_TO: Get assignment V901 -> L1 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"line_id\":\"L1\",\"since\":0}" %HOST% routegraph.RouteGraph.GetAssignedTo
echo.

echo --- ASSIGNED_TO: Update assignment V901 -> L1 (since -> 1234567890) 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"line_id\":\"L1\",\"since\":1234567890}" %HOST% routegraph.RouteGraph.UpdateAssignedTo
echo.

echo --- ASSIGNED_TO: Get after update 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"line_id\":\"L1\",\"since\":1234567890}" %HOST% routegraph.RouteGraph.GetAssignedTo
echo.

echo --- ASSIGNED_TO: Delete assignment V901 -> L1 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"line_id\":\"L1\",\"since\":0}" %HOST% routegraph.RouteGraph.DeleteAssignedTo
echo.

echo --- ASSIGNED_TO: Get (should not exist) 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"line_id\":\"L1\",\"since\":0}" %HOST% routegraph.RouteGraph.GetAssignedTo
echo.
pause

REM ---------- EDGES: PARKED_AT ----------
echo --- PARKED_AT: Create parked V901 -> D1 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"depot_id\":\"D1\",\"since\":0}" %HOST% routegraph.RouteGraph.CreateParkedAt
echo.

echo --- PARKED_AT: Get parked V901 -> D1 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"depot_id\":\"D1\",\"since\":0}" %HOST% routegraph.RouteGraph.GetParkedAt
echo.

echo --- PARKED_AT: Update parked V901 -> D1 (since -> 999999999) 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"depot_id\":\"D1\",\"since\":999999999}" %HOST% routegraph.RouteGraph.UpdateParkedAt
echo.

echo --- PARKED_AT: Get after update 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"depot_id\":\"D1\",\"since\":999999999}" %HOST% routegraph.RouteGraph.GetParkedAt
echo.

echo --- PARKED_AT: Delete parked relation V901 -> D1 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"depot_id\":\"D1\",\"since\":0}" %HOST% routegraph.RouteGraph.DeleteParkedAt
echo.

echo --- PARKED_AT: Get (should not exist) 1>&2
%G% -plaintext -d "{\"vehicle_uuid\":\"V901\",\"depot_id\":\"D1\",\"since\":0}" %HOST% routegraph.RouteGraph.GetParkedAt
echo.
pause

REM ---------- COMPLEX RPCs ----------
echo --- COMPLEX: Assign nearest idle vehicle to line L1 1>&2
%G% -plaintext -d "{\"line_id\":\"L1\"}" %HOST% routegraph.RouteGraph.AssignVehicle
echo.

echo --- COMPLEX: RecalibrateEdge S1->S2 observed=180 1>&2
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\",\"travel_time\":120,\"distance\":500}" %HOST% routegraph.RouteGraph.CreateNextEdge
echo.
%G% -plaintext -d "{\"from_id\":\"S1\",\"to_id\":\"S2\",\"observed_avg\":180}" %HOST% routegraph.RouteGraph.RecalibrateEdge
echo.

echo --- COMPLEX: TopPairs limit=5 1>&2
%G% -plaintext -d "{\"limit\":5}" %HOST% routegraph.RouteGraph.TopPairs
echo.

echo --- COMPLEX: DepotsIdleStats limit=3 1>&2
%G% -plaintext -d "{\"limit\":3}" %HOST% routegraph.RouteGraph.DepotsIdleStats
echo.

echo --- COMPLEX: ShortestPath S1 -> S10 max_hops=10 1>&2
%G% -plaintext -d "{\"start_id\":\"S1\",\"end_id\":\"S10\",\"max_hops\":10}" %HOST% routegraph.RouteGraph.ShortestPath
echo.

echo =====================================================
echo Demo complete.
pause
