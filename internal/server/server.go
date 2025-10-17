package server

import (
	"context"
	"fmt"
	"time"

	"route-graph-service/internal/repo"
	pb "route-graph-service/proto/routegraph"
	helper "route-graph-service/util"

	"github.com/jung-kurt/gofpdf"
)

type Server struct {
	pb.UnimplementedRouteGraphServer
	repo *repo.NeoRepo
}

func NewServer(r *repo.NeoRepo) *Server {
	return &Server{repo: r}
}

/* Stops */
func (s *Server) CreateStop(ctx context.Context, in *pb.Stop) (*pb.Stop, error) {
	if in == nil || in.Id == "" {
		return nil, fmt.Errorf("invalid stop")
	}
	err := s.repo.CreateStop(ctx, in.Id, in.Name, in.Lat, in.Lon, in.Zone, in.Shelter)
	if err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) GetStop(ctx context.Context, in *pb.ID) (*pb.Stop, error) {
	m, err := s.repo.GetStop(ctx, in.Id)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	return &pb.Stop{
		Id:   m["id"].(string),
		Name: m["name"].(string),
		Lat:  m["lat"].(float64),
		Lon:  m["lon"].(float64),
		Zone: m["zone"].(string),
	}, nil
}
func (s *Server) UpdateStop(ctx context.Context, in *pb.Stop) (*pb.Stop, error) {
	props := map[string]any{"id": in.Id, "name": in.Name, "lat": in.Lat, "lon": in.Lon, "zone": in.Zone, "shelter": in.Shelter}
	if err := s.repo.UpdateStop(ctx, props); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) DeleteStop(ctx context.Context, in *pb.ID) (*pb.Empty, error) {
	if err := s.repo.DeleteStop(ctx, in.Id); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

/* Lines */
func (s *Server) CreateLine(ctx context.Context, in *pb.Line) (*pb.Line, error) {
	if in == nil || in.Id == "" {
		return nil, fmt.Errorf("invalid line")
	}
	if err := s.repo.CreateLine(ctx, in.Id, in.Name, in.Mode, in.FrequencyMins, in.Active); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) GetLine(ctx context.Context, in *pb.ID) (*pb.Line, error) {
	m, err := s.repo.GetLine(ctx, in.Id)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	return &pb.Line{Id: m["id"].(string), Name: m["name"].(string), Mode: m["mode"].(string)}, nil
}
func (s *Server) UpdateLine(ctx context.Context, in *pb.Line) (*pb.Line, error) {
	props := map[string]any{"id": in.Id, "name": in.Name, "mode": in.Mode, "frequency_mins": in.FrequencyMins, "active": in.Active}
	if err := s.repo.UpdateLine(ctx, props); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) DeleteLine(ctx context.Context, in *pb.ID) (*pb.Empty, error) {
	if err := s.repo.DeleteLine(ctx, in.Id); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

/* Vehicles */
func (s *Server) CreateVehicle(ctx context.Context, in *pb.Vehicle) (*pb.Vehicle, error) {
	if in == nil || in.VehicleUuid == "" {
		return nil, fmt.Errorf("invalid vehicle")
	}
	props := map[string]any{
		"vehicle_uuid": in.VehicleUuid, "id": in.Id, "capacity": in.Capacity, "status": in.Status,
		"last_seen_ts": in.LastSeenTs, "last_known_lat": in.LastKnownLat, "last_known_lon": in.LastKnownLon,
	}
	if err := s.repo.CreateVehicle(ctx, props); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) GetVehicle(ctx context.Context, in *pb.ID) (*pb.Vehicle, error) {
	m, err := s.repo.GetVehicle(ctx, in.Id)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	v := &pb.Vehicle{
		VehicleUuid: m["vehicle_uuid"].(string),
		Id:          m["id"].(string),
	}
	return v, nil
}
func (s *Server) UpdateVehicle(ctx context.Context, in *pb.Vehicle) (*pb.Vehicle, error) {
	props := map[string]any{"id": in.VehicleUuid, "capacity": in.Capacity, "status": in.Status, "last_seen_ts": in.LastSeenTs, "last_known_lat": in.LastKnownLat, "last_known_lon": in.LastKnownLon}
	if err := s.repo.UpdateVehicle(ctx, props); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) DeleteVehicle(ctx context.Context, in *pb.ID) (*pb.Empty, error) {
	if err := s.repo.DeleteVehicle(ctx, in.Id); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

/* Depots */
func (s *Server) CreateDepot(ctx context.Context, in *pb.Depot) (*pb.Depot, error) {
	props := map[string]any{"id": in.Id, "name": in.Name, "lat": in.Lat, "lon": in.Lon, "capacity": in.Capacity}
	if err := s.repo.CreateDepot(ctx, props); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) GetDepot(ctx context.Context, in *pb.ID) (*pb.Depot, error) {
	m, err := s.repo.GetDepot(ctx, in.Id)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	return &pb.Depot{Id: m["id"].(string), Name: m["name"].(string)}, nil
}
func (s *Server) UpdateDepot(ctx context.Context, in *pb.Depot) (*pb.Depot, error) {
	props := map[string]any{"id": in.Id, "name": in.Name, "lat": in.Lat, "lon": in.Lon, "capacity": in.Capacity}
	if err := s.repo.UpdateDepot(ctx, props); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) DeleteDepot(ctx context.Context, in *pb.ID) (*pb.Empty, error) {
	if err := s.repo.DeleteDepot(ctx, in.Id); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

/* Edge RPCs */
func (s *Server) GetNextEdge(ctx context.Context, in *pb.NextEdge) (*pb.NextEdge, error) {
	m, err := s.repo.GetNext(ctx, in.FromId, in.ToId)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	props := m["props"].(map[string]any)
	return &pb.NextEdge{
		FromId:     in.FromId,
		ToId:       in.ToId,
		TravelTime: helper.AnyToInt32(props["travel_time"]),
		Distance:   helper.AnyToInt32(props["distance"]),
	}, nil
}

func (s *Server) CreateNextEdge(ctx context.Context, in *pb.NextEdge) (*pb.NextEdge, error) {
	if in == nil {
		return nil, fmt.Errorf("invalid")
	}
	if err := s.repo.CreateNext(ctx, in.FromId, in.ToId, in.TravelTime, in.Distance); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) UpdateNextEdge(ctx context.Context, in *pb.NextEdge) (*pb.NextEdge, error) {
	props := map[string]any{"travel_time": in.TravelTime, "distance": in.Distance}
	if err := s.repo.UpdateNext(ctx, in.FromId, in.ToId, props); err != nil {
		return nil, err
	}
	return in, nil
}
func (s *Server) DeleteNextEdge(ctx context.Context, in *pb.NextEdge) (*pb.Empty, error) {
	if err := s.repo.DeleteNext(ctx, in.FromId, in.ToId); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *Server) GetServesEdge(ctx context.Context, in *pb.ServesEdge) (*pb.ServesEdge, error) {
	m, err := s.repo.GetServes(ctx, in.LineId, in.StopId)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	props := m["props"].(map[string]any)
	ord := int32(0)
	if v, ok := props["order"]; ok {
		ord = helper.AnyToInt32(v)
	}
	return &pb.ServesEdge{LineId: in.LineId, StopId: in.StopId, Order: ord}, nil
}

func (s *Server) ServesList(ctx context.Context, in *pb.ServesListRequest) (*pb.ServesListResponse, error) {
	if in == nil || in.LineId == "" {
		return nil, fmt.Errorf("line_id required")
	}
	rows, err := s.repo.GetServesList(ctx, in.LineId)
	if err != nil {
		return nil, err
	}
	out := &pb.ServesListResponse{}
	for _, r := range rows {
		stopId, _ := r["stopId"].(string)
		ordAny := r["order"]
		var ord int32 = 0
		if ordAny != nil {
			switch v := ordAny.(type) {
			case int64:
				ord = int32(v)
			case int32:
				ord = v
			case int:
				ord = int32(v)
			}
		}
		out.Edges = append(out.Edges, &pb.ServesEdge{
			LineId: in.LineId,
			StopId: stopId,
			Order:  ord,
		})
	}
	return out, nil
}

func (s *Server) CreateServesEdge(ctx context.Context, in *pb.ServesEdge) (*pb.ServesEdge, error) {
	if err := s.repo.CreateServes(ctx, in.LineId, in.StopId, in.Order); err != nil {
		return nil, err
	}
	return in, nil
}

func (s *Server) UpdateServesEdge(ctx context.Context, in *pb.ServesEdge) (*pb.ServesEdge, error) {
	props := map[string]any{"order": in.Order}
	if err := s.repo.UpdateServes(ctx, in.LineId, in.StopId, props); err != nil {
		return nil, err
	}
	return in, nil
}

func (s *Server) DeleteServesEdge(ctx context.Context, in *pb.ServesEdge) (*pb.Empty, error) {
	if err := s.repo.DeleteServes(ctx, in.LineId, in.StopId); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *Server) GetAssignedTo(ctx context.Context, in *pb.AssignedTo) (*pb.AssignedTo, error) {
	m, err := s.repo.GetAssignedTo(ctx, in.VehicleUuid, in.LineId)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	props := m["props"].(map[string]any)
	return &pb.AssignedTo{
		VehicleUuid: in.VehicleUuid,
		LineId:      in.LineId,
		Since:       helper.AnyToInt64(props["since"]),
	}, nil
}

func (s *Server) CreateAssignedTo(ctx context.Context, in *pb.AssignedTo) (*pb.AssignedTo, error) {
	if err := s.repo.CreateAssignedTo(ctx, in.VehicleUuid, in.LineId, in.Since); err != nil {
		return nil, err
	}
	return in, nil
}

func (s *Server) UpdateAssignedTo(ctx context.Context, in *pb.AssignedTo) (*pb.AssignedTo, error) {
	props := map[string]any{"since": in.Since}
	if err := s.repo.UpdateAssignedTo(ctx, in.VehicleUuid, in.LineId, props); err != nil {
		return nil, err
	}
	return in, nil
}

func (s *Server) DeleteAssignedTo(ctx context.Context, in *pb.AssignedTo) (*pb.Empty, error) {
	if err := s.repo.DeleteAssignedTo(ctx, in.VehicleUuid, in.LineId); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

func (s *Server) GetParkedAt(ctx context.Context, in *pb.ParkedAt) (*pb.ParkedAt, error) {
	m, err := s.repo.GetParkedAt(ctx, in.VehicleUuid, in.DepotId)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("not found")
	}
	props := m["props"].(map[string]any)
	return &pb.ParkedAt{
		VehicleUuid: in.VehicleUuid,
		DepotId:     in.DepotId,
		Since:       helper.AnyToInt64(props["since"]),
	}, nil
}

func (s *Server) CreateParkedAt(ctx context.Context, in *pb.ParkedAt) (*pb.ParkedAt, error) {
	if err := s.repo.CreateParkedAt(ctx, in.VehicleUuid, in.DepotId, in.Since); err != nil {
		return nil, err
	}
	return in, nil
}

func (s *Server) UpdateParkedAt(ctx context.Context, in *pb.ParkedAt) (*pb.ParkedAt, error) {
	props := map[string]any{"since": in.Since}
	if err := s.repo.UpdateParkedAt(ctx, in.VehicleUuid, in.DepotId, props); err != nil {
		return nil, err
	}
	return in, nil
}

func (s *Server) DeleteParkedAt(ctx context.Context, in *pb.ParkedAt) (*pb.Empty, error) {
	if err := s.repo.DeleteParkedAt(ctx, in.VehicleUuid, in.DepotId); err != nil {
		return nil, err
	}
	return &pb.Empty{}, nil
}

/* ========== Complex RPCs mapping ========== */

func (s *Server) AssignVehicle(ctx context.Context, req *pb.AssignVehicleRequest) (*pb.AssignVehicleResponse, error) {
	out, err := s.repo.AssignNearestIdleVehicle(ctx, req.LineId)
	if err != nil {
		return nil, err
	}
	uuid := out["vehicle_uuid"].(string)
	vmap, err := s.repo.GetVehicle(ctx, uuid)
	if err != nil {
		return nil, err
	}
	v := &pb.Vehicle{
		VehicleUuid: vmap["vehicle_uuid"].(string),
		Id:          vmap["id"].(string),
	}
	return &pb.AssignVehicleResponse{Vehicle: v, LineId: req.LineId}, nil
}

func (s *Server) RecalibrateEdge(ctx context.Context, req *pb.RecalibrateRequest) (*pb.NextEdge, error) {
	out, err := s.repo.RecalibrateNext(ctx, req.FromId, req.ToId, req.ObservedAvg)
	if err != nil {
		return nil, err
	}
	newVal := int32(out["new"].(int64))
	return &pb.NextEdge{FromId: req.FromId, ToId: req.ToId, TravelTime: newVal}, nil
}

func (s *Server) ShortestPath(ctx context.Context, req *pb.PathRequest) (*pb.PathResponse, error) {
	ids, hops, err := s.repo.ShortestPath(ctx, req.StartId, req.EndId, int(req.MaxHops))
	if err != nil {
		return nil, err
	}
	return &pb.PathResponse{NodeIds: ids, Hops: int32(hops)}, nil
}

func (s *Server) TopPairs(ctx context.Context, req *pb.TopPairsRequest) (*pb.TopPairsResponse, error) {
	res, err := s.repo.TopPairs(ctx, int(req.Limit))
	if err != nil {
		return nil, err
	}
	out := &pb.TopPairsResponse{}
	for _, r := range res {
		out.Pairs = append(out.Pairs, &pb.Pair{From: r["from"].(string), To: r["to"].(string), Lines: int32(r["lines"].(int64))})
	}
	return out, nil
}

func (s *Server) DepotsIdleStats(ctx context.Context, req *pb.DepotsRequest) (*pb.DepotsResponse, error) {
	res, err := s.repo.DepotsIdleStats(ctx, int(req.Limit))
	if err != nil {
		return nil, err
	}
	out := &pb.DepotsResponse{}
	for _, r := range res {
		out.Stats = append(out.Stats, &pb.DepotStat{
			DepotId:     r["depot_id"].(string),
			DepotName:   r["depot_name"].(string),
			ParkedCount: int32(r["parked_count"].(int64)),
			AvgIdleMs:   r["avg_idle_ms"].(float64),
		})
	}
	return out, nil
}

/*Reports*/
type Vehicle = repo.Vehicle
type Stop = repo.Stop

func (s *Server) GenerateReport(ctx context.Context, req *pb.GenerateReportRequest) (*pb.GenerateReportResponse, error) {
	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.SetTitle("Izvestaj iz baze", false)
	pdf.AddPage()

	pdf.SetFont("Arial", "B", 18)
	pdf.Cell(0, 10, "Izvestaj javnog preduzeca")
	pdf.Ln(12)

	pdf.SetFont("Arial", "", 12)
	pdf.Cell(0, 8, fmt.Sprintf("Datum generisanja: %s", time.Now().Format("02.01.2006. 15:04")))
	pdf.Ln(10)

	pdf.SetDrawColor(0, 0, 0)
	pdf.SetLineWidth(0.5)
	pdf.Line(10, pdf.GetY(), 200, pdf.GetY())
	pdf.Ln(8)

	vehiclesByDepot, err := s.repo.GetVehiclesByDepot()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch vehicles: %w", err)
	}
	stopsByZone, err := s.repo.GetStopsByZone()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch stops: %w", err)
	}

	topStops, err := s.repo.GetTopConnectedStops(ctx, 10)
	if err != nil {
		return nil, fmt.Errorf("no top connected stops found: %w", err)
	}

	shortestPath, _, err := s.repo.ShortestPath(ctx, req.StartId, req.EndId, int(req.MaxHops))
	if err != nil {
		return nil, fmt.Errorf("no path found: %w", err)
	}

	pdf.SetFont("Arial", "B", 16)
	pdf.Cell(0, 16, "Izvestaj o trenutnim vozilima parkiranim u depoima")
	pdf.Ln(10)
	for depot, vehicles := range vehiclesByDepot {
		pdf.SetFont("Arial", "B", 14)
		pdf.Cell(0, 8, fmt.Sprintf("Depo: %s", depot))
		pdf.Ln(10)
		addVehiclesTable(pdf, vehicles)
	}

	pdf.SetFont("Arial", "B", 16)
	pdf.Cell(0, 16, "Izvestaj o stajalistima po zonama")
	pdf.Ln(10)
	for zone, stops := range stopsByZone {
		pdf.SetFont("Arial", "B", 14)
		pdf.Cell(0, 8, fmt.Sprintf("Zona: %s", zone))
		pdf.Ln(10)
		addStopsTable(pdf, stops)
	}

	addTopConnectedStopsChart(pdf, topStops)
	addShortestPath(pdf, shortestPath, req.StartId, req.EndId)

	filename := "report.pdf"
	pdf.OutputFileAndClose(filename)
	return &pb.GenerateReportResponse{Created: true, Filename: filename}, nil
}

func addVehiclesTable(pdf *gofpdf.Fpdf, vehicles []Vehicle) {
	pdf.SetFont("Arial", "", 12)
	for _, v := range vehicles {
		pdf.Cell(0, 6, fmt.Sprintf("Vozilo: %s | Status: %s | Kapacitet: %d", v.UUID, v.Status, v.Capacity))
		pdf.Ln(6)
	}
	pdf.Ln(4)
}

func addStopsTable(pdf *gofpdf.Fpdf, stops []Stop) {
	pdf.SetFont("Arial", "", 12)
	for _, s := range stops {
		pdf.Cell(0, 6, fmt.Sprintf("Stajaliste: %s | Naziv: %s | Nadkriveno: %v", s.ID, s.Name, s.Shelter))
		pdf.Ln(6)
	}
	pdf.Ln(4)
}

func addTopConnectedStopsChart(pdf *gofpdf.Fpdf, stops []map[string]any) {
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(0, 8, "Izvestaj o najvise povezanim stajalistima")
	pdf.Ln(12)

	if len(stops) == 0 {
		pdf.SetFont("Arial", "", 12)
		pdf.Cell(0, 6, "Nisu pronadjena povezna stajalista.")
		pdf.Ln(8)
		return
	}

	labels := make([]string, 0, len(stops))
	vals := make([]float64, 0, len(stops))
	maxv := float64(0)
	for _, s := range stops {
		name := s["stop_id"].(string)
		if n, ok := s["stop_name"].(string); ok && n != "" {
			name = fmt.Sprintf("%s (%s)", name, n)
		}
		labels = append(labels, name)
		deg := float64(s["degree"].(int64))
		vals = append(vals, deg)
		if deg > maxv {
			maxv = deg
		}
	}

	left := 30.0
	top := pdf.GetY() + 10.0
	barH := 8.0
	gap := 6.0
	chartW := 150.0
	for i := range labels {
		y := top + float64(i)*(barH+gap)
		pdf.SetXY(left, y)
		w := 0.0
		if maxv > 0 {
			w = (vals[i] / maxv) * chartW
		}
		pdf.SetFillColor(70, 130, 180)
		pdf.Rect(left, y, w, barH, "F")
		pdf.SetXY(10, y)
		pdf.SetFont("Arial", "", 9)
		pdf.CellFormat(0, barH, labels[i], "", 0, "", false, 0, "")
		pdf.SetXY(left+chartW+5, y)
		pdf.CellFormat(20, barH, fmt.Sprintf("%.0f", vals[i]), "", 0, "R", false, 0, "")
	}
	pdf.Ln(float64(len(labels))*(barH+gap) + 10)
}

func addShortestPath(pdf *gofpdf.Fpdf, path []string, start, stop string) {
	pdf.AddPage()
	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(0, 8, fmt.Sprintf("Izvestaj o najkracoj putanji izmedju %s i %s", start, stop))
	pdf.Ln(12)

	if len(path) == 0 {
		pdf.SetFont("Arial", "", 12)
		pdf.Cell(0, 6, "Nema pronađenog puta između zadatih čvorova.")
		pdf.Ln(6)
		return
	}

	startX := 30.0
	y := pdf.GetY() + 20.0
	step := 30.0
	r := 6.0
	pdf.SetLineWidth(0.7)
	for i, node := range path {
		x := startX + float64(i)*step
		pdf.Circle(x, y, r, "D")
		pdf.SetFont("Arial", "", 8)
		pdf.SetXY(x-8, y-3)
		pdf.CellFormat(16, 6, node, "", 0, "C", false, 0, "")
		if i < len(path)-1 {
			x2 := startX + float64(i+1)*step
			pdf.Line(x+r, y, x2-r, y)
		}
	}
	pdf.Ln(40)
}
