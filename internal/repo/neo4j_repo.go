package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type NeoRepo struct {
	drv neo4j.DriverWithContext
}

func New(uri, user, pass string) (*NeoRepo, error) {
	drv, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(user, pass, ""))
	if err != nil {
		return nil, err
	}
	return &NeoRepo{drv: drv}, nil
}

func (r *NeoRepo) Close(ctx context.Context) error {
	return r.drv.Close(ctx)
}

/* ========== Stops CRUD ========== */
func (r *NeoRepo) CreateStop(ctx context.Context, id, name string, lat, lon float64, zone string, shelter bool) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx,
			`CREATE (s:Stop {id:$id, name:$name, lat:$lat, lon:$lon, zone:$zone, shelter:$shelter, created_at:$now}) RETURN s`,
			map[string]any{"id": id, "name": name, "lat": lat, "lon": lon, "zone": zone, "shelter": shelter, "now": time.Now().Unix()})
		return nil, err
	})
	return err
}

func (r *NeoRepo) GetStop(ctx context.Context, id string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	rec, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `MATCH (s:Stop {id:$id}) RETURN s LIMIT 1`, map[string]any{"id": id})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			node := rs.Record().Values[0].(neo4j.Node)
			return node.Props, nil
		}
		return nil, nil
	})
	if rec == nil {
		return nil, err
	}
	return rec.(map[string]any), nil
}

func (r *NeoRepo) UpdateStop(ctx context.Context, props map[string]any) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (s:Stop {id:$id}) SET s += $props RETURN s`, map[string]any{"id": props["id"], "props": props})
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteStop(ctx context.Context, id string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (s:Stop {id:$id}) DETACH DELETE s`, map[string]any{"id": id})
		return nil, err
	})
	return err
}

/* ========== Line CRUD ========== */
func (r *NeoRepo) CreateLine(ctx context.Context, id, name, mode string, freq int32, active bool) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `CREATE (l:Line {id:$id, name:$name, mode:$mode, frequency_mins:$freq, active:$active})`, map[string]any{"id": id, "name": name, "mode": mode, "freq": freq, "active": active})
		return nil, err
	})
	return err
}

func (r *NeoRepo) GetLine(ctx context.Context, id string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	rec, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `MATCH (l:Line {id:$id}) RETURN l LIMIT 1`, map[string]any{"id": id})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			return rs.Record().Values[0].(neo4j.Node).Props, nil
		}
		return nil, nil
	})
	if rec == nil {
		return nil, err
	}
	return rec.(map[string]any), nil
}

func (r *NeoRepo) UpdateLine(ctx context.Context, props map[string]any) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	params := map[string]any{"id": props["id"], "props": props}
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (l:Line {id:$id}) SET l += $props RETURN l`, params)
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteLine(ctx context.Context, id string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (l:Line {id:$id}) DETACH DELETE l`, map[string]any{"id": id})
		return nil, err
	})
	return err
}

/* ========== Vehicle CRUD ========== */
func (r *NeoRepo) CreateVehicle(ctx context.Context, props map[string]any) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `CREATE (v:Vehicle $props)`, map[string]any{"props": props})
		return nil, err
	})
	return err
}

func (r *NeoRepo) GetVehicle(ctx context.Context, id string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	rec, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$id}) RETURN v LIMIT 1`, map[string]any{"id": id})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			return rs.Record().Values[0].(neo4j.Node).Props, nil
		}
		return nil, nil
	})
	if rec == nil {
		return nil, err
	}
	return rec.(map[string]any), nil
}

func (r *NeoRepo) UpdateVehicle(ctx context.Context, props map[string]any) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	params := map[string]any{"id": props["id"], "props": props}
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$id}) SET v += $props RETURN v`, params)
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteVehicle(ctx context.Context, id string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$id}) DETACH DELETE v`, map[string]any{"id": id})
		return nil, err
	})
	return err
}

/* ========== Depot CRUD ========== */
func (r *NeoRepo) CreateDepot(ctx context.Context, props map[string]any) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `CREATE (d:Depot $props)`, map[string]any{"props": props})
		return nil, err
	})
	return err
}

func (r *NeoRepo) GetDepot(ctx context.Context, id string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	rec, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `MATCH (d:Depot {id:$id}) RETURN d LIMIT 1`, map[string]any{"id": id})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			return rs.Record().Values[0].(neo4j.Node).Props, nil
		}
		return nil, nil
	})
	if rec == nil {
		return nil, err
	}
	return rec.(map[string]any), nil
}

func (r *NeoRepo) UpdateDepot(ctx context.Context, props map[string]any) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	params := map[string]any{"id": props["id"], "props": props}
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (d:Depot {id:$id}) SET d += $props RETURN d`, params)
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteDepot(ctx context.Context, id string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (d:Depot {id:$id}) DETACH DELETE d`, map[string]any{"id": id})
		return nil, err
	})
	return err
}

/* ========== Edges CRUD ========== */
func (r *NeoRepo) GetNext(ctx context.Context, from, to string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `
			MATCH (a:Stop {id:$from})-[r:NEXT]->(b:Stop {id:$to})
			RETURN r, a.id AS fromId, b.id AS toId LIMIT 1
		`, map[string]any{"from": from, "to": to})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			rec := rs.Record()
			rel := rec.Values[0].(neo4j.Relationship)
			return map[string]any{
				"from":  rec.Values[1].(string),
				"to":    rec.Values[2].(string),
				"props": rel.Props,
			}, nil
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return out.(map[string]any), nil
}

func (r *NeoRepo) CreateNext(ctx context.Context, from, to string, travel, dist int32) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx,
			`MATCH (a:Stop {id:$from}), (b:Stop {id:$to})
             CREATE (a)-[:NEXT {travel_time:$travel, distance:$dist, created_at:$now}]->(b)`,
			map[string]any{"from": from, "to": to, "travel": travel, "dist": dist, "now": time.Now().Unix()})
		return nil, err
	})
	return err
}

func (r *NeoRepo) UpdateNext(ctx context.Context, from, to string, props map[string]any) error {
	params := map[string]any{"from": from, "to": to, "props": props}
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (a:Stop {id:$from})-[e:NEXT]->(b:Stop {id:$to}) SET e += $props RETURN e`, params)
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteNext(ctx context.Context, from, to string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (a:Stop {id:$from})-[e:NEXT]->(b:Stop {id:$to}) DELETE e`, map[string]any{"from": from, "to": to})
		return nil, err
	})
	return err
}

/* SERVES */
func (r *NeoRepo) GetServes(ctx context.Context, lineId, stopId string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `
			MATCH (l:Line {id:$line})-[r:SERVES]->(s:Stop {id:$stop})
			RETURN r, l.id AS lineId, s.id AS stopId LIMIT 1
		`, map[string]any{"line": lineId, "stop": stopId})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			rec := rs.Record()
			rel := rec.Values[0].(neo4j.Relationship)
			return map[string]any{
				"line":  rec.Values[1].(string),
				"stop":  rec.Values[2].(string),
				"props": rel.Props,
			}, nil
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return out.(map[string]any), nil
}

func (r *NeoRepo) CreateServes(ctx context.Context, lineId, stopId string, order int32) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (l:Line {id:$l}), (s:Stop {id:$s}) CREATE (l)-[:SERVES {order:$o}]->(s)`, map[string]any{"l": lineId, "s": stopId, "o": order})
		return nil, err
	})
	return err
}

func (r *NeoRepo) UpdateServes(ctx context.Context, lineId, stopId string, props map[string]any) error {
	params := map[string]any{"lineId": lineId, "stopId": stopId, "props": props}
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx,
			`MATCH (l:Line {id:$lineId})-[r:SERVES]->(s:Stop {id:$stopId})
             SET r += $props
             RETURN r`,
			params)
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteServes(ctx context.Context, lineId, stopId string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (l:Line {id:$l})-[r:SERVES]->(s:Stop {id:$s}) DELETE r`, map[string]any{"l": lineId, "s": stopId})
		return nil, err
	})
	return err
}

/* ASSIGNED_TO */
func (r *NeoRepo) GetAssignedTo(ctx context.Context, vehicleUUID, lineId string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `
			MATCH (v:Vehicle {vehicle_uuid:$v})-[r:ASSIGNED_TO]->(l:Line {id:$l})
			RETURN r, v.vehicle_uuid AS vehicle, l.id AS line LIMIT 1
		`, map[string]any{"v": vehicleUUID, "l": lineId})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			rec := rs.Record()
			rel := rec.Values[0].(neo4j.Relationship)
			return map[string]any{
				"vehicle": rec.Values[1].(string),
				"line":    rec.Values[2].(string),
				"props":   rel.Props,
			}, nil
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return out.(map[string]any), nil
}

func (r *NeoRepo) CreateAssignedTo(ctx context.Context, vehicleUUID, lineId string, since int64) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$v}), (l:Line {id:$l}) CREATE (v)-[:ASSIGNED_TO {since:$since}]->(l) SET v.status='ACTIVE'`, map[string]any{"v": vehicleUUID, "l": lineId, "since": since})
		return nil, err
	})
	return err
}

func (r *NeoRepo) UpdateAssignedTo(ctx context.Context, vehicleUUID, lineId string, props map[string]any) error {
	params := map[string]any{"vehicleUUID": vehicleUUID, "lineId": lineId, "props": props}
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx,
			`MATCH (v:Vehicle {vehicle_uuid:$vehicleUUID})-[r:ASSIGNED_TO]->(l:Line {id:$lineId})
             SET r += $props
             RETURN r`,
			params)
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteAssignedTo(ctx context.Context, vehicleUUID, lineId string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$v})-[r:ASSIGNED_TO]->(l:Line {id:$l}) DELETE r`, map[string]any{"v": vehicleUUID, "l": lineId})
		return nil, err
	})
	return err
}

/* PARKED_AT */
func (r *NeoRepo) GetParkedAt(ctx context.Context, vehicleUUID, depotId string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `
			MATCH (v:Vehicle {vehicle_uuid:$v})-[r:PARKED_AT]->(d:Depot {id:$d})
			RETURN r, v.vehicle_uuid AS vehicle, d.id AS depot LIMIT 1
		`, map[string]any{"v": vehicleUUID, "d": depotId})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			rec := rs.Record()
			rel := rec.Values[0].(neo4j.Relationship)
			return map[string]any{
				"vehicle": rec.Values[1].(string),
				"depot":   rec.Values[2].(string),
				"props":   rel.Props,
			}, nil
		}
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return out.(map[string]any), nil
}

func (r *NeoRepo) CreateParkedAt(ctx context.Context, vehicleUUID, depotId string, since int64) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$v}), (d:Depot {id:$d}) CREATE (v)-[:PARKED_AT {since:$since}]->(d) SET v.status='IDLE'`, map[string]any{"v": vehicleUUID, "d": depotId, "since": since})
		return nil, err
	})
	return err
}

func (r *NeoRepo) UpdateParkedAt(ctx context.Context, vehicleUUID, depotId string, props map[string]any) error {
	params := map[string]any{"vehicleUUID": vehicleUUID, "depotId": depotId, "props": props}
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx,
			`MATCH (v:Vehicle {vehicle_uuid:$vehicleUUID})-[r:PARKED_AT]->(d:Depot {id:$depotId})
             SET r += $props
             RETURN r`,
			params)
		return nil, err
	})
	return err
}

func (r *NeoRepo) DeleteParkedAt(ctx context.Context, vehicleUUID, depotId string) error {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		_, err := tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$v})-[r:PARKED_AT]->(d:Depot {id:$d}) DELETE r`, map[string]any{"v": vehicleUUID, "d": depotId})
		return nil, err
	})
	return err
}

/* ========== COMPLEX QUERIES (the 5 required) ========== */

/*
 1. Assign nearest idle vehicle to line (complex CRUD)
    MATCH... WHERE... WITH... + CREATE + UPDATE in one transaction
*/
func (r *NeoRepo) AssignNearestIdleVehicle(ctx context.Context, lineId string) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	out, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		// get start stop
		rs, err := tx.Run(ctx, `
            MATCH (l:Line {id:$line})-[:SERVES {order:1}]->(s:Stop)
            RETURN s.lat AS lat, s.lon AS lon LIMIT 1
        `, map[string]any{"line": lineId})
		if err != nil {
			return nil, err
		}
		if !rs.Next(ctx) {
			return nil, fmt.Errorf("start stop for line %s not found", lineId)
		}
		lat := rs.Record().Values[0].(float64)
		lon := rs.Record().Values[1].(float64)

		// find nearest IDLE vehicle
		rs2, err := tx.Run(ctx, `
            MATCH (v:Vehicle {status:'IDLE'})
			WHERE v.last_known_lat IS NOT NULL AND v.last_known_lon IS NOT NULL
            RETURN v.vehicle_uuid AS uuid, v.last_known_lat AS lat, v.last_known_lon AS lon
        `, nil)
		if err != nil {
			return nil, err
		}
		var best string
		var bestDist float64 = 1e18
		for rs2.Next(ctx) {
			rec := rs2.Record()
			uuid := rec.Values[0].(string)
			vlat := rec.Values[1].(float64)
			vlon := rec.Values[2].(float64)
			dx := vlat - lat
			dy := vlon - lon
			d := dx*dx + dy*dy
			if d < bestDist {
				bestDist = d
				best = uuid
			}
		}
		if best == "" {
			return nil, fmt.Errorf("no idle vehicles available")
		}
		_, err = tx.Run(ctx, `MATCH (v:Vehicle {vehicle_uuid:$v}), (l:Line {id:$l}) CREATE (v)-[:ASSIGNED_TO {since:$now}]->(l) SET v.status='ACTIVE' RETURN v.vehicle_uuid`, map[string]any{"v": best, "l": lineId, "now": time.Now().Unix()})
		if err != nil {
			return nil, err
		}
		return map[string]any{"vehicle_uuid": best}, nil
	})
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

/*
 2. Recalibrate NEXT: conditional update on relationship (complex CRUD)
    This uses MATCH, WHERE, WITH in the cypher inside ExecuteWrite
*/
func (r *NeoRepo) RecalibrateNext(ctx context.Context, from, to string, observed int32) (map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)
	out, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `
            MATCH (a:Stop {id:$from})-[r:NEXT]->(b:Stop {id:$to})
            WHERE r.travel_time IS NOT NULL
            WITH r, r.travel_time AS cur, $obs AS observed
            WHERE observed > cur * 1.2
            SET r.travel_time = observed, r.last_calibrated = timestamp(), r.calibration_count = coalesce(r.calibration_count,0) + 1
            RETURN r.travel_time AS new_travel_time, r.calibration_count AS calibration_count, r.last_calibrated AS last_calibrated
        `, map[string]any{"from": from, "to": to, "obs": observed})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			rec := rs.Record()
			return map[string]any{
				"new":  rec.Values[0].(int64),
				"cnt":  rec.Values[1].(int64),
				"when": rec.Values[2].(int64),
			}, nil
		}
		// if no update happened, return current value
		rs2, err := tx.Run(ctx, `MATCH (a:Stop {id:$from})-[r:NEXT]->(b:Stop {id:$to}) RETURN r.travel_time AS cur`, map[string]any{"from": from, "to": to})
		if err != nil {
			return nil, err
		}
		if rs2.Next(ctx) {
			return map[string]any{"new": rs2.Record().Values[0].(int64)}, nil
		}
		return nil, fmt.Errorf("edge not found")
	})
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

/* 3) TopPairs analytic (MATCH, WHERE, WITH, aggregate COUNT) */
func (r *NeoRepo) TopPairs(ctx context.Context, limit int) ([]map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `
            MATCH (s1:Stop)-[:NEXT]->(s2:Stop)
            MATCH (l:Line)-[:SERVES]->(s1), (l)-[:SERVES]->(s2)
            WHERE s1.id <> s2.id
            WITH s1, s2, count(DISTINCT l) AS linesCount
            ORDER BY linesCount DESC
            LIMIT $limit
            RETURN s1.id AS from, s2.id AS to, linesCount
        `, map[string]any{"limit": limit})
		if err != nil {
			return nil, err
		}
		var res []map[string]any
		for rs.Next(ctx) {
			rec := rs.Record()
			res = append(res, map[string]any{
				"from":  rec.Values[0].(string),
				"to":    rec.Values[1].(string),
				"lines": rec.Values[2].(int64),
			})
		}
		return res, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]map[string]any), nil
}

/* 4) DepotsIdleStats analytic (MATCH, WHERE, WITH, aggregate COUNT + AVG) */
func (r *NeoRepo) DepotsIdleStats(ctx context.Context, limit int) ([]map[string]any, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		rs, err := tx.Run(ctx, `
            MATCH (d:Depot)<-[p:PARKED_AT]-(v:Vehicle)
            WHERE v.status = 'IDLE' AND v.last_seen_ts IS NOT NULL
            WITH d, count(v) AS parked_count, avg(timestamp() - v.last_seen_ts) AS avg_idle_ms
            RETURN d.id AS depot_id, d.name AS depot_name, parked_count, avg_idle_ms
            ORDER BY parked_count DESC
            LIMIT $limit
        `, map[string]any{"limit": limit})
		if err != nil {
			return nil, err
		}
		var res []map[string]any
		for rs.Next(ctx) {
			rec := rs.Record()
			// parked_count may be int64, avg_idle_ms float64
			res = append(res, map[string]any{
				"depot_id":     rec.Values[0].(string),
				"depot_name":   rec.Values[1].(string),
				"parked_count": rec.Values[2].(int64),
				"avg_idle_ms":  rec.Values[3].(float64),
			})
		}
		return res, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]map[string]any), nil
}

/* 5) ShortestPath utility (used by server) */
func (r *NeoRepo) ShortestPath(ctx context.Context, start, end string, maxHops int) ([]string, int, error) {
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)
	out, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := fmt.Sprintf(`
            MATCH (a:Stop {id:$start}), (b:Stop {id:$end})
            MATCH p = shortestPath((a)-[:NEXT*..%d]->(b))
            RETURN [n IN nodes(p) | n.id] AS ids, length(p) AS hops
        `, maxHops)
		rs, err := tx.Run(ctx, query, map[string]any{"start": start, "end": end})
		if err != nil {
			return nil, err
		}
		if rs.Next(ctx) {
			rec := rs.Record()
			idsAny := rec.Values[0].([]any)
			ids := make([]string, 0, len(idsAny))
			for _, v := range idsAny {
				ids = append(ids, v.(string))
			}
			hops := int(rec.Values[1].(int64))
			return map[string]any{"ids": ids, "hops": hops}, nil
		}
		return nil, fmt.Errorf("no path found")
	})
	if err != nil {
		return nil, 0, err
	}
	m := out.(map[string]any)
	return m["ids"].([]string), m["hops"].(int), nil
}

/* Methods for generating report*/
type Vehicle struct {
	UUID     string
	Status   string
	Capacity int
	Depot    string
}

type Stop struct {
	ID      string
	Name    string
	Zone    string
	Shelter bool
}

func (r *NeoRepo) GetVehiclesByDepot() (map[string][]Vehicle, error) {
	ctx := context.Background()
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result := make(map[string][]Vehicle)
	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `MATCH (v:Vehicle)-[:PARKED_AT]->(d:Depot) RETURN v.vehicle_uuid, v.status, v.capacity, d.name`
		rs, err := tx.Run(ctx, query, nil)
		if err != nil {
			return nil, err
		}
		for rs.Next(ctx) {
			rec := rs.Record()
			depot := rec.Values[3].(string)
			v := Vehicle{
				UUID:     rec.Values[0].(string),
				Status:   rec.Values[1].(string),
				Capacity: int(rec.Values[2].(int64)),
				Depot:    depot,
			}
			result[depot] = append(result[depot], v)
		}
		return nil, nil
	})
	return result, err
}

func (r *NeoRepo) GetStopsByZone() (map[string][]Stop, error) {
	ctx := context.Background()
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result := make(map[string][]Stop)
	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `MATCH (s:Stop) RETURN s.id, s.name, s.zone, s.shelter`
		rs, err := tx.Run(ctx, query, nil)
		if err != nil {
			return nil, err
		}
		for rs.Next(ctx) {
			rec := rs.Record()
			s := Stop{
				ID:      rec.Values[0].(string),
				Name:    rec.Values[1].(string),
				Zone:    rec.Values[2].(string),
				Shelter: rec.Values[3].(bool),
			}
			result[s.Zone] = append(result[s.Zone], s)
		}
		return nil, nil
	})
	return result, err
}

func (r *NeoRepo) GetAverageOccupancyByDepot() (map[string]float64, error) {
	ctx := context.Background()
	session := r.drv.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	result := make(map[string]float64)
	_, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		query := `
			MATCH (v:Vehicle)-[:PARKED_AT]->(d:Depot)
			RETURN d.name, avg(v.capacity * CASE WHEN v.status='ACTIVE' THEN 1 ELSE 0 END) as avgOccupied
		`
		rs, err := tx.Run(ctx, query, nil)
		if err != nil {
			return nil, err
		}
		for rs.Next(ctx) {
			rec := rs.Record()
			depot := rec.Values[0].(string)
			avg := rec.Values[1].(float64)
			result[depot] = avg
		}
		return nil, nil
	})
	return result, err
}
