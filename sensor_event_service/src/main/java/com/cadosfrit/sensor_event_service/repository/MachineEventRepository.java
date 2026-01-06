package com.cadosfrit.sensor_event_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.cadosfrit.sensor_event_service.model.MachineEvent;

import java.time.Instant;
import java.util.List;

@Repository
public interface MachineEventRepository extends JpaRepository<MachineEvent, String> {

    List<MachineEvent> findByMachineIdAndEventTimeBetween(
        String machineId, 
        Instant start, 
        Instant end
    );

    @Query(value = """
        WITH LineStats AS (
            SELECT 
                m.line_id AS lineId,
                SUM(CASE WHEN e.defect_count > 0 THEN e.defect_count ELSE 0 END) AS totalDefects,
                COUNT(e.event_id) AS eventCount
            FROM machine_events e
            JOIN machines m ON e.machine_id = m.id
            JOIN production_lines pl ON m.line_id = pl.id
            WHERE pl.factory_id = :factoryId
              AND e.event_time >= :start 
              AND e.event_time < :end
            GROUP BY m.line_id
        )
        SELECT 
            lineId, 
            totalDefects, 
            eventCount,
            ROUND((totalDefects * 100.0) / eventCount, 2) AS defectsPercent
        FROM LineStats
        ORDER BY totalDefects DESC
        LIMIT :limit
        """, nativeQuery = true)
    List<Object[]> findTopDefectLinesCTE(
        @Param("factoryId") String factoryId,
        @Param("start") Instant start,
        @Param("end") Instant end,
        @Param("limit") int limit
    );
}