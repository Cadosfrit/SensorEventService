package com.cadosfrit.sensor_event_service.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.cadosfrit.sensor_event_service.model.MachineEvent;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Repository
public interface MachineEventRepository extends JpaRepository<MachineEvent, String> {

    @Query(value = "CALL process_event_batch(:jsonBatch)", nativeQuery = true)
    List<Map<String, Object>> processBatchInDB(@Param("jsonBatch") String jsonBatch);

    @Query(value = "CALL process_event_batch(:jsonBatch)", nativeQuery = true)
    List<Map<String, Object>> processFastBatchSP(@Param("jsonBatch") String jsonBatch);

    @Query(value = "CALL process_batch_sequential(:jsonBatch)", nativeQuery = true)
    List<Map<String, Object>> processSlowBatchSP(@Param("jsonBatch") String jsonBatch);

    /**
     * Windowed Stats Query
     * Rule: defect_count = -1 is ignored in the SUM but counts in time window.
     */
    @Query(value = "SELECT " +
            "SUM(CASE WHEN defect_count = -1 THEN 0 ELSE defect_count END) as totalDefects, " +
            "COUNT(event_id) as totalEvents " +
            "FROM machine_events " +
            "WHERE machine_id = :machineId " +
            "AND event_time >= :startTime " + // Inclusive
            "AND event_time < :endTime",      // Exclusive
            nativeQuery = true)
    Object[] getMachineStatsRaw(@Param("machineId") String machineId,
                                @Param("startTime") Instant startTime,
                                @Param("endTime") Instant endTime);

    @Query(value = """
            SELECT\s
                 pl.line_id AS lineId,\s
                 COALESCE(SUM(CASE WHEN me.defect_count = -1 THEN 0 ELSE me.defect_count END), 0) AS totalDefects,
                 COUNT(me.event_id) AS eventCount,
                 (COALESCE(SUM(CASE WHEN me.defect_count = -1 THEN 0 ELSE me.defect_count END), 0) * 100.0 /\s
                      NULLIF(COUNT(me.event_id), 0)) AS defectPercentage
             FROM factories f
             JOIN production_lines pl ON f.factory_id = pl.factory_id
             JOIN machines m ON pl.line_id = m.line_id
             JOIN machine_events me ON m.machine_id = me.machine_id
             WHERE f.factory_id = :factoryId
               AND me.event_time >= :from
               AND me.event_time < :to
             GROUP BY pl.line_id
             ORDER BY defectPercentage DESC
             LIMIT :limit
       \s""", nativeQuery = true)
    List<Object[]> findTopDefectLinesRaw(
            @Param("factoryId") String factoryId,
            @Param("from") Instant from,
            @Param("to") Instant to,
            @Param("limit") int limit
    );
}