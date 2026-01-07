package com.cadosfrit.sensor_event_service.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.query.Procedure;
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
}