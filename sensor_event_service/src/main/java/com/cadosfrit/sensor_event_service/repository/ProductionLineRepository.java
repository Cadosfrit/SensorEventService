package com.cadosfrit.sensor_event_service.repository;

import com.cadosfrit.sensor_event_service.model.ProductionLine;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface ProductionLineRepository extends JpaRepository<ProductionLine, String> {

    @Query(value = """
        SELECT\s
            pl.line_id AS lineId,\s
            COALESCE(SUM(CASE WHEN me.defect_count = -1 THEN 0 ELSE me.defect_count END), 0) AS totalDefects,
            COUNT(me.event_id) AS eventCount
        FROM production_lines pl
        JOIN machines m ON pl.line_id = m.line_id
        JOIN machine_events me ON m.machine_id = me.machine_id
        WHERE pl.factory_id = :factoryId
          AND me.timestamp >= :fromTime
          AND me.timestamp < :toTime
        GROUP BY pl.line_id
        ORDER BY totalDefects DESC
        LIMIT :limit
       \s""", nativeQuery = true)
    List<Object[]> findTopDefectLinesRaw(@Param("factoryId") String factoryId,
                                         @Param("fromTime") Instant fromTime,
                                         @Param("toTime") Instant toTime,
                                         @Param("limit") int limit);
}