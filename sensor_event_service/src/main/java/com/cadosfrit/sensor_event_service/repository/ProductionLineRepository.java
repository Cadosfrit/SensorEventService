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

    @Query(value = "SELECT pl.line_id, " +
            "SUM(CASE WHEN me.defect_count = -1 THEN 0 ELSE me.defect_count END) as totalDefects, " +
            "COUNT(me.event_id) as eventCount " +
            "FROM production_lines pl " +
            "JOIN machines m ON pl.line_id = m.line_id " +
            "JOIN machine_events me ON m.machine_id = me.machine_id " +
            "WHERE pl.factory_id = :factoryId " +
            "AND me.event_time >= :fromTime " +
            "AND me.event_time < :toTime " +
            "GROUP BY pl.line_id " +
            "ORDER BY (SUM(CASE WHEN me.defect_count = -1 THEN 0 ELSE me.defect_count END) / COUNT(me.event_id)) DESC " +
            "LIMIT :limit",
            nativeQuery = true)
    List<Object[]> findTopDefectLinesRaw(@Param("factoryId") String factoryId,
                                         @Param("fromTime") Instant fromTime,
                                         @Param("toTime") Instant toTime,
                                         @Param("limit") int limit);
}