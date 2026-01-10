package com.cadosfrit.sensor_event_service.repository;

import com.cadosfrit.sensor_event_service.model.ProductionLine;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductionLineRepository extends JpaRepository<ProductionLine, String> {
}
