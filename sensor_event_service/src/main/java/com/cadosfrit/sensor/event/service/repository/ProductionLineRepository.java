package com.cadosfrit.sensor.event.service.repository;

import com.cadosfrit.sensor.event.service.model.ProductionLine;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductionLineRepository extends JpaRepository<ProductionLine, String> {
}
