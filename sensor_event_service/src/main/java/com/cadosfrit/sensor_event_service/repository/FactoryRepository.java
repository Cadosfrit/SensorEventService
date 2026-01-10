package com.cadosfrit.sensor_event_service.repository;

import com.cadosfrit.sensor_event_service.model.Factory;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FactoryRepository extends JpaRepository<Factory, String> {
}
