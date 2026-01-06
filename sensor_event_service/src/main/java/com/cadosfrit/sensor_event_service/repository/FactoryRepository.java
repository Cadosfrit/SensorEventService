package com.cadosfrit.sensor_event_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cadosfrit.sensor_event_service.model.Factory;

@Repository
public interface FactoryRepository extends JpaRepository<Factory, String> {
}