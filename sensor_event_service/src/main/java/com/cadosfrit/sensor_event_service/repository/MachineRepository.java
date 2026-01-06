package com.cadosfrit.sensor_event_service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cadosfrit.sensor_event_service.model.Machine;

@Repository
public interface MachineRepository extends JpaRepository<Machine, String> {
}