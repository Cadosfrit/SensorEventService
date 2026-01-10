package com.cadosfrit.sensor.event.service.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cadosfrit.sensor.event.service.model.Machine;

@Repository
public interface MachineRepository extends JpaRepository<Machine, String> {
}