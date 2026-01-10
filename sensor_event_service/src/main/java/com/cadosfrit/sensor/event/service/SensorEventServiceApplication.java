package com.cadosfrit.sensor.event.service;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.TimeZone;

@SpringBootApplication
public class SensorEventServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(SensorEventServiceApplication.class, args);
	}

    @PostConstruct
    public void init() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }
}
