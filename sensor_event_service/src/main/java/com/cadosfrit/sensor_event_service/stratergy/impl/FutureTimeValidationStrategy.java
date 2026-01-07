package com.cadosfrit.sensor_event_service.stratergy.impl;

import com.cadosfrit.sensor_event_service.constants.AppConstants;
import com.cadosfrit.sensor_event_service.constants.Constants;
import com.cadosfrit.sensor_event_service.dto.EventRequestDTO;
import com.cadosfrit.sensor_event_service.stratergy.EventValidationStrategy;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

@Component
public class FutureTimeValidationStrategy implements EventValidationStrategy {
    
    @Override
    public Optional<String> validate(EventRequestDTO event) {
        Instant limit = Instant.now().plus(AppConstants.FUTURE_TIME_ALLOWANCE_MINS, ChronoUnit.MINUTES);
        if (event.getEventTime() != null && event.getEventTime().isAfter(limit)) {
            return Optional.of(Constants.FUTURE_EVENT_TIME.getCode());
        }
        return Optional.empty();
    }
}