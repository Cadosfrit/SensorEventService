package com.cadosfrit.sensor.event.service.stratergy.impl;

import com.cadosfrit.sensor.event.service.constants.AppConstants;
import com.cadosfrit.sensor.event.service.constants.Constants;
import com.cadosfrit.sensor.event.service.dto.EventRequestDTO;
import com.cadosfrit.sensor.event.service.stratergy.EventValidationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

@Component
public class FutureTimeValidationStrategy implements EventValidationStrategy {
    
    private static final Logger logger = LoggerFactory.getLogger(FutureTimeValidationStrategy.class);

    @Override
    public Optional<String> validate(EventRequestDTO event) {
        try {
            if (event == null) {
                logger.warn("FutureTimeValidationStrategy: Event object is null");
                return Optional.of(Constants.FUTURE_EVENT_TIME.getCode());
            }

            Instant eventTime = event.getEventTime();

            if (eventTime == null) {
                logger.warn("FutureTimeValidationStrategy: Event time is null for eventId: {}",
                    event.getEventId());
                return Optional.of(Constants.FUTURE_EVENT_TIME.getCode());
            }

            try {
                Instant limit = Instant.now().plus(AppConstants.FUTURE_TIME_ALLOWANCE_MINS, ChronoUnit.MINUTES);

                if (eventTime.isAfter(limit)) {
                    logger.warn("FutureTimeValidationStrategy: Event time {} is in the future (limit: {}) for eventId: {}",
                        eventTime, limit, event.getEventId());
                    return Optional.of(Constants.FUTURE_EVENT_TIME.getCode());
                }

                logger.debug("FutureTimeValidationStrategy: Event time {} is valid for eventId: {}",
                    eventTime, event.getEventId());
                return Optional.empty();

            } catch (ArithmeticException e) {
                logger.error("FutureTimeValidationStrategy: ArithmeticException while calculating time limit", e);
                return Optional.of(Constants.FUTURE_EVENT_TIME.getCode());
            }

        } catch (NullPointerException e) {
            logger.error("FutureTimeValidationStrategy: NullPointerException during validation", e);
            return Optional.of(Constants.FUTURE_EVENT_TIME.getCode());
        } catch (Exception e) {
            logger.error("FutureTimeValidationStrategy: Unexpected error during validation: {}", e.getMessage(), e);
            return Optional.of(Constants.FUTURE_EVENT_TIME.getCode());
        }
    }
}