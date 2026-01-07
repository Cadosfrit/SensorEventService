package com.cadosfrit.sensor_event_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class IngestResponseDTO {
    private String status;
    private int accepted;
    private int deduped;
    private int updated;
    private int rejected;
    private List<Rejection> rejections;

    @Data
    @Builder
    @AllArgsConstructor
    public static class Rejection {
        private String eventId;
        private String reason;
    }
}