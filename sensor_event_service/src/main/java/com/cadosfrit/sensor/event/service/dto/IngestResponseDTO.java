package com.cadosfrit.sensor.event.service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import java.util.List;

@Data
@Builder
public class IngestResponseDTO {
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