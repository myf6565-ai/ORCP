package com.orcp.ingest.api;

import com.orcp.common.model.EventRequest;
import com.orcp.common.web.ApiResponse;
import com.orcp.ingest.service.IngestEventService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/events")
public class IngestController {

    private final IngestEventService ingestEventService;

    public IngestController(IngestEventService ingestEventService) {
        this.ingestEventService = ingestEventService;
    }

    @PostMapping
    public ApiResponse<Map<String, String>> ingest(@RequestBody @Valid EventRequest request) {
        ingestEventService.ingest(request);
        return ApiResponse.success(Map.of("eventId", request.eventId(), "status", "accepted"));
    }
}
