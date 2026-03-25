package com.orcp.ingest;

import com.orcp.common.kafka.KafkaTopics;
import com.orcp.common.kafka.producer.EventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class IngestControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EventProducer eventProducer;

    @MockBean
    private KafkaTopics kafkaTopics;

    @Test
    void shouldAcceptEvent() throws Exception {
        org.mockito.BDDMockito.given(kafkaTopics.rawEvents()).willReturn("raw-events");

        String payload = """
                {
                  "eventId":"evt-1",
                  "bizKey":"user-1",
                  "eventType":"ORDER_CREATED",
                  "eventTime":"2026-03-25T10:00:00Z",
                  "payload":{"amount":100}
                }
                """;

        mockMvc.perform(post("/api/events")
                        .contentType("application/json")
                        .content(payload))
                .andExpect(status().isOk());
    }
}
