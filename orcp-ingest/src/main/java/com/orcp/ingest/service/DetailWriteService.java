package com.orcp.ingest.service;

import com.orcp.common.dto.SourceEvent;
import com.orcp.ingest.entity.OrderEntity;
import com.orcp.ingest.exception.IngestException;
import com.orcp.ingest.mapper.DedupMapper;
import com.orcp.ingest.mapper.OrderMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Atomically claims the eventId (via {@code INSERT IGNORE t_dedup}) and
 * persists the order row.  This is the ONLY place where we commit detail
 * rows; the Kafka listener calls us inside its own thread and then forwards
 * to the mid topic AFTER our transaction has committed.
 *
 * <p>Write-ahead pattern:
 * <pre>
 *   BEGIN
 *     INSERT IGNORE t_dedup  -- if 0 rows affected -> duplicate, ROLLBACK path
 *     INSERT t_order
 *   COMMIT
 *   (return true = please forward to mid topic)
 *   (return false = duplicate, skip forward)
 * </pre>
 *
 * <p>If the forward-to-Kafka step later fails and the listener throws, the
 * message is redelivered; the dedup ledger makes that redelivery a no-op.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DetailWriteService {

    private final DedupMapper dedupMapper;
    private final OrderMapper orderMapper;

    /**
     * @return {@code true} if this call is the writer of record for {@code event.eventId}
     *         and the caller should forward to the mid topic; {@code false} if another
     *         attempt has already succeeded and we should drop this one.
     */
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public boolean tryClaimAndPersist(SourceEvent event) {
        final String eventId = event.getEventId();
        final LocalDateTime now = LocalDateTime.now();

        int inserted;
        try {
            inserted = dedupMapper.insertIgnore(eventId, now);
        } catch (DuplicateKeyException e) {
            // INSERT IGNORE should not raise DuplicateKeyException in the first
            // place on a simple single-column PK; but some drivers and some
            // isolation levels wrap things differently, so treat it as a dup.
            log.debug("Dedup insert treated as duplicate via exception: {}", eventId);
            return false;
        }
        if (inserted == 0) {
            log.debug("eventId already present in t_dedup, skipping: {}", eventId);
            return false;
        }

        OrderEntity order;
        try {
            order = toOrder(event, now);
        } catch (Exception e) {
            throw new IngestException("Failed to map SourceEvent -> OrderEntity for " + eventId, e);
        }

        try {
            orderMapper.insert(order);
        } catch (DuplicateKeyException e) {
            // A prior attempt claimed t_dedup but crashed before the order
            // insert landed, then a re-delivery recreated the dedup row?
            // Extremely unlikely because the two inserts are in the same
            // transaction, but if it ever happens, keep going -- we already
            // have the order row persisted.
            log.warn("t_order already contains order_id={}, continuing (eventId={})",
                    order.getOrderId(), eventId);
        }

        log.info("persisted eventId={} orderId={} bizType={} customerId={}",
                eventId, order.getOrderId(), order.getBizType(), order.getCustomerId());
        return true;
    }

    private static OrderEntity toOrder(SourceEvent event, LocalDateTime createdAt) {
        return OrderEntity.builder()
                .orderId(parseOrderIdFromBizKey(event.getBizKey()))
                .customerId(event.getCustomerId())
                .bizType(event.getBizType())
                .amount(event.getAmount())
                .status("NEW")
                .createdAt(event.getEventTime() != null ? event.getEventTime() : createdAt)
                .updatedAt(createdAt)
                .build();
    }

    /**
     * The upstream bizKey has the form {@code order-0000000123}.  We keep a
     * BIGINT primary key in t_order, so strip the prefix and parse the suffix.
     */
    static long parseOrderIdFromBizKey(String bizKey) {
        if (bizKey == null) {
            throw new IngestException("bizKey is null");
        }
        int dash = bizKey.lastIndexOf('-');
        String numeric = dash >= 0 ? bizKey.substring(dash + 1) : bizKey;
        try {
            return Long.parseLong(numeric);
        } catch (NumberFormatException e) {
            throw new IngestException("Cannot parse numeric suffix from bizKey: " + bizKey, e);
        }
    }
}
