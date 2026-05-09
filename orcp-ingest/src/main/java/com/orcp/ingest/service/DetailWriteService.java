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
 * 原子性地声明 eventId 所有权（{@code INSERT IGNORE t_dedup}）并持久化订单行。
 * 这是唯一向明细库写入数据的地方；Kafka listener 在本事务提交后再转发到 mid topic。
 *
 * <p>写入前置模式：
 * <pre>
 *   BEGIN
 *     INSERT IGNORE t_dedup  -- 返回 0 行 -> 重复，走 ROLLBACK 路径
 *     INSERT t_order
 *   COMMIT
 *   （返回 true = 调用方应转发到 mid topic）
 *   （返回 false = 重复，跳过转发）
 * </pre>
 *
 * <p>若 Kafka 转发步骤后来失败导致 listener 抛出异常，Kafka 会重投该消息；
 * 去重账本使得重放成为空操作，确保幂等性。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DetailWriteService {

    private final DedupMapper dedupMapper;
    private final OrderMapper orderMapper;

    /**
     * @return {@code true} 表示本次调用成功声明了 {@code event.eventId} 的所有权，
     *         调用方应将事件转发到 mid topic；
     *         {@code false} 表示该 eventId 已被其他调用先行处理，应丢弃本次事件。
     */
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public boolean tryClaimAndPersist(SourceEvent event) {
        final String eventId = event.getEventId();
        final LocalDateTime now = LocalDateTime.now();

        int inserted;
        try {
            inserted = dedupMapper.insertIgnore(eventId, now);
        } catch (DuplicateKeyException e) {
            // 正常情况下 INSERT IGNORE 不会抛出 DuplicateKeyException，
            // 但某些驱动版本或隔离级别下可能有差异，统一作重复处理。
            log.debug("通过异常识别为重复事件：{}", eventId);
            return false;
        }
        if (inserted == 0) {
            log.debug("t_dedup 中已存在该 eventId，跳过：{}", eventId);
            return false;
        }

        OrderEntity order;
        try {
            order = toOrder(event, now);
        } catch (Exception e) {
            throw new IngestException("将 SourceEvent 映射为 OrderEntity 失败，eventId=" + eventId, e);
        }

        try {
            orderMapper.insert(order);
        } catch (DuplicateKeyException e) {
            // 极端情况：前一次尝试声明了 t_dedup 但在 order 写入前崩溃，
            // 重投后重新声明 t_dedup 但 t_order 主键已存在。
            // 由于这两个插入在同一事务中，理论上不应出现此情况，
            // 但若出现则继续执行——order 行已存在，数据完整。
            log.warn("t_order 中已存在 order_id={}，继续执行（eventId={}）",
                    order.getOrderId(), eventId);
        }

        log.info("已持久化 eventId={} orderId={} bizType={} customerId={}",
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
     * 上游 bizKey 格式为 {@code order-0000000123}；
     * t_order 使用 BIGINT 主键，因此截取后缀数字并解析。
     */
    static long parseOrderIdFromBizKey(String bizKey) {
        if (bizKey == null) {
            throw new IngestException("bizKey 为 null");
        }
        int dash = bizKey.lastIndexOf('-');
        String numeric = dash >= 0 ? bizKey.substring(dash + 1) : bizKey;
        try {
            return Long.parseLong(numeric);
        } catch (NumberFormatException e) {
            throw new IngestException("无法从 bizKey 中解析数字后缀：" + bizKey, e);
        }
    }
}
