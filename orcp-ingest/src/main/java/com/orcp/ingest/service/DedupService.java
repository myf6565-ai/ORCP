package com.orcp.ingest.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;

/**
 * 快速第一层去重缓存（Caffeine LRU）。
 *
 * <p>与 {@code INSERT IGNORE INTO t_dedup}（第二层 DB 去重）配合使用，
 * 实现 DEV_SPEC §6.2 描述的双层去重方案：
 *
 * <ul>
 *   <li>Caffeine 仅存在于当前进程，重启后失效，但可拦截 ~99% 的重复事件而无需访问 DB。</li>
 *   <li>DB 表是跨实例的权威去重账本，具备竞争安全性。</li>
 * </ul>
 *
 * <p>Caffeine 只回答"可能已见过"（正向缓存），
 * 未命中时 <strong>不</strong>意味着"从未见过"——DB 仍需查询。
 */
@Slf4j
@Service
public class DedupService {

    @Value("${orcp.dedup.caffeine-max-size:10000}")
    private long maxSize;

    @Value("${orcp.dedup.caffeine-ttl-minutes:10}")
    private long ttlMinutes;

    private Cache<String, Boolean> recentlySeen;

    @PostConstruct
    void init() {
        this.recentlySeen = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofMinutes(ttlMinutes))
                .recordStats()
                .build();
        log.info("Caffeine 去重缓存初始化完成：maxSize={}，ttl={}min", maxSize, ttlMinutes);
    }

    /** @return true 表示缓存最近已见过该 eventId。 */
    public boolean isCachedHit(String eventId) {
        return recentlySeen.getIfPresent(eventId) != null;
    }

    /** 标记该 eventId 已成功处理。 */
    public void markSeen(String eventId) {
        recentlySeen.put(eventId, Boolean.TRUE);
    }
}
