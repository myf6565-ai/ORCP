package com.orcp.ingest.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;

/**
 * Fast first-tier dedup cache.
 *
 * <p>Combined with {@code INSERT IGNORE INTO t_dedup} (second tier) this
 * gives us the two-layer scheme called out by DEV_SPEC §6.2:
 *
 * <ul>
 *   <li>Caffeine is in-process only and may miss right after a restart,
 *       but catches ~99% of duplicates without hitting the DB.</li>
 *   <li>The DB table is authoritative and race-free across instances.</li>
 * </ul>
 *
 * <p>Caffeine only answers "might-have-seen" (positive cache).  We do NOT
 * treat a miss here as "never seen" -- the DB is always consulted.
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
        log.info("Caffeine dedup cache initialised: maxSize={}, ttl={}min",
                maxSize, ttlMinutes);
    }

    /** @return true iff the cache has seen this eventId recently. */
    public boolean isCachedHit(String eventId) {
        return recentlySeen.getIfPresent(eventId) != null;
    }

    /** Record that we have successfully processed this eventId. */
    public void markSeen(String eventId) {
        recentlySeen.put(eventId, Boolean.TRUE);
    }
}
