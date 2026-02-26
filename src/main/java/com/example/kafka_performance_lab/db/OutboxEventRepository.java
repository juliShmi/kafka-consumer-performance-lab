package com.example.kafka_performance_lab.db;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    @Query(
            value = """
            with first_per_partition as (
              select distinct on (source_partition) id, status, next_attempt_at, source_partition, source_offset
              from outbox_events
              where status in ('PENDING', 'IN_FLIGHT')
              order by source_partition asc, source_offset asc
            )
            select o.*
            from outbox_events o
            join first_per_partition f on f.id = o.id
            where o.status = 'PENDING'
              and o.next_attempt_at <= now()
            order by o.source_partition asc, o.source_offset asc
            for update of o skip locked
            limit :limit
            """,
            nativeQuery = true
    )
    List<OutboxEventEntity> lockNextBatch(@Param("limit") int limit);

    @Modifying
    @Query(
            value = """
            update outbox_events
            set status = 'PENDING',
                locked_at = null,
                next_attempt_at = now(),
                last_error = :reason
            where status = 'IN_FLIGHT'
              and locked_at is not null
              and locked_at < :cutoff
            """,
            nativeQuery = true
    )
    int releaseTimedOutInFlight(@Param("cutoff") Instant cutoff, @Param("reason") String reason);
}

