-- 1. FAST SP: Set-based processing using a Staging Table
DROP PROCEDURE IF EXISTS process_event_batch;;

CREATE PROCEDURE process_event_batch(IN jsonBatch JSON)
BEGIN
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
    CREATE TEMPORARY TABLE IF NOT EXISTS staging_events (
        event_id VARCHAR(50) PRIMARY KEY,
        machine_id VARCHAR(50),
        event_time DATETIME(6),
        received_time DATETIME(6),
        duration_ms BIGINT,
        defect_count INT
    );

    TRUNCATE TABLE staging_events;

    INSERT INTO staging_events (event_id, machine_id, event_time, received_time, duration_ms, defect_count)
    SELECT
        jt.event_id,
        jt.machine_id,
        CAST(REPLACE(jt.event_time, 'Z', '') AS DATETIME(6)),
        CAST(REPLACE(jt.received_time, 'Z', '') AS DATETIME(6)),
        jt.duration_ms,
        jt.defect_count
    FROM JSON_TABLE(jsonBatch, '$[*]' COLUMNS (
        event_id VARCHAR(50) PATH '$.event_id',
        machine_id VARCHAR(50) PATH '$.machine_id',
        event_time VARCHAR(50) PATH '$.event_time',
        received_time VARCHAR(50) PATH '$.received_time',
        duration_ms BIGINT PATH '$.duration_ms',
        defect_count INT PATH '$.defect_count'
    )) AS jt
    ON DUPLICATE KEY UPDATE
        machine_id = VALUES(machine_id),
        event_time = VALUES(event_time),
        received_time = VALUES(received_time),
        duration_ms = VALUES(duration_ms),
        defect_count = VALUES(defect_count);

    SELECT
        CASE
            WHEN t.event_id IS NULL THEN 'ACCEPTED'
            WHEN (
                t.machine_id = s.machine_id AND
                t.event_time = s.event_time AND
                t.duration_ms = s.duration_ms AND
                t.defect_count = s.defect_count
            ) THEN 'DEDUPED'
            ELSE 'UPDATED'
        END AS status,
        COUNT(*) AS count
    FROM staging_events s
    LEFT JOIN machine_events t ON s.event_id = t.event_id
    GROUP BY status;

    INSERT INTO machine_events (event_id, machine_id, event_time, received_time, duration_ms, defect_count)
    SELECT event_id, machine_id, event_time, received_time, duration_ms, defect_count
    FROM staging_events
    ON DUPLICATE KEY UPDATE
        machine_id = VALUES(machine_id),
        event_time = VALUES(event_time),
        received_time = VALUES(received_time),
        duration_ms = VALUES(duration_ms),
        defect_count = VALUES(defect_count);

    DROP TEMPORARY TABLE IF EXISTS staging_events;
    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
END;;

-- 2. SEQUENTIAL SP: Row-by-row processing using a Cursor
DROP PROCEDURE IF EXISTS process_batch_sequential;;

CREATE PROCEDURE process_batch_sequential(IN jsonBatch JSON)
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_event_id VARCHAR(50);
    DECLARE v_machine_id VARCHAR(50);
    DECLARE v_event_time DATETIME(6);
    DECLARE v_received_time DATETIME(6);
    DECLARE v_duration_ms BIGINT;
    DECLARE v_defect_count INT;

    DECLARE v_accepted INT DEFAULT 0;
    DECLARE v_updated INT DEFAULT 0;
    DECLARE v_deduped INT DEFAULT 0;

    DECLARE db_machine_id VARCHAR(50);
    DECLARE db_duration_ms BIGINT;
    DECLARE db_event_time DATETIME(6);
    DECLARE db_defect_count INT;
    DECLARE row_exists INT;

    DECLARE event_cursor CURSOR FOR
    SELECT
        event_id, machine_id,
        CAST(REPLACE(event_time, 'Z', '') AS DATETIME(6)),
        CAST(REPLACE(received_time, 'Z', '') AS DATETIME(6)),
        duration_ms, defect_count
    FROM JSON_TABLE(jsonBatch, '$[*]' COLUMNS (
        event_id VARCHAR(50) PATH '$.event_id',
        machine_id VARCHAR(50) PATH '$.machine_id',
        event_time VARCHAR(50) PATH '$.event_time',
        received_time VARCHAR(50) PATH '$.received_time',
        duration_ms BIGINT PATH '$.duration_ms',
        defect_count INT PATH '$.defect_count'
    )) AS jt;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
    OPEN event_cursor;

    read_loop: LOOP
        FETCH event_cursor INTO v_event_id, v_machine_id, v_event_time, v_received_time, v_duration_ms, v_defect_count;
        IF done THEN
            LEAVE read_loop;
        END IF;

        SELECT count(*), MAX(machine_id), MAX(duration_ms), MAX(defect_count), MAX(event_time)
        INTO row_exists, db_machine_id, db_duration_ms, db_defect_count, db_event_time
        FROM machine_events
        WHERE event_id = v_event_id;

        IF row_exists = 0 THEN
            INSERT INTO machine_events (event_id, machine_id, event_time, received_time, duration_ms, defect_count)
            VALUES (v_event_id, v_machine_id, v_event_time, v_received_time, v_duration_ms, v_defect_count);
            SET v_accepted = v_accepted + 1;
        ELSE
            IF (v_machine_id = db_machine_id AND v_duration_ms = db_duration_ms AND v_defect_count = db_defect_count AND v_event_time = db_event_time) THEN
                SET v_deduped = v_deduped + 1;
            ELSE
                UPDATE machine_events
                SET machine_id = v_machine_id,
                    event_time = v_event_time,
                    received_time = v_received_time,
                    duration_ms = v_duration_ms,
                    defect_count = v_defect_count
                WHERE event_id = v_event_id;
                SET v_updated = v_updated + 1;
            END IF;
        END IF;
    END LOOP;

    CLOSE event_cursor;

    SELECT 'ACCEPTED' as status, v_accepted as count
    UNION ALL
    SELECT 'UPDATED' as status, v_updated as count
    UNION ALL
    SELECT 'DEDUPED' as status, v_deduped as count;
    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
END;;