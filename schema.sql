DROP TABLE IF EXISTS atpquery.raw_records;
DROP MATERIALIZED VIEW IF EXISTS atpquery.agg_records;
DROP MATERIALIZED VIEW IF EXISTS atpquery.tombstones;
DROP VIEW IF EXISTS atpquery.records;

CREATE TABLE atpquery.raw_records (
    repo STRING NOT NULL,
    collection STRING,
    rkey STRING,
    rev INT64,
    record JSON
);

CREATE MATERIALIZED VIEW atpquery.agg_records AS (
    SELECT
        repo,
        collection,
        rkey,
        ARRAY_AGG(rev ORDER BY rev DESC) AS revs,
        ARRAY_AGG(COALESCE(record, JSON 'null') ORDER BY rev DESC) AS records
    FROM atpquery.raw_records
    WHERE collection IS NOT NULL AND rkey IS NOT NULL AND rev IS NOT NULL
    GROUP BY repo, collection, rkey
);

CREATE MATERIALIZED VIEW atpquery.tombstones AS (
    SELECT repo
    FROM atpquery.raw_records
    WHERE collection IS NULL AND rkey IS NULL AND rev IS NULL
);

CREATE VIEW atpquery.records AS (
    SELECT
        repo,
        collection,
        rkey,
        revs[0] AS rev,
        records[0] AS record
    FROM atpquery.agg_records
    WHERE JSON_TYPE(records[0]) != 'null'
);
