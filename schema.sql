CREATE TABLE atpquery.raw_records (
    repo STRING NOT NULL,
    collection STRING NOT NULL,
    rkey STRING NOT NULL,
    rev INT64 NOT NULL,
    record JSON
);

ALTER TABLE atpquery.raw_records ADD PRIMARY KEY (repo, collection, rkey, rev) NOT ENFORCED;
