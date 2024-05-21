create table test_distribute_bad_1
(
    some_int4        integer,
    some_int8        bigint,
    some_bool        boolean,
    some_float4      real,
    some_float8      double precision,
    some_varchar10   varchar(10),
    some_text        text,
    some_date        date,
    some_time        time,
    some_timestamptz timestamp with time zone,
    some_uuid        uuid,
    bad_distributed  text
)
    with (appendonly = true, orientation = column, compresstype = zstd, compresslevel = 1)
    distributed by (bad_distributed);
