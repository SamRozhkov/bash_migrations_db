create table public.test_distribute_bad_2
(
    some_int4_2        integer,
    some_int8_2        bigint,
    some_bool_2        boolean,
    some_float4_2      real,
    some_float8_2      double precision,
    some_varchar10_2   varchar(10),
    some_text_2        text,
    some_date_2        date,
    some_time_2        time,
    some_timestamptz_2 timestamp with time zone,
    some_uuid_2        uuid,
    bad_distributed_2  text
)
    with (appendonly = true, orientation = column, compresstype = zstd, compresslevel = 1)
    distributed by (bad_distributed_2);
