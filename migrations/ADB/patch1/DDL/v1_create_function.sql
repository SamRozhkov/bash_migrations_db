create or replace function test_distribute() returns integer
    language plpgsql
as
$$
declare
	function_time_one timestamp := now();
	bad_distribute text;
	number_insert_records bigint = 100000;
	number_searched bigint;
	comment_operation_text text;
	start_datetime_execute timestamp;
	table_searched text;
	time_execute interval;
begin
	create table if not exists logger.load_testing_checkpoint (
		start_date timestamp,
		table_name text,
		time_execute_operation interval,
		total_loaded bigint,
		oriented text,
		compresstype text,
		compresslevel int,
		compress_column text,
		comment_operation text
	)
	DISTRIBUTED REPLICATED;
	COMMENT ON TABLE logger.load_testing_checkpoint IS 'Таблица для логирования нагрузочного тестирования';
	comment on column logger.load_testing_checkpoint.start_date is 'Время начала выполнения хранимой функции';
	comment on column logger.load_testing_checkpoint.table_name is 'Наименование таблицы с помощью которой выполнялась операция';
	comment on column logger.load_testing_checkpoint.time_execute_operation is 'Время выполнения операции';
	comment on column logger.load_testing_checkpoint.total_loaded is 'Общее количество записей в объекте ADB';
	comment on column logger.load_testing_checkpoint.oriented is 'Тип хранения таблицы';
	comment on column logger.load_testing_checkpoint.compresstype is 'Кодек сжатия';
	comment on column logger.load_testing_checkpoint.compresslevel is 'Уровень сжатия';
	comment on column logger.load_testing_checkpoint.compress_column is 'Сжимаемая колонка';
	comment on column logger.load_testing_checkpoint.comment_operation is 'Описание операции';

---- объединение таблиц
	start_datetime_execute = clock_timestamp();
	perform
	join1.*, join2.*
	from
	public.test_distribute_bad_1 join1
	left join
	public.test_distribute_bad_2 join2
	on join1.bad_distributed = join2.bad_distributed;
	get diagnostics number_searched = row_count;
	time_execute = (clock_timestamp() - start_datetime_execute);
	comment_operation_text = 'Объединение таблица размером:' || number_insert_records || ' с плохим ключом дистрибьюции';
	insert into logger.load_testing_checkpoint (start_date, table_name, time_execute_operation, total_loaded, comment_operation) values
		(function_time_one, table_searched,  time_execute, number_searched, comment_operation_text);

--------test_distribute_good_1
	start_datetime_execute = clock_timestamp();
	table_searched = 'test_distribute_good_1';
	drop table if exists public.test_distribute_good_1;
	create table public.test_distribute_good_1
		with (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)
		as
		select
		(random()*10)::int4 as some_int4,
		(random()*100)::int8 as some_int8,
		random()::int::bool as some_bool,
		(random()*0.123456)::float4 as some_float4,
		(random()*0.1234567891234567)::float8 as some_float8,
		md5(random()::text)::varchar(10) as some_varchar10,
		md5(random()::text) as some_text,
		(now() - interval '2 year' + interval '1 year' * random())::date as some_date,
		(now() - interval '2 hour' + interval '1 hour' * random())::time as some_time,
		(now() - interval '1 day' * round(random()*100))::timestamptz as some_timestamptz,
		md5(random()::text || clock_timestamp()::text)::uuid as some_uuid,
		'bad_distributed_1' :: text as bad_distributed
		from generate_series(1,number_insert_records)
		distributed by (some_uuid);
	get diagnostics number_searched = row_count;
	comment_operation_text = 'Вставка записей с хорошим ключом дистрибьюции';
	time_execute = (clock_timestamp() - start_datetime_execute);
	insert into logger.load_testing_checkpoint (start_date, table_name, time_execute_operation, total_loaded, comment_operation) values
		(function_time_one, table_searched,  time_execute, number_searched, comment_operation_text);

------test_distribute_good_2
	start_datetime_execute = clock_timestamp();
	table_searched = 'test_distribute_good_2';
	drop table if exists public.test_distribute_good_2;
	create table public.test_distribute_good_2
		with (appendonly=true, orientation=column, compresstype=zstd, compresslevel=1)
		as
		select
		(random()*20)::int4 as some_int4,
		(random()*150)::int8 as some_int8,
		random()::int::bool as some_bool,
		(random()*0.123456)::float4 as some_float4,
		(random()*0.1234567891234567)::float8 as some_float8,
		md5(random()::text)::varchar(10) as some_varchar10,
		md5(random()::text) as some_text,
		(now() - interval '2 year' + interval '1 year' * random())::date as some_date,
		(now() - interval '2 hour' + interval '1 hour' * random())::time as some_time,
		(now() - interval '1 day' * round(random()*100))::timestamptz as some_timestamptz,
		md5(random()::text || clock_timestamp()::text)::uuid as some_uuid,
		'bad_distributed_2' :: text as bad_distributed
		from generate_series(1,number_insert_records)
		distributed by (some_uuid);
	get diagnostics number_searched = row_count;
	comment_operation_text = 'Вставка записей с хорошим ключом дистрибьюции';
	time_execute = (clock_timestamp() - start_datetime_execute);
	insert into logger.load_testing_checkpoint (start_date, table_name, time_execute_operation, total_loaded, comment_operation) values
		(function_time_one, table_searched,  time_execute, number_searched, comment_operation_text);

---- объединение таблиц
	start_datetime_execute = clock_timestamp();
	perform
	join1.*, join2.*
	from
	public.test_distribute_good_1 join1
	left join
	public.test_distribute_good_2 join2
	on join1.some_uuid = join2.some_uuid;
	get diagnostics number_searched = row_count;
	time_execute = (clock_timestamp() - start_datetime_execute);
	comment_operation_text = 'Объединение таблица размером:' || number_insert_records || ' с хорошим ключом дистрибьюции';
	insert into logger.load_testing_checkpoint (start_date, table_name, time_execute_operation, total_loaded, comment_operation) values
		(function_time_one, table_searched,  time_execute, number_searched, comment_operation_text);

    return 1;
end;
$$;
