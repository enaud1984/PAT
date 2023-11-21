CREATE TABLE ingestion.log (
	run_id varchar(128) NULL,
	script varchar(64) NULL,
	description varchar(4000) NULL,
	execution_time float4 NULL,
	success int4 NULL,
	start_time timestamp NULL,
	end_time timestamp NULL,
	expiration_date timestamp NULL,
	exit_code int4 NULL,
	app_id varchar(255) NULL,
	log_path varchar(500) NULL
);


CREATE TABLE ingestion.log_operation (
	run_id varchar(128) NULL,
	operation varchar(64) NULL,
	start_time timestamp NULL,
	end_time timestamp NULL,
	exit_code int4 NULL
);

CREATE TABLE ingestion.log_exit_code (
	exit_code SERIAL PRIMARY KEY,
	type varchar(200) NULL,
	message varchar(700) NULL
);

CREATE TABLE ingestion.log_file (
	run_id varchar(128) NULL,
	filename varchar(200) NULL,
	filepath varchar(1000) NULL,
	exit_code int4 NULL
);
