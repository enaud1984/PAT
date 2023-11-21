CREATE TABLE GEODSS_ADT.log (
	run_id varchar(128) NULL,
	script varchar(64) NULL,
	description varchar(4000) NULL,
	execution_time number NULL,
	success number NULL,
	start_time timestamp NULL,
	end_time timestamp NULL,
	expiration_date timestamp NULL,
	exit_code number NULL,
	app_id varchar(255) NULL,
	log_path varchar(500) NULL
);


CREATE TABLE GEODSS_ADT.log_operation (
	run_id varchar(128) NULL,
	operation varchar(64) NULL,
	start_time timestamp NULL,
	end_time timestamp NULL,
	exit_code number NULL
);


CREATE SEQUENCE GEODSS_ADT.log_exit_code_seq START WITH 1;
CREATE TABLE GEODSS_ADT.log_exit_code (
	exit_code number DEFAULT GEODSS_ADT.log_exit_code_seq.nextval NOT NULL,
	type varchar(200) NULL,
	message varchar(700) NULL
);
ALTER TABLE GEODSS_ADT.log_exit_code ADD (
  CONSTRAINT log_exit_code_pk PRIMARY KEY (exit_code));

CREATE TABLE GEODSS_ADT.log_file (
	run_id varchar(128) NULL,
	filename varchar(200) NULL,
	filepath varchar(1000) NULL,
	exit_code number NULL
);
