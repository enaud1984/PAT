CREATE TABLE ingestion.istat_import_export
(
    nazione         STRING,
    export_peso_km  DOUBLE,
    import_peso_km  DOUBLE,
    export_peso     DOUBLE,
    import_peso     DOUBLE,
    categoria_merce STRING,
    tipo_trasporto  STRING,
    ingestion_dttm  TIMESTAMP,
    modifica_dttm   TIMESTAMP
) PARTITIONED BY (anno_rif INT)
STORED AS PARQUET
;

CREATE TABLE ingestion.istat_merci
(
    categoria_merce STRING,
    peso            DOUBLE,
    peso_km         DOUBLE,
    ingestion_dttm  TIMESTAMP,
    modifica_dttm   TIMESTAMP
) PARTITIONED BY (anno_rif INT)
STORED AS PARQUET
;

CREATE TABLE ingestion.istat_merci_pericolose
(
    categoria_merce      STRING,
    merce_pericolosa_flg STRING,
    peso                 DOUBLE,
    peso_km              DOUBLE,
    modalita_trasporto   STRING,
    ingestion_dttm       TIMESTAMP,
    modifica_dttm        TIMESTAMP
) PARTITIONED BY (anno_rif INT)
STORED AS PARQUET
;

CREATE TABLE ingestion.pic_carri
(
    id_treno          STRING,
    id_tratta         STRING,
    arrivo_dttm       TIMESTAMP,
    partenza_dttm     TIMESTAMP,
    origine           STRING,
    destinazione      STRING,
    id_carro          STRING,
    distanza_percorsa DOUBLE,
    carro_retr_flg    STRING,
    ingestion_dttm    TIMESTAMP,
    modifica_dttm     TIMESTAMP
) PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
;

CREATE TABLE ingestion.pic_merce
(
    id_treno             STRING,
    id_tratta            STRING,
    arrivo_dttm          TIMESTAMP,
    partenza_dttm        TIMESTAMP,
    origine              STRING,
    destinazione         STRING,
    tipologia_trasporti  STRING,
    peso_merce           DOUBLE,
    merce_pericolosa_flg STRING,
    categoria_merce      STRING,
    progressivo          INT,
    ingestion_dttm       TIMESTAMP,
    modifica_dttm        TIMESTAMP
) PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
;

CREATE TABLE ingestion.pic_treni
(
    id_treno           VARCHAR(255),
    id_tratta          VARCHAR(255),
    arrivo_dttm        TIMESTAMP,
    partenza_dttm      TIMESTAMP,
    origine            VARCHAR(255),
    destinazione       VARCHAR(255),
    arrivo_eff_dttm    TIMESTAMP,
    partenza_eff_dttm  TIMESTAMP,
    tipologia_trazione VARCHAR(255),
    treno_km           DOUBLE,
    distanza_percorsa  DOUBLE,
    nodo_riferimento   VARCHAR(255),
    ingestion_dttm     TIMESTAMP,
    modifica_dttm      TIMESTAMP
) PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET
;