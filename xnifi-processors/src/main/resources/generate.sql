create table XNIFI_GENERATED_SEQUENCE
(
   SEQ_CODE             VARCHAR(64)         not null,
   STRATEGY             VARCHAR(36),
   STEP                 VARCHAR(36),
   FORMAT               VARCHAR(64),
   INIT_VALUE           VARCHAR(64),
   MAX_VALUE            VARCHAR(64),
   VALUE                VARCHAR(64),
   PREV_VALUE           VARCHAR(64),
   LAST_UPDATE_TIME     DATETIME,
   constraint PK_XNIFI_GENERATED_SEQUENCE primary key (SEQ_CODE)
)