create table "CHOICEBI"."QUEST_SL_EXPLAIN1" 
( STATEMENT_ID           VARCHAR2(30), 
       PLAN_ID                NUMBER, 
       TIMESTAMP              DATE, 
       REMARKS                VARCHAR2(4000), 
       OPERATION              VARCHAR2(30), 
       OPTIONS                VARCHAR2(255), 
       OBJECT_NODE            VARCHAR2(128), 
       OBJECT_OWNER           VARCHAR2(30), 
       OBJECT_NAME            VARCHAR2(30), 
       OBJECT_ALIAS           VARCHAR2(65), 
       OBJECT_INSTANCE        NUMERIC, 
       OBJECT_TYPE            VARCHAR2(30), 
       OPTIMIZER              VARCHAR2(255), 
       SEARCH_COLUMNS         NUMBER, 
       ID                     NUMERIC, 
       PARENT_ID              NUMERIC, 
       DEPTH                  NUMERIC, 
       POSITION               NUMERIC, 
       COST                   NUMERIC, 
       CARDINALITY            NUMERIC, 
       BYTES                  NUMERIC, 
       OTHER_TAG              VARCHAR2(255), 
       PARTITION_START        VARCHAR2(255), 
       PARTITION_STOP         VARCHAR2(255), 
       PARTITION_ID           NUMERIC, 
       OTHER                  LONG, 
       OTHER_XML              CLOB, 
       DISTRIBUTION           VARCHAR2(30), 
       CPU_COST               NUMERIC, 
       IO_COST                NUMERIC, 
       TEMP_SPACE             NUMERIC, 
       ACCESS_PREDICATES      VARCHAR2(4000), 
       FILTER_PREDICATES      VARCHAR2(4000), 
       PROJECTION             VARCHAR2(4000), 
       TIME                   NUMBER(20,2), 
       QBLOCK_NAME            VARCHAR2(30) 
) 
               
/