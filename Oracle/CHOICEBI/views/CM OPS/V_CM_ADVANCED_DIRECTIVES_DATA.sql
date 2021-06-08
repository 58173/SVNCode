DROP VIEW CHOICEBI.V_CM_ADVANCED_DIRECTIVES_DATA;

/* Formatted on 6/7/2021 8:18:08 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_ADVANCED_DIRECTIVES_DATA
(
    MONTH_ID,
    DL_CM_MEASURE_SK,
    MEMBER_ID,
    DL_LOB_ID,
    SUBSCRIBER_ID,
    DL_PLAN_SK,
    DL_REF_CM_SCRIPT_SK,
    SCRIPT_ID,
    HEALTH_NOTE_TYPE_ID,
    PATIENT_FOLLOWUP_ID,
    DL_ASSESS_SK,
    CM_MEMBER_ID,
    CCM_MEMBER_ID,
    PATIENT_FORM_ID,
    SRC_KEY_DESC1,
    SRC_KEY1,
    SRC_KEY_DESC2,
    SRC_KEY2,
    ACTIVITY_DATE,
    NUM,
    DENUM,
    NOT_COMPLETED_AGING,
    LAST_UAS_DATE,
    LAST_PSP_DATE
)
BEQUEATH DEFINER
AS
    WITH
        AdvDir_data
        AS
            (SELECT *
               FROM (SELECT a.*,
                            ROW_NUMBER ()
                                OVER (
                                    PARTITION BY subscriber_id,
                                                 SEMI_ANNUAL_ID
                                    ORDER BY ADDRESSED_DATE DESC)    seq
                       FROM (SELECT /*+ no_merge materialize */
                                    LUM.SEMI_ANNUAL_ID,
                                    TO_DATE (MONTH_ID, 'YYYYMM')
                                        MONTH_ID_DT,
                                    MONTH_ID,
                                    A.PATIENT_ID,
                                    B.UNIQUE_ID
                                        SUBSCRIBER_ID,
                                    A.ADDRESSED_DATE,
                                    PAT_ADV_DIR_ID,
                                    A.ADV_DIR_IND_ID
                               FROM CMGC.PAT_ADV_DIR_INDICATORS  A
                                    JOIN MSTRSTG.LU_MONTH lum
                                        ON (lum.month_id =
                                            TO_CHAR (addressed_date,
                                                     'YYYYMM'))
                                    JOIN CMGC.PATIENT_DETAILS B
                                        ON (A.PATIENT_ID = B.PATIENT_ID)
                                    JOIN CMGC.ADV_DIR_INDICATORS c
                                        ON (A.ADV_DIR_IND_ID =
                                            c.ADV_DIR_INDICATOR_ID)
                              WHERE                   --adv_dir_ind_id = 1 AND
                                    ADDRESSED_DATE >= '01-JAN-2018'
                             UNION ALL
                             SELECT LUM.SEMI_ANNUAL_ID,
                                    A.CREATED_ON
                                        MONTH_ID_DT,
                                    TO_NUMBER (
                                        TO_CHAR (A.CREATED_ON, 'YYYYMM'))
                                        MONTH_ID,
                                    A.PATIENT_ID,
                                    B.UNIQUE_ID
                                        SUBSCRIBER_ID,
                                    A.CREATED_ON,
                                    A.RECORD_ID
                                        PARAMETER_ID,
                                    C.PARAMETER_ID
                                        AS AD_TYPE
                               FROM CMGC.HEALTH_INDICATOR_RECORD  A
                                    JOIN CMGC.PATIENT_DETAILS B
                                        ON A.PATIENT_ID = B.PATIENT_ID
                                    JOIN MSTRSTG.LU_MONTH lum
                                        ON (lum.month_id =
                                            TO_CHAR (A.CREATED_ON, 'YYYYMM'))
                                    LEFT JOIN
                                    CMGC.HEALTH_INDICATOR_PARAMETER C
                                        ON A.PARAMETER_ID = C.PARAMETER_ID
                              WHERE     A.PARAMETER_ID BETWEEN 63 AND 70
                                    AND B.UNIQUE_ID IS NOT NULL) A)
              WHERE seq = 1),
        /*AdvDir_data
        AS
            (SELECT *
               FROM (SELECT /*+ no_merge materialize * /
                            lum.semi_annual_id,
                            TO_DATE (month_id, 'YYYYMM')             MONTH_ID_DT,
                            MONTH_ID,
                            A.PATIENT_ID,
                            B.UNIQUE_ID                              SUBSCRIBER_ID,
                            a.addressed_date,
                            PAT_ADV_DIR_ID,
                            A.ADV_DIR_IND_ID,
                            ROW_NUMBER ()
                                OVER (
                                    PARTITION BY unique_id,
                                                 lum.SEMI_ANNUAL_ID
                                    ORDER BY addressed_date DESC)    seq
                       FROM cmgc.PAT_ADV_DIR_INDICATORS  A
                            JOIN MSTRSTG.LU_MONTH lum
                                ON (lum.month_id =
                                    TO_CHAR (addressed_date, 'YYYYMM'))
                            JOIN CMGC.PATIENT_DETAILS B
                                ON (A.PATIENT_ID = B.PATIENT_ID)
                            JOIN CMGC.ADV_DIR_INDICATORS c
                                ON (A.ADV_DIR_IND_ID = c.ADV_DIR_INDICATOR_ID)
                      WHERE                           --adv_dir_ind_id = 1 AND
                            addressed_date >= '01-JAN-2018')
              WHERE seq = 1),*/
        v_adv_dir
        AS
            (SELECT                             --a.month_id, a.subscriber_id,
                    A.MONTH_ID,
                    5                                 DL_CM_MEASURE_SK,
                    MEMBER_ID,
                    DL_LOB_ID,
                    A.SUBSCRIBER_ID,
                    DL_PLAN_SK,
                    NULL                              DL_REF_CM_SCRIPT_SK,
                    NULL                              SCRIPT_ID,
                    NULL                              HEALTH_NOTE_TYPE_ID,
                    NULL                              PATIENT_FOLLOWUP_ID,
                    NULL                              DL_ASSESS_SK,
                    NULL                              CM_MEMBER_ID,
                    NULL                              CCM_MEMBER_ID,
                    NULL                              PATIENT_FORM_ID,
                    'PAT_ADV_DIR_ID'                  SRC_KEY_DESC1,
                    PAT_ADV_DIR_ID                    SRC_KEY1,
                    'ADV_DIR_IND_ID'                  SRC_KEY_DESC2,
                    ADV_DIR_IND_ID                    SRC_KEY2,
                    b.addressed_date                  ACTIVITY_DATE,
                    NVL2 (b.addressed_date, 1, 0)     NUM,
                    1                                 DENUM,
                    NULL                              NOT_COMPLETED_AGING,
                    NULL                              LAST_UAS_DATE,
                    NULL                              LAST_PSP_DATE
               FROM V_DIM_SEMI_ANNUAL_MEMBER_CENSUS  A
                    LEFT JOIN advdir_data B
                        --ON (B.SUBSCRIBER_ID = A.SUBSCRIBER_ID AND A.month_id = B.MONTH_ID)
                        ON (    B.SUBSCRIBER_ID = A.SUBSCRIBER_ID
                            AND b.month_id <= A.month_id
                            AND b.semi_annual_id = a.semi_annual_id))
    SELECT                                      --a.month_id, a.subscriber_id,
           MONTH_ID,
           DL_CM_MEASURE_SK,
           MEMBER_ID,
           DL_LOB_ID,
           SUBSCRIBER_ID,
           DL_PLAN_SK,
           DL_REF_CM_SCRIPT_SK,
           SCRIPT_ID,
           HEALTH_NOTE_TYPE_ID,
           PATIENT_FOLLOWUP_ID,
           DL_ASSESS_SK,
           CM_MEMBER_ID,
           CCM_MEMBER_ID,
           PATIENT_FORM_ID,
           SRC_KEY_DESC1,
           SRC_KEY1,
           SRC_KEY_DESC2,
           SRC_KEY2,
           ACTIVITY_DATE,
           --decode(row_number() over (partition by semi_annual_id, subscriber_id order by num desc, a.month_id desc),1,1,0) denum,
           --decode(num,1, decode(row_number() over (partition by semi_annual_id, subscriber_id order by num desc, a.month_id desc),1,1,0),0) num,
           num,
           DENUM,
           NOT_COMPLETED_AGING,
           LAST_UAS_DATE,
           LAST_PSP_DATE
      FROM v_adv_dir a;


GRANT SELECT ON CHOICEBI.V_CM_ADVANCED_DIRECTIVES_DATA TO CHOICEBI_RO_NEW;
