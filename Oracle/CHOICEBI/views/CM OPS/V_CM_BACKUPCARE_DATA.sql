DROP VIEW CHOICEBI.V_CM_BACKUPCARE_DATA;

/* Formatted on 6/7/2021 8:18:40 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_BACKUPCARE_DATA
(
    MONTH_ID,
    LAST_SA_MONTH_ID,
    MTH,
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
        bcp_data
        AS
            (SELECT *
               FROM (SELECT /*+ no_merge materialize */
                            lum.semi_annual_id,
                            TO_DATE (MONTH_ID, 'YYYYMM')       MONTH_ID_DT,
                            MONTH_ID,
                            A.PATIENT_ID,
                            B.UNIQUE_ID                        SUBSCRIBER_ID,
                            RECORD_DATE,
                            RECORD_ID,
                            ALERT_DISPLAY_STATUS,
                            A.COMMENTS,
                            VALUE_ENTERED,
                            NVL (
                                LEAD (RECORD_DATE)
                                    OVER (
                                        PARTITION BY A.PATIENT_ID,
                                                     PARAMETER_ID
                                        ORDER BY RECORD_DATE),
                                SYSDATE)                       VALID_UPTO_DT,
                            ROW_NUMBER ()
                                OVER (
                                    PARTITION BY UNIQUE_ID,
                                                 LUM.SEMI_ANNUAL_ID
                                    ORDER BY MONTH_ID DESC)    SEQ
                       FROM CMGC.HEALTH_INDICATOR_RECORD  A
                            JOIN CMGC.PATIENT_DETAILS B
                                ON (A.PATIENT_ID = B.PATIENT_ID)
                            JOIN MSTRSTG.LU_MONTH LUM
                                ON (MONTH_ID =
                                    TO_CHAR (RECORD_DATE, 'YYYYMM'))
                      WHERE PARAMETER_ID = 18     --AND VALUE_ENTERED = 'True'
                                             )
              WHERE SEQ = 1),
        bcp_care_data1
        AS
            (SELECT A.MONTH_ID,
                    B.MONTH_ID                mth,
                    4                         DL_CM_MEASURE_SK,
                    MEMBER_ID,
                    DL_LOB_ID,
                    A.SUBSCRIBER_ID,
                    DL_PLAN_SK,
                    NULL                      DL_REF_CM_SCRIPT_SK,
                    NULL                      SCRIPT_ID,
                    NULL                      HEALTH_NOTE_TYPE_ID,
                    NULL                      PATIENT_FOLLOWUP_ID,
                    NULL                      DL_ASSESS_SK,
                    NULL                      CM_MEMBER_ID,
                    NULL                      CCM_MEMBER_ID,
                    NULL                      PATIENT_FORM_ID,
                    'HEALTH_IND_RECORD_ID'    SRC_KEY_DESC1,
                    RECORD_ID                 SRC_KEY1,
                    NULL                      SRC_KEY_DESC2,
                    NULL                      SRC_KEY2,
                    RECORD_DATE               ACTIVITY_DATE,
                    CASE
                        WHEN UPPER (VALUE_ENTERED) IN ('TRUE', 'YES') THEN 1
                        ELSE 0
                    END                       NUM,
                    1                         DENUM,
                    NULL                      NOT_COMPLETED_AGING,
                    NULL                      LAST_UAS_DATE,
                    NULL                      LAST_PSP_DATE,
                    LAST_SA_MONTH_ID
               FROM V_DIM_SEMI_ANNUAL_MEMBER_CENSUS  A
                    LEFT JOIN bcp_data B
                        ON (    B.SUBSCRIBER_ID = A.SUBSCRIBER_ID
                            AND b.month_id <= A.month_id
                            AND b.semi_annual_id = a.semi_annual_id --AND b.month_id between A.month_id AND A.last_SA_month_id
                                                                   ) --where a.DISENROLLED_FLAG=0
                                                                    )
    SELECT MONTH_ID,
           last_SA_month_id,
           mth,
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
      FROM bcp_care_data1;


GRANT SELECT ON CHOICEBI.V_CM_BACKUPCARE_DATA TO CHOICEBI_RO_NEW;
