DROP VIEW CHOICEBI.V_CM_MD_COLLABRATOR;

/* Formatted on 6/7/2021 8:19:07 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_MD_COLLABRATOR
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
        active_census
        AS
            (SELECT MONTH_ID,
                    SUBSCRIBER_ID,
                    MEMBER_ID,
                    DL_LOB_ID,
                    DL_PLAN_SK
               FROM (SELECT /*+ materialize no_merge */
                            MONTH_ID,
                            SUBSCRIBER_ID,
                            MEMBER_ID,
                            DL_LOB_ID,
                            DL_PLAN_SK,
                            MAX (disenrolled_flag)
                                OVER (
                                    PARTITION BY a.member_id, a.subscriber_id
                                    ORDER BY a.month_id DESC)    max_disenroll_flag
                       FROM FACT_MEMBER_MONTH a
                      WHERE     DL_LOB_ID IN (2, 5)
                            AND program = 'MLTC'
                            AND   EXTRACT (YEAR FROM SYSDATE)
                                - EXTRACT (
                                      YEAR FROM TO_DATE (a.MONTH_ID,
                                                         'YYYYMM')) <
                                3)
              WHERE max_disenroll_flag <> 1),
        MD_C
        AS
            (SELECT *
               FROM (SELECT TO_DATE (TO_CHAR (A.created_on, 'YYYYMM'),
                                     'YYYYMM')
                                MONTH_ID_DT,
                            TO_CHAR (A.created_on, 'YYYYMM')
                                MONTH_ID,
                            a.care_giver_id,
                            a.patient_id,
                            B.UNIQUE_ID
                                SUBSCRIBER_ID,
                            TRUNC (A.created_on),
                            A.FIRST_NAME,
                            A.LAST_NAME,
                            A.created_on,
                            A.UPDATED_ON,
                            RANK ()
                                OVER (
                                    PARTITION BY a.patient_id
                                    ORDER BY
                                        A.CREATED_ON DESC, A.UPDATED_ON DESC)
                                RK,
                            NVL (a.first_name, -1),
                            NVL (a.last_name, -1)
                       FROM CMGC.PATIENT_PRIMARY_CAREGIVER  A
                            JOIN CMGC.PATIENT_DETAILS B
                                ON (A.PATIENT_ID = B.PATIENT_ID)
                      WHERE PATIENT_RELATION_ID = 15 AND IS_PRIMARY <> 1)
              WHERE rk = 1)
    SELECT m.MONTH_ID,
           6                  DL_CM_MEASURE_SK,
           MEMBER_ID,
           DL_LOB_ID,
           M.SUBSCRIBER_ID,
           DL_PLAN_SK,
           NULL               DL_REF_CM_SCRIPT_SK,
           NULL               SCRIPT_ID,
           NULL               HEALTH_NOTE_TYPE_ID,
           NULL               PATIENT_FOLLOWUP_ID,
           NULL               DL_ASSESS_SK,
           NULL               CM_MEMBER_ID,
           NULL               CCM_MEMBER_ID,
           NULL               PATIENT_FORM_ID,
           'CARE_GIVER_ID'    SRC_KEY_DESC1,
           c.care_giver_id    SRC_KEY1,
           'PATIENT_ID'       SRC_KEY_DESC2,
           c.patient_id       SRC_KEY2,
           c.created_on       ACTIVITY_DATE,
           CASE
               WHEN c.first_name IS NULL OR c.last_name IS NULL THEN 0
               ELSE 1
           END                NUM,
           1                  DENUM,
           NULL               NOT_COMPLETED_AGING,
           NULL               LAST_UAS_DATE,
           NULL               LAST_PSP_DATE
      FROM MD_C  C
           RIGHT JOIN active_census M ON M.subscriber_id = c.subscriber_id
-- and c.month_id=m.month_id
;


GRANT SELECT ON CHOICEBI.V_CM_MD_COLLABRATOR TO CHOICEBI_RO_NEW;
