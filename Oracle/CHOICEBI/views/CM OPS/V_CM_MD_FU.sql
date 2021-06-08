DROP VIEW CHOICEBI.V_CM_MD_FU;

/* Formatted on 6/7/2021 8:19:26 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_MD_FU
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
        MD_FU
        AS
            (SELECT A.APPOINTMENT_ID                             MIN_APP_ID,
                    B.PATIENT_ID --TRUNC(A.APPOINTMENT_DATE) MIN_APP_DATE      --   175513   175864
                                ,
                    TRUNC (B.APPOINTMENT_DATE)                   MAX_APP_DATE,
                    b.appointment_id,
                      EXTRACT (YEAR FROM A.APPOINTMENT_DATE) * 100
                    + EXTRACT (MONTH FROM A.APPOINTMENT_DATE)    MIN_APP_DATE
               FROM (SELECT *
                       FROM (SELECT APPOINTMENT_ID,
                                    APPOINTMENT_DATE,
                                    PATIENT_ID,
                                    RANK ()
                                        OVER (
                                            PARTITION BY EXTRACT (
                                                             YEAR FROM APPOINTMENT_DATE),
                                                         PATIENT_ID
                                            ORDER BY APPOINTMENT_DATE)    RK_MIN
                               FROM CMGC.APPOINTMENT
                              WHERE APPOINTMENT_STATUS = 4)
                      WHERE RK_MIN = 1) A
                    JOIN
                    (SELECT *
                       FROM (SELECT APPOINTMENT_ID,
                                    APPOINTMENT_DATE,
                                    PATIENT_ID,
                                    RANK ()
                                        OVER (
                                            PARTITION BY EXTRACT (
                                                             YEAR FROM APPOINTMENT_DATE),
                                                         PATIENT_ID
                                            ORDER BY APPOINTMENT_DATE DESC)    RK_MAX
                               FROM CMGC.APPOINTMENT
                              WHERE APPOINTMENT_STATUS = 4)
                      WHERE RK_MAX = 1) B
                        ON     A.PATIENT_ID = B.PATIENT_ID
                           AND EXTRACT (YEAR FROM A.APPOINTMENT_DATE) =
                               EXTRACT (YEAR FROM B.APPOINTMENT_DATE))
    SELECT m.MONTH_ID,
           7
               DL_CM_MEASURE_SK,
           MEMBER_ID,
           DL_LOB_ID,
           M.SUBSCRIBER_ID,
           DL_PLAN_SK,
           NULL
               DL_REF_CM_SCRIPT_SK,
           NULL
               SCRIPT_ID,
           NULL
               HEALTH_NOTE_TYPE_ID,
           NULL
               PATIENT_FOLLOWUP_ID,
           NULL
               DL_ASSESS_SK,
           NULL
               CM_MEMBER_ID,
           NULL
               CCM_MEMBER_ID,
           NULL
               PATIENT_FORM_ID,
           'APPOINTMENT_ID'
               SRC_KEY_DESC1,
           n.APPOINTMENT_ID
               SRC_KEY1,
           'PATIENT_ID'
               SRC_KEY_DESC2,
           n.patient_id
               SRC_KEY2,
           n.MAX_APP_DATE
               ACTIVITY_DATE,
           CASE WHEN MIN_APP_DATE <= MONTH_ID THEN 1 ELSE 0 END
               NUM,
           1
               DENUM,
           NULL
               NOT_COMPLETED_AGING,
           NULL
               LAST_UAS_DATE,
           NULL
               LAST_PSP_DATE
      FROM MD_FU  n
           JOIN CMGC.PATIENT_DETAILS B ON (n.PATIENT_ID = B.PATIENT_ID)
           RIGHT JOIN active_census M
               ON     M.subscriber_id = b.unique_id
                  AND EXTRACT (YEAR FROM TO_DATE (M.MONTH_ID, 'YYYYMM')) =
                      EXTRACT (YEAR FROM n.Max_APP_DATE)
-- and c.month_id=m.month_id
;


GRANT SELECT ON CHOICEBI.V_CM_MD_FU TO CHOICEBI_RO_NEW;
