DROP VIEW CHOICEBI.V_CM_PCPS_ENROLL_DATA;

/* Formatted on 6/7/2021 8:20:26 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_PCPS_ENROLL_DATA
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
    DENUM
)
BEQUEATH DEFINER
AS
    SELECT TO_NUMBER (TO_CHAR (enrollment_date, 'YYYYMM'))
               MONTH_ID,
           --TO_NUMBER(TO_CHAR(PCSP_CREATED_ON,'YYYYMM'))  MONTH_ID,
           2
               DL_CM_MEASURE_SK,
           MEMBER_ID,
           DL_LOB_ID,
           SUBSCRIBER_ID,
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
           PATIENT_FORM_ID,
           'PATIENT_FORM_ID'
               SRC_KEY_DESC1,
           PATIENT_FORM_ID
               SRC_KEY1,
           NULL
               SRC_KEY_DESC2,
           NULL
               SRC_KEY2,
           PCSP_CREATED_ON
               ACTIVITY_DATE,
           DECODE (pcsp_created_on, NULL, 0, 1)
               NUM,
           1
               DENUM
      FROM V_PCSP_ENROLL_NEW
    UNION ALL
    SELECT TO_NUMBER (TO_CHAR (ASSESSMENTDATE, 'YYYYMM'))
               MONTH_ID,
           --TO_NUMBER(TO_CHAR(PCSP_CREATED_ON,'YYYYMM'))  MONTH_ID,
           3
               DL_CM_MEASURE_SK,
           MEMBER_ID,
           DL_LOB_ID,
           SUBSCRIBER_ID,
           DL_PLAN_SK,
           NULL
               DL_REF_CM_SCRIPT_SK,
           NULL
               SCRIPT_ID,
           NULL
               HEALTH_NOTE_TYPE_ID,
           NULL
               PATIENT_FOLLOWUP_ID,
           DL_ASSESS_SK,
           NULL
               CM_MEMBER_ID,
           NULL
               CCM_MEMBER_ID,
           PATIENT_FORM_ID,
           'PATIENT_FORM_ID'
               SRC_KEY_DESC1,
           PATIENT_FORM_ID
               SRC_KEY1,
           NULL
               SRC_KEY_DESC2,
           NULL
               SRC_KEY2,
           PCSP_CREATED_ON
               ACTIVITY_DATE,
           DECODE (pcsp_created_on, NULL, 0, 1)
               NUM,
           1
               DENUM
      FROM V_PCSP_REASSESS_NEW;


GRANT SELECT ON CHOICEBI.V_CM_PCPS_ENROLL_DATA TO CHOICEBI_RO_NEW;
