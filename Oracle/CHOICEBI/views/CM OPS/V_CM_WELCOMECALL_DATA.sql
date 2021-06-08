DROP VIEW CHOICEBI.V_CM_WELCOMECALL_DATA;

/* Formatted on 6/7/2021 8:20:46 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_WELCOMECALL_DATA
(
    DL_CM_MEASURE_SK,
    MONTH_ID,
    MEMBER_ID,
    DL_LOB_ID,
    SUBSCRIBER_ID,
    DL_PLAN_SK,
    CM_MEMBER_ID,
    CCM_MEMBER_ID,
    SCRIPT_ID,
    HEALTH_NOTE_TYPE_ID,
    PATIENT_FOLLOWUP_ID,
    DL_ASSESS_SK,
    PATIENT_FORM_ID,
    SRC_KEY_DESC1,
    SCR_KEY1,
    SCR_KEY_DESC2,
    SCR_KEY2,
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
        NEW_ENROLL
        AS
            (SELECT MONTH_ID,
                    SUBSCRIBER_ID,
                    MEMBER_ID,
                    DL_LOB_ID,
                    DL_PLAN_SK,
                    ENROLLMENT_DATE,
                    VPIN
               FROM CHOICEBI.FACT_MEMBER_MONTH
              WHERE     ENROLLED_FLAG = 1
                    AND dl_lob_id IN (2, 5)
                    AND PROGRAM IN ('MLTC')
                    AND ENROLLMENT_DATE >=
                        ADD_MONTHS (TRUNC (SYSDATE, 'month'), -12))
    SELECT '12'                      DL_CM_MEASURE_SK,
           DEN.MONTH_ID,
           DEN.MEMBER_ID,
           DEN.DL_LOB_ID,
           DEN.SUBSCRIBER_ID,
           DEN.DL_PLAN_SK,
           ''                        CM_MEMBER_ID,
           ''                        CCM_MEMBER_ID,
           SCR.SCRIPT_ID,
           ''                        HEALTH_NOTE_TYPE_ID,
           ''                        PATIENT_FOLLOWUP_ID,
           ''                        DL_ASSESS_SK,
           ''                        PATIENT_FORM_ID,
           ''                        SRC_KEY_DESC1,
           ''                        SCR_KEY1,
           'PATIENT_ID'              SCR_KEY_DESC2,
           SCR.PATIENT_ID            SCR_KEY2,
           SCR.ATTEMP_CREATED_ON     ACTIVITY_DATE,
           NUM,
           DENUM,
           NULL                      NOT_COMPLETED_AGING,
           NULL                      LAST_UAS_DATE,
           NULL                      LAST_PSP_DATE
      FROM NEW_ENROLL  DEN
           LEFT JOIN V_CM_OPS_SCRIPT_DATA SCR
               ON     SCR.SUBSCRIBER_ID = DEN.SUBSCRIBER_ID
                  AND DEN.MONTH_ID = SCR.MONTH_ID
                  AND SCR.SCRIPT_ID = 259
                  AND SCR.DENUM = 1;
