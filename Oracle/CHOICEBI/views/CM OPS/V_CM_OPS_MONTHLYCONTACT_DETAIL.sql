DROP VIEW CHOICEBI.V_CM_OPS_MONTHLYCONTACT_DETAIL;

/* Formatted on 6/7/2021 8:05:07 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_OPS_MONTHLYCONTACT_DETAIL
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
    LAST_PSP_DATE,
    ATTEMP_CREATED_ON,
    SUCCESSFUL_IND,
    CARE_NOTES,
    QUESTION_ID1,
    QUESTION1,
    QUESTION_RESPONSE_ID1,
    QUESTION_OPTION_ID1,
    OPTION_VALUE1,
    SUB_OPTION_ID1,
    SUB_OPTION_VALUE1,
    ATTEMPT_CREATED_ON1,
    CARE_NOTES1,
    QUESTION_ID2,
    QUESTION2,
    QUESTION_RESPONSE_ID2,
    QUESTION_OPTION_ID2,
    OPTION_VALUE2,
    SUB_OPTION_ID2,
    SUB_OPTION_VALUE2,
    ATTEMPT_CREATED_ON2,
    CARE_NOTES2,
    QUESTION_ID3,
    QUESTION3,
    QUESTION_RESPONSE_ID3,
    QUESTION_OPTION_ID3,
    OPTION_VALUE3,
    SUB_OPTION_ID3,
    SUB_OPTION_VALUE3,
    ATTEMPT_CREATED_ON3,
    CARE_NOTES3,
    TOTAL_ATTEMPT
)
BEQUEATH DEFINER
AS
    WITH
        v_cm_ope_data
        AS
            (SELECT 1                        DL_CM_MEASURE_SK,
                    TO_NUMBER (MONTH_ID)     MONTH_ID,
                    a.PATIENT_ID,
                    END_DATE                 CREATED_DATE,
                    NULL                     CREATED_BY,
                    DL_REF_CM_SCRIPT_SK,
                    'SCRIPT_RUN_LOG_ID'      SRC1,
                    a.SCRIPT_RUN_LOG_ID      SRC_ID1,
                    'SCRIPT_ID'              SRC2,
                    SCRIPT_ID                SRC_ID2,
                    NULL                     MEMBER_ID,
                    a.SUBSCRIBER_ID,
                    ATTEMP_CREATED_ON,
                    SUCCESSFUL_IND,
                    CARE_NOTES,
                    QUESTION_ID1,
                    QUESTION1,
                    QUESTION_RESPONSE_ID1,
                    QUESTION_OPTION_ID1,
                    OPTION_VALUE1,
                    SUB_OPTION_ID1,
                    SUB_OPTION_VALUE1,
                    ATTEMPT_CREATED_ON1,
                    CARE_NOTES1,
                    QUESTION_ID2,
                    QUESTION2,
                    QUESTION_RESPONSE_ID2,
                    QUESTION_OPTION_ID2,
                    OPTION_VALUE2,
                    SUB_OPTION_ID2,
                    SUB_OPTION_VALUE2,
                    ATTEMPT_CREATED_ON2,
                    CARE_NOTES2,
                    QUESTION_ID3,
                    QUESTION3,
                    QUESTION_RESPONSE_ID3,
                    QUESTION_OPTION_ID3,
                    OPTION_VALUE3,
                    SUB_OPTION_ID3,
                    SUB_OPTION_VALUE3,
                    ATTEMPT_CREATED_ON3,
                    CARE_NOTES3,
                    TOTAL_ATTEMPT
               FROM V_CM_OPS_SCRIPT_DATA A
              WHERE A.DL_REF_CM_SCRIPT_SK IN (107,                --HRA Script
                                              108,             --Master Script
                                              185,             --Master Script
                                              231,             --Master Script
                                              237,  --COVID - 19 CHOICE Script
                                              250,         --Master Final GCV8
                                              244, --Transition of Care Final GCV8
                                              245,   --Reassessment Final GCV8
                                              244,   --Welcome Call Final GCV8
                                              246,   --Welcome Call Final GCV8
                                              254 --Advanced Illness Assessment PPSv3
                                                 )),
        -- WHERE script_id IN (SELECT script_id
        --   FROM DIM_CM_MEASURES_SCRIPT_CFG
        --  WHERE script_freq = 'MONTHLY')--   AND denum = 1
        --                                ),
        --   v_cm_ope_data1
        --  AS
        --      (SELECT *
        --         FROM (SELECT a.*,
        --                      ROW_NUMBER () OVER (PARTITION BY patient_id, month_id
        --                                ORDER BY month_id ASC)    opre_seq
        --                 FROM v_cm_ope_data A)
        --        WHERE opre_seq = 1),
        dat
        AS
            (SELECT B.MONTH_ID,
                    B.MEMBER_ID,
                    B.DL_LOB_ID,
                    B.PROGRAM,
                    B.SUBSCRIBER_ID,
                    B.dl_plan_sk,
                    A.PATIENT_ID,
                    A.CREATED_DATE,
                    A.CREATED_BY,
                    A.SRC1                             SRC_KEY_DESC1,
                    A.SRC_ID1                          SRC_KEY1,
                    A.SRC2                             SRC_KEY_DESC2,
                    A.SRC_ID2                          SRC_KEY2,
                    A.DL_REF_CM_SCRIPT_SK,
                    -- OPRE_SEQ,
                    DECODE (A.SRC_ID1, NULL, 0, 1)     NUM,
                    1                                  DENUM,
                    c.cm_staff_id                      AS CM_MEMBER_ID,
                    b.first_name                       AS CM_First_Name,
                    b.last_name                        AS CM_Last_Name, --Co-ordinated Care Manager Info (CCM)
                    d.cm_staff_id                      AS CCM_MEMBER_ID,
                    d.first_name                       AS CCM_First_Name,
                    d.last_name                        AS CCM_Last_name,
                    ATTEMP_CREATED_ON,
                    SUCCESSFUL_IND,
                    CARE_NOTES,
                    QUESTION_ID1,
                    QUESTION1,
                    QUESTION_RESPONSE_ID1,
                    QUESTION_OPTION_ID1,
                    OPTION_VALUE1,
                    SUB_OPTION_ID1,
                    SUB_OPTION_VALUE1,
                    ATTEMPT_CREATED_ON1,
                    CARE_NOTES1,
                    QUESTION_ID2,
                    QUESTION2,
                    QUESTION_RESPONSE_ID2,
                    QUESTION_OPTION_ID2,
                    OPTION_VALUE2,
                    SUB_OPTION_ID2,
                    SUB_OPTION_VALUE2,
                    ATTEMPT_CREATED_ON2,
                    CARE_NOTES2,
                    QUESTION_ID3,
                    QUESTION3,
                    QUESTION_RESPONSE_ID3,
                    QUESTION_OPTION_ID3,
                    OPTION_VALUE3,
                    SUB_OPTION_ID3,
                    SUB_OPTION_VALUE3,
                    ATTEMPT_CREATED_ON3,
                    CARE_NOTES3,
                    TOTAL_ATTEMPT
               FROM FACT_MEMBER_MONTH  B
                    LEFT JOIN v_cm_ope_data A
                        ON (    A.SUBSCRIBER_ID = B.SUBSCRIBER_ID
                            AND A.MONTH_ID = B.MONTH_ID)
                    LEFT JOIN choice.dim_member_care_manager@DLAKE b1
                        ON (    B.cm_sk_id = b1.cm_sk_id
                            AND b1.DL_ACTIVE_REC_IND = 'Y')
                    LEFT JOIN choice.DIM_CM_STAFF_DETAILS@dlake c
                        ON (    c.dl_active_rec_ind = 'Y'
                            AND c.cm_staff_id = b1.care_manager_id)
                    LEFT JOIN choice.DIM_CM_STAFF_DETAILS@dlake d
                        ON (    c.dl_active_rec_ind = 'Y'
                            AND d.cm_staff_id = c.assigned_to
                            AND d.dl_active_rec_ind = 'Y')
              WHERE B.PROGRAM = 'MLTC')
    SELECT MONTH_ID,
           1
               DL_CM_MEASURE_SK,
           MEMBER_ID,
           DL_LOB_ID,
           SUBSCRIBER_ID,
           DL_PLAN_SK,
           DL_REF_CM_SCRIPT_SK,
           DECODE (SRC_KEY_DESC1,
                   'SCRIPT_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'SCRIPT_ID', SRC_KEY2))
               SCRIPT_ID,
           DECODE (SRC_KEY_DESC1,
                   'HEALT H_NOTE_TYPE_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'HEALTH_NOTE_TYPE_ID', SRC_KEY2))
               HEALTH_NOTE_TYPE_ID,
           DECODE (SRC_KEY_DESC1,
                   'PATIENT_FOLLOWUP_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'PATIENT_FOLLOWUP_ID', SRC_KEY2))
               PATIENT_FOLLOWUP_ID,
           DECODE (SRC_KEY_DESC1,
                   'RECORD_ID', SRC_KEY1,
                   DECODE (SRC_KEY_DESC2, 'RECORD_ID', SRC_KEY2))
               DL_ASSESS_SK,
           CM_MEMBER_ID,
           CCM_MEMBER_ID,
           NULL
               PATIENT_FORM_ID,
           SRC_KEY_DESC1,
           SRC_KEY1,
           SRC_KEY_DESC2,
           SRC_KEY2,
           CREATED_DATE
               ACTIVITY_DATE,
           NUM,
           DENUM,
           CASE
               WHEN a.num = 0
               THEN
                   CASE
                       WHEN DECODE (
                                LAG (num)
                                    OVER (PARTITION BY subscriber_id
                                          ORDER BY month_id),
                                0, 0,
                                NULL) =
                            0
                       THEN
                           CASE
                               WHEN DECODE (
                                        LAG (num, 2, '')
                                            OVER (PARTITION BY subscriber_id
                                                  ORDER BY month_id),
                                        0, 0,
                                        NULL) =
                                    0
                               THEN
                                   3
                               ELSE
                                   2
                           END
                       ELSE
                           1
                   END
               ELSE
                   0
           END
               NOT_COMPLETED_AGING,
           NULL
               LAST_UAS_DATE,
           NULL
               LAST_PSP_DATE,
           ATTEMP_CREATED_ON,
           SUCCESSFUL_IND,
           CARE_NOTES,
           QUESTION_ID1,
           QUESTION1,
           QUESTION_RESPONSE_ID1,
           QUESTION_OPTION_ID1,
           OPTION_VALUE1,
           SUB_OPTION_ID1,
           SUB_OPTION_VALUE1,
           ATTEMPT_CREATED_ON1,
           CARE_NOTES1,
           QUESTION_ID2,
           QUESTION2,
           QUESTION_RESPONSE_ID2,
           QUESTION_OPTION_ID2,
           OPTION_VALUE2,
           SUB_OPTION_ID2,
           SUB_OPTION_VALUE2,
           ATTEMPT_CREATED_ON2,
           CARE_NOTES2,
           QUESTION_ID3,
           QUESTION3,
           QUESTION_RESPONSE_ID3,
           QUESTION_OPTION_ID3,
           OPTION_VALUE3,
           SUB_OPTION_ID3,
           SUB_OPTION_VALUE3,
           ATTEMPT_CREATED_ON3,
           CARE_NOTES3,
           TOTAL_ATTEMPT
      FROM dat a
--  WHERE month_id >=
--        TO_CHAR (ADD_MONTHS (TRUNC (SYSDATE, 'month'), -12), 'YYYYMM');
;
