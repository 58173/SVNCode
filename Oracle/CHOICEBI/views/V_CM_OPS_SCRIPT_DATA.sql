DROP VIEW CHOICEBI.V_CM_OPS_SCRIPT_DATA;

/* Formatted on 6/7/2021 8:05:46 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_CM_OPS_SCRIPT_DATA
(
    MONTH_ID,
    SUBSCRIBER_ID,
    DENUM,
    NUM,
    TOTAL_CONTACT_MADE,
    PATIENT_ID,
    SCRIPT_RUN_LOG_ID,
    SCRIPT_RUN_LOG_DETAIL_ID,
    END_DATE,
    SCRIPT_ID,
    DL_REF_CM_SCRIPT_SK,
    SCRIPT_NAME,
    QUESTION_ID,
    QUESTION,
    QUESTION_RESPONSE_ID,
    QUESTION_OPTION_ID,
    OPTION_VALUE,
    SUB_OPTION_ID,
    SUB_OPTION_VALUE,
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
    TOTAL_ATTEMPT,
    RNK
)
BEQUEATH DEFINER
AS
      SELECT SUCC.MONTH_ID,
             SUCC.SUBSCRIBER_ID,
             DECODE (
                 ROW_NUMBER ()
                     OVER (
                         PARTITION BY SUCC.MONTH_ID, SUCC.SUBSCRIBER_ID
                         ORDER BY
                             SUCC.SUCCESSFUL_IND DESC,
                             SUCC.SCRIPT_RUN_LOG_ID DESC),
                 1, 1,
                 0)
                 DENUM,
             MAX (SUCC.SUCCESSFUL_IND)
                 OVER (PARTITION BY SUCC.MONTH_ID, SUCC.SUBSCRIBER_ID
                       ORDER BY SUCC.SCRIPT_RUN_LOG_ID DESC)
                 NUM,
             COUNT (1) OVER (PARTITION BY SUCC.MONTH_ID, SUCC.SUBSCRIBER_ID)
                 TOTAL_CONTACT_MADE,
             SUCC.PATIENT_ID,
             SUCC.SCRIPT_RUN_LOG_ID,
             SUCC.SCRIPT_RUN_LOG_DETAIL_ID,
             SUCC.END_DATE,
             SUCC.SCRIPT_ID,
             SUCC.DL_REF_CM_SCRIPT_SK,
             SUCC.SCRIPT_NAME,
             SUCC.QUESTION_ID,
             SUCC.QUESTION,
             SUCC.QUESTION_RESPONSE_ID,
             SUCC.QUESTION_OPTION_ID,
             SUCC.OPTION_VALUE,
             SUCC.SUB_OPTION_ID,
             SUCC.SUB_OPTION_VALUE,
             SUCC.ATTEMP_CREATED_ON,
             SUCC.SUCCESSFUL_IND,
             SUCC.CARE_NOTES,
             ATT1.QUESTION_ID
                 QUESTION_ID1,
             ATT1.QUESTION
                 QUESTION1,
             ATT1.QUESTION_RESPONSE_ID
                 QUESTION_RESPONSE_ID1,
             ATT1.QUESTION_OPTION_ID
                 QUESTION_OPTION_ID1,
             ATT1.OPTION_VALUE
                 OPTION_VALUE1,
             ATT1.SUB_OPTION_ID
                 SUB_OPTION_ID1,
             ATT1.SUB_OPTION_VALUE
                 SUB_OPTION_VALUE1,
             ATT1.ATTEMP_CREATED_ON
                 ATTEMPT_CREATED_ON1,
             ATT1.CARE_NOTES
                 CARE_NOTES1,
             ATT2.QUESTION_ID
                 QUESTION_ID2,
             ATT2.QUESTION
                 QUESTION2,
             ATT2.QUESTION_RESPONSE_ID
                 QUESTION_RESPONSE_ID2,
             ATT2.QUESTION_OPTION_ID
                 QUESTION_OPTION_ID2,
             ATT2.OPTION_VALUE
                 OPTION_VALUE2,
             ATT2.SUB_OPTION_ID
                 SUB_OPTION_ID2,
             ATT2.SUB_OPTION_VALUE
                 SUB_OPTION_VALUE2,
             ATT2.ATTEMP_CREATED_ON
                 ATTEMPT_CREATED_ON2,
             ATT2.CARE_NOTES
                 CARE_NOTES2,
             ATT3.QUESTION_ID
                 QUESTION_ID3,
             ATT3.QUESTION
                 QUESTION3,
             ATT3.QUESTION_RESPONSE_ID
                 QUESTION_RESPONSE_ID3,
             ATT3.QUESTION_OPTION_ID
                 QUESTION_OPTION_ID3,
             ATT3.OPTION_VALUE
                 OPTION_VALUE3,
             ATT3.SUB_OPTION_ID
                 SUB_OPTION_ID3,
             ATT3.SUB_OPTION_VALUE
                 SUB_OPTION_VALUE3,
             ATT3.ATTEMP_CREATED_ON
                 ATTEMPT_CREATED_ON3,
             ATT3.CARE_NOTES
                 CARE_NOTES3,
               NVL2 (ATT1.QUESTION_RESPONSE_ID, 1, 0)
             + NVL2 (ATT3.QUESTION_RESPONSE_ID, 1, 0)
             + NVL2 (ATT3.QUESTION_RESPONSE_ID, 1, 0)
                 TOTAL_ATTEMPT,
             SUCC.RNK
        FROM (SELECT A.*
                FROM MV_DIM_CM_MEASURE_SCPT_DATA A
                     JOIN DIM_CM_MEASURES_SCRIPT_CFG B
                         ON A.QUESTION_ID = B.QUESTION_ID) SUCC
             LEFT JOIN
             (SELECT A.*
                FROM MV_DIM_CM_MEASURE_SCPT_DATA A
                     JOIN DIM_CM_MEASURES_SCRIPT_CFG B
                         ON A.QUESTION_ID = B.ATTEMPT1_QUESTION_ID) ATT1
                 ON (SUCC.SCRIPT_RUN_LOG_ID = ATT1.SCRIPT_RUN_LOG_ID)
             LEFT JOIN
             (SELECT A.*
                FROM MV_DIM_CM_MEASURE_SCPT_DATA A
                     JOIN DIM_CM_MEASURES_SCRIPT_CFG B
                         ON A.QUESTION_ID = B.ATTEMPT2_QUESTION_ID) ATT2
                 ON (SUCC.SCRIPT_RUN_LOG_ID = ATT2.SCRIPT_RUN_LOG_ID)
             LEFT JOIN
             (SELECT A.*
                FROM MV_DIM_CM_MEASURE_SCPT_DATA A
                     JOIN DIM_CM_MEASURES_SCRIPT_CFG B
                         ON A.QUESTION_ID = B.ATTEMPT3_QUESTION_ID) ATT3
                 ON (SUCC.SCRIPT_RUN_LOG_ID = ATT3.SCRIPT_RUN_LOG_ID)
       WHERE succ.rnk = 1
    ORDER BY MONTH_ID, SUBSCRIBER_ID;
