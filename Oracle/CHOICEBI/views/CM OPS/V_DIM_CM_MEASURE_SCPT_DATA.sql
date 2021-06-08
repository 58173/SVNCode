DROP VIEW CHOICEBI.V_DIM_CM_MEASURE_SCPT_DATA;

/* Formatted on 6/7/2021 8:24:36 PM (QP5 v5.336) */
CREATE OR REPLACE FORCE VIEW CHOICEBI.V_DIM_CM_MEASURE_SCPT_DATA
(
    PATIENT_ID,
    SUBSCRIBER_ID,
    SCRIPT_RUN_LOG_ID,
    SCRIPT_RUN_LOG_DETAIL_ID,
    CURR_HRA,
    END_DATE,
    MONTH_ID,
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
    RNK
)
BEQUEATH DEFINER
AS
    WITH
        CM_ADMIN_SCRIPT
        AS
            (SELECT /*+ no_merge materialize */
                    A.SCRIPT_ID,
                    SCRIPT_NAME,
                    B.QUESTION_ID,
                    QUESTION
               --QUESTION_OPTION_ID,
               --QUESTION_OPTION
               FROM                      --CMGC.SCPT_ADMIN_QUESTION_OPTION  C,
                    CMGC.SCPT_ADMIN_QUESTION B, CMGC.SCPT_ADMIN_SCRIPT A
              WHERE     A.SCRIPT_ID = B.SCRIPT_ID
                    --AND B.QUESTION_ID = C.QUESTION_ID
                    AND B.QUESTION_ID IN
                            (SELECT QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG
                             UNION ALL
                             SELECT ATTEMPT1_QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG
                             UNION ALL
                             SELECT ATTEMPT2_QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG
                             UNION ALL
                             SELECT ATTEMPT3_QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG)),
        data
        AS
            (SELECT /*+ use_hash(a q a1 resOption dim1)*/
                    A.PATIENT_ID,
                    A.SUBSCRIBER_ID,
                    A1.SCRIPT_RUN_LOG_ID,
                    A1.SCRIPT_RUN_LOG_DETAIL_ID,
                    CAST (A.END_DATE AS DATE)          CURR_HRA,
                    --A.START_DATE,
                    A.END_DATE,
                    TO_CHAR (A.END_DATE, 'YYYYMM')     AS MONTH_ID,
                    A1.SCRIPT_ID,
                    A2.DL_REF_CM_SCRIPT_SK,
                    DIM1.SCRIPT_NAME,
                    A1.QUESTION_ID,
                    DIM1.QUESTION,
                    Q.QUESTION_RESPONSE_ID,
                    Q.QUESTION_OPTION_ID,
                    Q.OPTION_VALUE,
                    Q.SUB_OPTION_ID,
                    Q.SUB_OPTION_VALUE,
                    A1.CREATED_ON                      ATTEMP_CREATED_ON,
                    CASE
                        WHEN Q.QUESTION_OPTION_ID IN
                                 (SELECT SUCCESS_OPTION_ID
                                    FROM DIM_CM_MEASURES_SCRIPT_CFG)
                        THEN
                            1
                        ELSE
                            0
                    END                                AS SUCCESSFUL_IND,
                    A1.CARE_NOTES,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY TO_CHAR (A.END_DATE, 'YYYYMM'),
                                         A.subscriber_id,
                                         A1.SCRIPT_RUN_LOG_ID
                            ORDER BY
                                CASE
                                    WHEN Q.QUESTION_OPTION_ID IN
                                             (SELECT SUCCESS_OPTION_ID
                                                FROM DIM_CM_MEASURES_SCRIPT_CFG)
                                    THEN
                                        1
                                    ELSE
                                        0
                                END DESC,
                                A1.CREATED_ON DESC)    rnk
               FROM CHOICE.FCT_CM_SCRIPT_MEM_RUN_LOG@DLAKE  A
                    JOIN CHOICE.FCT_CM_MEMBER_FOLLOWUP@dlake a2
                        ON (a2.DL_FCT_CM_MEMBER_FOLLOWUP_SK =
                            a.DL_FCT_CM_MEMBER_FOLLOWUP_SK)
                    JOIN CMGC.SCPT_PAT_SCRIPT_RUN_LOG_DET A1
                        ON (A.SCRIPT_RUN_LOG_ID = A1.SCRIPT_RUN_LOG_ID)
                    LEFT JOIN CMGC.SCPT_QUESTION_RESPONSE Q
                        ON (    Q.SCRIPT_RUN_LOG_DETAIL_ID =
                                A1.SCRIPT_RUN_LOG_DETAIL_ID
                            AND IS_ACTIVE = 1)
                    --LEFT JOIN CM_ADMIN_SCRIPT RESOPTION ON (Q.QUESTION_OPTION_ID = RESOPTION.QUESTION_OPTION_ID)
                    JOIN CM_ADMIN_SCRIPT DIM1
                        ON (    DIM1.SCRIPT_ID = A1.SCRIPT_ID
                            AND DIM1.QUESTION_ID = A1.QUESTION_ID)
              WHERE     1 = 1
                    AND Q.DELETED_BY IS NULL
                    AND Q.DELETED_ON IS NULL
                    AND A.STATUS_ID = 1
                    AND A1.SCRIPT_ID IN
                            (SELECT SCRIPT_ID FROM DIM_CM_MEASURES_SCRIPT_CFG)
                    AND DIM1.QUESTION_ID IN
                            (SELECT QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG
                             UNION ALL
                             SELECT ATTEMPT1_QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG
                             UNION ALL
                             SELECT ATTEMPT2_QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG
                             UNION ALL
                             SELECT ATTEMPT3_QUESTION_ID
                               FROM DIM_CM_MEASURES_SCRIPT_CFG))
      SELECT "PATIENT_ID",
             "SUBSCRIBER_ID",
             "SCRIPT_RUN_LOG_ID",
             "SCRIPT_RUN_LOG_DETAIL_ID",
             "CURR_HRA",
             "END_DATE",
             "MONTH_ID",
             "SCRIPT_ID",
             "DL_REF_CM_SCRIPT_SK",
             "SCRIPT_NAME",
             "QUESTION_ID",
             "QUESTION",
             "QUESTION_RESPONSE_ID",
             "QUESTION_OPTION_ID",
             "OPTION_VALUE",
             "SUB_OPTION_ID",
             "SUB_OPTION_VALUE",
             "ATTEMP_CREATED_ON",
             "SUCCESSFUL_IND",
             "CARE_NOTES",
             "RNK"
        FROM data                                                --where rnk=1
    ORDER BY SUBSCRIBER_ID, SCRIPT_RUN_LOG_ID, SCRIPT_RUN_LOG_DETAIL_ID;
