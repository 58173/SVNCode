DROP VIEW V_GC_AUTH_AND_DECISION;

CREATE OR REPLACE VIEW V_GC_AUTH_AND_DECISION
(
    AUTH_NO,
    AUTH_ID,
    AUTH_NOTI_DATE,
    AUTH_CREATED_ON,
    DECISION_NO,
    DECISION_ID,
    DECISION_NOTI_DATE,
    AUTH_PRIORITY_ID,
    IS_EXTENSION,
    DECISION_CREATED_ON
) AS
    SELECT A.AUTH_NO,
           A.AUTH_ID,
           a.Auth_Noti_Date,
           a.created_on AS AUTH_CREATED_ON,
           D.DECISION_NO,
           D.DECISION_ID,
           d.auth_noti_Date Decision_Noti_Date,
           a.AUTH_PRIORITY_ID,
           a.IS_EXTENSION,
           D.CREATED_ON AS DECISION_CREATED_ON
    FROM   CMGC.UM_AUTH A
           LEFT JOIN CMGC.UM_DECISION D ON A.AUTH_NO = D.AUTH_NO
    WHERE  d.deleted_on IS NULL


GRANT SELECT ON V_GC_AUTH_AND_DECISION TO MSTRSTG