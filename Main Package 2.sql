DECLARE
BEGIN
  pkg_fss_settlement.upd_fss_daily_transaction;
END;
/

DECLARE
BEGIN
  pkg_fss_settlement.ins_daily_settlement;
END;
/

DECLARE
BEGIN
  pkg_fss_settlement.DailySettlement;
END;
/

CREATE OR REPLACE PACKAGE pkg_fss_settlement
AS
  PROCEDURE upd_fss_daily_transaction;
  PROCEDURE ins_daily_settlement;
  PROCEDURE DailySettlement;
END pkg_fss_settlement;
/

CREATE OR REPLACE PACKAGE BODY pkg_fss_settlement
IS
  g_last_transaction_nr NUMBER := 1;
  
  PROCEDURE upd_daily_transaction_settled
  IS
  BEGIN
    UPDATE fss_daily_transactions
    SET settlementStatus = 'Done'
    WHERE transactionNr <= g_last_transaction_nr;
    COMMIT;
  END upd_daily_transaction_settled;
  
  FUNCTION get_format_bsb(p_value VARCHAR2)
  RETURN VARCHAR2
  IS
  BEGIN
    RETURN substr(p_value, 1,3) || '-' || substr(p_value, 4,6);
  END get_format_bsb;
 
  PROCEDURE upd_error_table(p_err_msg VARCHAR2, p_location VARCHAR2) 
  IS
  BEGIN
    INSERT INTO error_table
    VALUES
      (
        p_err_msg
        , systimestamp
        , p_location
      );
    COMMIT; 
  END upd_error_table; 
  
  FUNCTION get_last_transaction_nr
  RETURN NUMBER IS
    l_last_transaction_nr fss_daily_transactions.transactionNr%TYPE;
  BEGIN
    SELECT MAX(transactionNr)
    INTO l_last_transaction_nr
    FROM fss_daily_transactions;    
    IF l_last_transaction_nr IS NOT NULL
    THEN 
      RETURN l_last_transaction_nr;
--    ELSE
--      RETURN 1;
    END IF;
  EXCEPTION
    WHEN OTHERS 
    THEN
      upd_error_table(SQLERRM, 'get_last_transaction_nr');
  END get_last_transaction_nr;
  
  PROCEDURE upd_fss_daily_transaction
  IS
  BEGIN  
    INSERT INTO fss_daily_transactions
      SELECT transactionNr
      , downloadDate
      , terminalId
      , cardId
      , transactionDate
      , cardOldValue
      , transactionAmount
      , cardNewValue
      , transactionStatus
      , errorCode
      , null
      FROM fss_transactions
      WHERE transactionNr > g_last_transaction_nr;
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      upd_error_table(SQLERRM, 'upd_fss_daily_transaction');
  END upd_fss_daily_transaction;
  
  PROCEDURE ins_daily_settlement
  IS
  BEGIN
    INSERT INTO fss_daily_settlement
      (
        merchantBsb
        , merchantAccNum
        , transaction
        , merchantTitle
        , trace
      )
      SELECT bsbm.merchantBsb
        , m.merchantBankAccNr
        , sum(dt.transactionAmount) AS value
        , m.merchantAccountTitle
        , (
            SELECT substr(orgBsbNr, 1,3) || '-' || substr(orgBsbNr, 4,6) || '   ' || orgBankAccount
            FROM fss_organisation
          ) AS trace
      FROM fss_daily_transactions dt
      INNER JOIN fss_smartcard s
      ON dt.cardId = s.cardId
      INNER JOIN fss_terminal te
      ON dt.terminalId = te.terminalId
      INNER JOIN fss_terminal_type tet
      ON te.terminalType = tet.typeName
      INNER JOIN fss_merchant m
      ON te.merchantId = m.merchantId
      INNER JOIN
      (
        SELECT merchantBankBsb
          , substr(merchantBankBsb, 1,3) || '-' || substr(merchantBankBsb, 4,6) AS merchantBsb
        FROM fss_merchant
      ) bsbm
      ON bsbm.merchantBankBsb =  m.merchantBankBsb
      WHERE dt.settlementStatus IS NULL
      GROUP BY bsbm.merchantBsb
        , m.merchantBankAccNr
        , m.merchantAccountTitle;
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      upd_error_table(SQLERRM, 'ins_daily_settlement');
  END ins_daily_settlement;
  
  PROCEDURE DailySettlement
  IS
  BEGIN
    dbms_output.put_line(g_last_transaction_nr);
    upd_fss_daily_transaction;
    ins_daily_settlement;
    g_last_transaction_nr := get_last_transaction_nr;
    dbms_output.put_line(g_last_transaction_nr);
    upd_daily_transaction_settled;
  EXCEPTION
    WHEN OTHERS 
    THEN
      upd_error_table(SQLERRM, 'DailySettlement');
  END DailySettlement;
BEGIN
  EXECUTE IMMEDIATE 'TRUNCATE fss_daily_transactions';
  EXECUTE IMMEDIATE 'TRUNCATE fss_daily_settlement';
END pkg_fss_settlement;