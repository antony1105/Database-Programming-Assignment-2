DECLARE
BEGIN
  pkg_fss_settlement.upd_fss_daily_transaction;
END;
/

CREATE OR REPLACE PACKAGE pkg_fss_settlement
AS
  PROCEDURE upd_fss_daily_transaction;
  FUNCTION get_last_transaction_nr 
  RETURN NUMBER;
END pkg_fss_settlement;
/

CREATE OR REPLACE PACKAGE BODY pkg_fss_settlement
IS
  g_last_transaction_nr NUMBER := get_last_transaction_nr;
 
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
    ELSE
      RETURN 1;
    END IF;
  EXCEPTION
    WHEN OTHERS 
    THEN
      upd_error_table(SQLERRM, 'get_last_transaction_nr');
  END get_last_transaction_nr;
  
  PROCEDURE upd_fss_daily_transaction
  IS
    l_err_code VARCHAR2(50);
    l_err_msg varchar2(50);
    l_row_count NUMBER;
  BEGIN
    SELECT COUNT(transactionNr)
    INTO l_row_count
    FROM fss_daily_transactions;
    
    IF l_row_count > 0
    THEN      
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
        WHERE transactionNr >
          (
            SELECT MAX(transactionNr)
            FROM fss_daily_transactions
          );
    ELSE
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
        FROM fss_transactions;
    END IF;
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      upd_error_table(SQLERRM, 'upd_fss_daily_transaction');
  END upd_fss_daily_transaction;
END pkg_fss_settlement;