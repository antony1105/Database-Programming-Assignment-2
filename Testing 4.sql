select * from fss_merchant;
select * from fss_terminal
order by merchantId, terminalType;
select * from fss_terminal_type;
select * from fss_daily_transactions
--where settlementStatus = 'Null'
--where terminalId = 0051001500;
order by transactionNr desc;
select * from fss_reference;
select * from fss_transactions
order by transactionNr;
select * from fss_smartcard;
select * from fss_organisation;
select * from fss_daily_settlement;

select sum(transaction)
from fss_daily_settlement
where bankingFLag = 'F';

begin
  execute immediate('truncate table fss_daily_transactions');
  execute immediate('truncate table fss_daily_settlement');
  COMMIT;
end;
/

select distinct terminalId from fss_daily_transactions
where settlementStatus <> 'Null'
order by terminalId;

SELECT terminalId
FROM fss_daily_transactions
--WHERE settlementStatus = 'Null'
HAVING minTransaction < SUM(transactionAmount)
GROUP BY terminalID, minTransaction
ORDER BY terminalId;
-- allow all terminal ID to be settled

UPDATE fss_daily_transactions
SET settlementStatus = 'Settled'
WHERE transactionNr < 87400
AND terminalId IN 
  (
    SELECT terminalId
    FROM fss_daily_transactions
    WHERE settlementStatus IS NULL
    HAVING minTransaction < SUM(transactionAmount)
    GROUP BY terminalId, minTransaction
  );

INSERT INTO fss_daily_transactions
  SELECT t.transactionNr
  , t.downloadDate
  , t.terminalId
  , t.cardId
  , t.transactionDate
  , t.cardOldValue
  , t.transactionAmount
  , t.cardNewValue
  , t.transactionStatus
  , t.errorCode
  , te.merchantId
  , 'Null'
  FROM fss_transactions t
  INNER JOIN fss_terminal te
  ON t.terminalId = te.terminalId
  WHERE transactionNr < 90000;

CREATE OR REPLACE PROCEDURE upd_daily_transaction_settled(p_change_value VARCHAR2)
IS
BEGIN
  UPDATE fss_daily_transactions
  SET settlementStatus = p_change_value
  WHERE transactionNr <= pkg_fss_settlement.get_last_transaction_nr
  AND terminalId IN 
    (
      SELECT terminalId
      FROM fss_daily_transactions
      WHERE settlementStatus IS NULL
      HAVING minTransaction < SUM(transactionAmount)
      GROUP BY terminalId, minTransaction
    );
  COMMIT;
END;
/

INSERT INTO fss_daily_settlement
      (
        merchantBsb
        , merchantAccNum
        , tranCode
        , transaction
        , merchantTitle
        , bankingFlag
      )
      SELECT m.merchantBankBsb
        , m.merchantBankAccNr
        , '13'
        , SUM(dt.transactionAmount) AS value
        , m.merchantAccountTitle
        , 'F'
      FROM fss_daily_transactions dt
      INNER JOIN fss_smartcard s
      ON dt.cardId = s.cardId
      INNER JOIN fss_terminal te
      ON dt.terminalId = te.terminalId
      INNER JOIN fss_terminal_type tet
      ON te.terminalType = tet.typeName
      INNER JOIN fss_merchant m
      ON te.merchantId = m.merchantId
      WHERE dt.settlementStatus = 'Checked'
      GROUP BY m.merchantBankBsb
        , m.merchantBankAccNr
        , m.merchantAccountTitle;
        
CREATE OR REPLACE FUNCTION get_last_lodgement_ref
  RETURN NUMBER
  IS
    l_last_lodgement_ref NUMBER;
  BEGIN
    SELECT MAX(lodgementRef)
    INTO l_last_lodgement_ref
    FROM fss_daily_settlement;
    RETURN l_last_lodgement_ref;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_last_lodgement_ref');
  END;
  /