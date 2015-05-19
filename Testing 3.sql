SELECT MAX(LENGTH(merchantTitle))
    FROM fss_daily_settlement;

SELECT MAX(transactionNr)
    FROM fss_daily_transactions;

SELECT bsbm.merchantBsb
  , m.merchantBankAccNr
  , sum(dt.transactionAmount) AS value
  , m.merchantAccountTitle
  , (
      SELECT to_char(floor(orgBsbNr / 1000), '000') || '-' || to_char(orgBsbNr - floor(orgBsbNr/1000) * 1000, 'FM000') || '   ' || orgBankAccount AS trace
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
    , to_char(floor(merchantBankBsb / 1000), '000') || '-' || to_char(merchantBankBsb - floor(merchantBankBsb/1000) * 1000, 'FM000') AS merchantBsb
  FROM fss_merchant
) bsbm
ON bsbm.merchantBankBsb =  m.merchantBankBsb
WHERE dt.settlementStatus IS NULL
GROUP BY bsbm.merchantBsb
  , m.merchantBankAccNr
  , m.merchantAccountTitle;
  
BEGIN
  ins_daily_settlement;
END;
/
  
CREATE OR REPLACE PROCEDURE ins_daily_settlement
AS
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
          SELECT to_char(floor(orgBsbNr / 1000), '000') || '-' || to_char(orgBsbNr - floor(orgBsbNr/1000) * 1000, 'FM000') || '   ' || orgBankAccount AS trace
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
        , to_char(floor(merchantBankBsb / 1000), '000') || '-' || to_char(merchantBankBsb - floor(merchantBankBsb/1000) * 1000, 'FM000') AS merchantBsb
      FROM fss_merchant
    ) bsbm
    ON bsbm.merchantBankBsb =  m.merchantBankBsb
    WHERE dt.settlementStatus IS NULL
    GROUP BY bsbm.merchantBsb
      , m.merchantBankAccNr
      , m.merchantAccountTitle;
  COMMIT;
END;
/

CREATE OR REPLACE FUNCTION get_format_bsb(p_value VARCHAR2)
RETURN VARCHAR2
IS
BEGIN
  RETURN substr(p_value ,1,3) || '-' || substr(p_value, 4,6);
END;
/

select get_format_bsb(merchantbankbsb)
from fss_merchant;
  