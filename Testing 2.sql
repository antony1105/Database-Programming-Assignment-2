select * from fss_merchant;
select * from fss_terminal;
select * from fss_terminal_type;
select * from fss_daily_transactions
order by transactionNr desc;
select * from fss_reference;
select * from fss_transactions;
select * from fss_smartcard;
select * from fss_organisation;
select * from fss_daily_settlement;

truncate table fss_daily_transactions;

truncate table fss_daily_settlement;

INSERT INTO fss_daily_transactions
  (transactionNr)
VALUES (88000);


INSERT INTO fss_daily_settlement
  (lodgementRef)
VALUES
  (to_char(sysdate, 'YYYYMMDD')||to_char(seq_lodgement_reference.nextval, 'FM0000000'));

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
  
Select * from all_objects where object_type = 'DIRECTORY'
order by created desc;

CREATE OR REPLACE DIRECTORY WT_11993577 AS '/exports/orcloz';
GRANT READ ON WT_TEST TO PUBLIC; 

DECLARE
  l_file  utl_file.file_type;
BEGIN
  l_file := utl_file.fopen ('WT_TEST','U11993577.txt', 'W');
  utl_file.put_line(l_file, 'Test print');
  utl_file.fclose(l_file);
END;
/

-- Merchant BSB 999-999  -- Make Procedure
SELECT merchantBankBsb
  , to_char(floor(merchantBankBsb / 1000), '000') || '-' || to_char(merchantBankBsb - floor(merchantBankBsb/1000) * 1000, 'FM000') AS Richard
FROM fss_merchant;

-- Trace BSB and Account Number
SELECT to_char(floor(orgBsbNr / 1000), '000') || '-' || to_char(orgBsbNr - floor(orgBsbNr/1000) * 1000, 'FM000') || '   ' || orgBankAccount AS trace
FROM fss_organisation;