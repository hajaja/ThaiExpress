------3511, 合约信息, sheet: Contract
--SELECT Top 1* FROM [10.1.37.70].wind.dbo.TB_OBJECT_3511
SELECT
F1_3511 as SecuCodeRaw, 
F2_3511 as SecuAbbr, 
F6_3511 as FirstTradingDay,
F7_3511 as LastTradingDay,
F9_3511 as DeliveryDay, 
F11_3511 as DeliveryMonth, 
F13_3511 as SecuID, 
F14_3511 as SecuAbbrShort
FROM [10.1.37.70].wind.dbo.TB_OBJECT_3511
WHERE F4_3511 in ('CZCE', 'DCE', 'SHFE', 'CFFEX')
AND F2_3511 not like '%仿真%'
AND F7_3511 > '20160101'


------3517, 行情信息, sheet: ALL
--SELECT Top 1* FROM [10.1.37.70].wind.dbo.TB_OBJECT_3517

SELECT
F1_3517	as SecuCodeRaw,
F2_3517 as 'Date', 
F3_3517	as 'Open',
F4_3517	as 'High',
F5_3517	as 'Low',
F6_3517	as 'Close',
F7_3517	as Settle,
F8_3517	as TurnoverVolume,
F9_3517	as Position,
F10_3517 as TurnoverValue, 
F11_3517 as DeltaClose,
F12_3517 as DeltaSettle,
F13_3517 as DeltaPosition,
F14_3517 as PreSettle,
F15_3517 as SecuID
FROM [10.1.37.70].wind.dbo.TB_OBJECT_3517
WHERE F15_3517 in 
(
	SELECT F13_3511 FROM 
	(
	SELECT DISTINCT F2_3511, F13_3511 FROM [10.1.37.70].wind.dbo.TB_OBJECT_3511
	WHERE F4_3511 in ('CZCE', 'DCE', 'SHFE', 'CFFEX')
	AND F2_3511 not like '%仿真%'
	) t
)
AND F2_3517> '20170824'
--AND F1_3517 like 'FG%'
ORDER BY Date

