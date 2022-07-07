MERGE INTO MONITORING.LOOKER_DASHBOARDS dwh
USING TEMP.DASHBOARD_DATA looker 
	ON dwh.dashboard_id=looker.dashboard_id 
WHEN MATCHED THEN 
	UPDATE SET 
	dashboard_name=looker.dashboard_name,
	creator = looker.creator,
	updated_at=current_timestamp::DATETIME,
	replaced_at=NULL 	
WHEN NOT MATCHED THEN 
	INSERT (dashboard_id, dashboard_name, creator, updated_at)
	VALUES (looker.dashboard_id, looker.dashboard_name, looker.creator, current_timestamp::DATETIME);

UPDATE MONITORING.LOOKER_DASHBOARDS dwh
FROM (
	SELECT dwh.dashboard_id
	FROM MONITORING.LOOKER_DASHBOARDS dwh 
	LEFT JOIN TEMP.DASHBOARD_DATA looker 
		ON dwh.dashboard_id=looker.dashboard_id 
	WHERE looker.dashboard_id IS NULL ) looker 
SET replaced_at=current_timestamp::DATETIME 
WHERE 1=1
	AND dwh.dashboard_id=looker.dashboard_id;

DROP TABLE TEMP.DASHBOARD_DATA;