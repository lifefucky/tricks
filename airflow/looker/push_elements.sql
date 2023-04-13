MERGE INTO MONITORING.LOOKER_ELEMENTS dwh
USING TEMP.ELEMENT_DATA looker 
	ON dwh.element_id=looker.element_id AND dwh.dashboard_id=looker.dashboard_id
WHEN MATCHED THEN 
	UPDATE SET 
	element_title=looker.element_title,
	element_model=looker.element_model,
	element_view=looker.element_view,
	element_uses_marts=looker.element_uses_marts,
	updated_at=current_timestamp::DATETIME,
	replaced_at=NULL
WHEN NOT MATCHED THEN 
	INSERT (dashboard_id, element_id, element_title, element_model, element_view, element_uses_marts, updated_at)
	VALUES (looker.dashboard_id, looker.element_id, looker.element_title, looker.element_model, looker.element_view, looker.element_uses_marts, current_timestamp::DATETIME);

UPDATE MONITORING.LOOKER_ELEMENTS dwh
FROM (
	SELECT dwh.dashboard_id, dwh.element_id
	FROM MONITORING.LOOKER_ELEMENTS dwh 
	LEFT JOIN TEMP.ELEMENT_DATA looker 
		ON dwh.element_id=looker.element_id AND dwh.dashboard_id=looker.dashboard_id
	WHERE looker.dashboard_id IS NULL ) looker 
SET replaced_at=current_timestamp::DATETIME 
WHERE 1=1
	AND dwh.element_id=looker.element_id AND dwh.dashboard_id=looker.dashboard_id;

DELETE FROM MONITORING.LOOKER_ALERTS_ELEMENT WHERE ALERT_ELEMENT_ID IN (SELECT ELEMENT_ID FROM MONITORING.LOOKER_ELEMENTS WHERE REPLACED_AT IS NOT NULL );

TRUNCATE TEMP.ELEMENT_DATA;
