select 
	COUNT(*) = 0
FROM (		
	select
		t.id,
		COUNT(subobject_id) cnt 
	from erp.table t
	WHERE t.DELETED_AT IS NULL
		AND t.id IS NOT NULL
	GROUP BY
		t.id
	HAVING 
		COUNT(t.id) > 1 )