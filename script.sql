With aaa as (
	SELECT
	 pc.id
	,pc."name"
	,c.name as equipamento_nome
	,medidas.measure_datetime
	,medidas.calculated_measure
	FROM pointconfig pc

	INNER JOIN client c ON c."id" = pc.client_id
	
	LEFT JOIN LATERAL (
		SELECT 
			m.measure_datetime
		   ,m.calculated_measure
		
		FROM measure m 
		WHERE m.pointconfig_id = pc."id"
		LIMIT 5000
	) medidas on true
	
	WHERE
	c.modeltype_id NOT IN (7,13,18)
	--AND m.measure_datetime BETWEEN (NOW() - INTERVAL '5 minutes') AND NOW()
	AND c.id NOT IN (93)
	AND pc.id IN (12652,12669,44347,54359,85035,12651,12670,39654,44348,45139,47863,49934,49944,54361,74853,85034)

)

SELECT
pc.id
,pc."name"
,pc.equipamento_nome
,MIN(pc.measure_datetime) "MIN_DATETIME"
,MAX(pc.measure_datetime) "MAX_DATETIME"
,AVG(pc.calculated_measure) "MEAN_CAL_MEASURE"
,MIN(pc.calculated_measure) "MIN_CAL_MEASURE"
,MAX(pc.calculated_measure) "MAX_CAL_MEASURE"
,COUNT(*)
FROM aaa pc

GROUP BY
pc.id
,pc."name"
,pc.equipamento_nome

HAVING MIN(pc.measure_datetime) IS NOT null


ORDER BY pc."name", pc.id, MAX(measure_datetime)
