SELECT 

equip.id
,equip.name
,pc.id
,pc.name
,MIN(measure.measure_datetime)
,MAX(measure.measure_datetime)

FROM equipmentinstallation equip

INNER JOIN pointconfig pc on pc.equipmentinstallation_id = equip.id
INNER JOIN measure on measure.pointconfig_id = pc.id

WHERE 
-- pc.id IN (84925,84926)
equip.id IN (341)


and measure.measure_datetime BETWEEN '2022-01-01' and NOW()

GROUP BY

equip.id
,equip.name
,pc.id
,pc.name

ORDER BY pc.name