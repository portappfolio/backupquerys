select count(*)
from (
    select distinct trim(CMP.CMP_CODE) SERIAL
		from
			db_synergy.tbl_brz_absences AB 
			left join db_synergy.tbl_brz_cicmpy CMP on AB.CUSTOMERID = CMP.CMP_WWN
			
		WHERE 1=1
			AND AB.TYPE = 2090
			and date(ab.syscreated) >= '2023-06-01'
) 
where 
serial in (
	'01020018509511',
)