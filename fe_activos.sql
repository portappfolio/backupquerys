
select *
      --ih.*
      --, rx.contacto_1
      --, rx.nombre_1
      --, rx.contacto_2
      --, rx.nombre_2
      --, rx.contacto_3
      --, rx.nombre_3
from db_servicios.tbl_slv_fe_activos fe 
--inner join db_estrategia.tbl_slv_info_hubspot ih 
  --on fe.serial = trim(ih.serial)
--inner join db_estrategia.tbl_brz_rx_base_instalada rx on fe.serial = rx.serial

where 
fe.estado_synergy = 'Active'
--and fe.estado_fe = "Exitoso"
--and left(start_date,7) = '2023-02'
--order by fe.uso_60 desc
--or
--(estado_synergy = 'Blocked' and left(prolongation_date,7) = '2024-05')