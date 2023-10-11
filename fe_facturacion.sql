select 
    a.cohorte_renovacion
    , a.antiguedad
    , a.producto_renovacion
    , case 
        when a.producto_renovacion in ('50','24','12','6') then 'FE 24'
        when a.producto_renovacion in ('60','') then 'FE 60'
        when a.producto_renovacion in ('80','120') then 'FE 80 -120'
        when a.producto_renovacion in ('260','300') then 'FE 260 - 300'
        when a.producto_renovacion in ('FE pro','500','1500','Pos') then 'FE 1500 Pro'
        when a.producto_renovacion in ('100') then 'FE 100 Mensual'
    end tier
    , a.uso
    , count(a.serial) `Total Units`
    , sum(a.valor_renovacion) `Total Revenue`
    , sum(`2023`) `Units`
    , sum(`$$ 2023`) `Revenue`
    , sum(`2023 Rn`) `Units Rn`
    , sum(`$$ 2023 Rn`) `Revenue Rn`
    , sum(`2023 Up`) `Units Up`
    , sum(`$$ 2023 Up`) `Revenue Up`
    , sum(`2023 Df`) `Units Df`
    , sum(`$$ 2023 Df`) `Revenue Df`
  
from
(
  select 
      b.*
      , coalesce(f.`_2022`,0) `2022`
      , coalesce(f.Pagos_2022,0) `$$ 2022`
      , coalesce(f.`_2023`,0) `2023`
      , coalesce(f.Pagos_2023,0) `$$ 2023`
      , coalesce(f.`_2023_Rn`,0) `2023 Rn`
      , coalesce(f.Pagos_2023_Rn,0) `$$ 2023 Rn`
      , coalesce(f.`_2023_Up`,0) `2023 Up`
      , coalesce(f.Pagos_2023_Up,0) `$$ 2023 Up`
      , coalesce(f.`_2023_Df`,0) `2023 Df`
      , coalesce(f.Pagos_2023_Df,0) `$$ 2023 Df`
      
  from
  (
    select
          trim(b.serial) serial 
          , trim(b.nit) nit
          , trim(coalesce(b.sucursal,'0')) sucursal
          , b.debtor_code
          , b.tipo_fe producto_renovacion
          , case when date(b.start_date) < '2021-12-01' then '2' else '1' end antiguedad
          , concat(
              SUBSTRING(b.prolongation_date, 1,7)
              ,'-01'
          ) cohorte_renovacion
          , case 
              when tipo_fe in ('50','24') then 90000
              when tipo_fe in ('60','') then 138000
              when tipo_fe in ('12') then 42000
              when tipo_fe in ('6') then 21000
              when tipo_fe in ('80','120') then 223900
              when tipo_fe in ('260','300') then 433900
              when tipo_fe in ('FE pro','500') then 558000
              when tipo_fe in ('1500') then 870000
              when tipo_fe in ('100') then 400000
          end valor_renovacion
          , case 
              when con_uso_60 >= 1 then 'Uso 60'
              when con_uso_90 >= 1 then 'Uso 90'
              when con_uso_historico >= 1 then 'Uso Historico'
              when sin_uso >= 1 then "Sin Uso"
          end uso
          , case when b.estado_synergy = 'Blocked' then 'PPTO 2022' else 'PPTO 2023' end tipo
      FROM 
      db_servicios.tbl_slv_clientes_ppto_fe b
  ) b
  left join db_servicios.tbl_slv_fe_facturacion f
    on trim(b.serial) = trim(f.serial)
) a
group by 1, 2, 3, 4, 5

