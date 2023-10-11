with nomina as (
    select distinct serial
    from db_synergy.tbl_slv_contratos_synergy
    where assortment_description = 'Nómina Electrónica' and estado_producto = 'Activo'
)

, activos as (
    select '01020120811309' serial
    union all select '01580023061601' serial
    union all select '01020023816602' serial
    union all select '01020020834907' serial
    union all select '01020222423005' serial
    union all select '01020522224211' serial
    union all select '01020220939910' serial
    union all select '01020521149711' serial
    union all select '01010022124010' serial
    union all select '01020522870012' serial
    union all select '01520022116102' serial
    union all select '01520020166509' serial
    union all select '01020023091601' serial
    union all select '01020521202512' serial
    union all select '01580022849406' serial
    union all select '01520121056112' serial
    union all select '01020422690910' serial
    union all select '01020321643109' serial
    union all select '01020422691210' serial
    union all select '01580022537004' serial
    union all select '01020322433407' serial
)

select 
        fe.serial
        , fe.nit
        , fe.CloudTenantIDMarket
        , ct.CloudTenantCompanyKey
        , fe.tipo_fe
        , fe.uso_60
        , rx.email
        , ih.*

from 
db_servicios.tbl_slv_fe_activos fe
left join db_cloudcontrol.tbl_brz_cloudtenant ct on fe.CloudTenantIDMarket = ct.CloudTenantID
--left join nomina n on fe.serial = n.serial
left join activos a on fe.serial = a.serial
left join db_estrategia.tbl_brz_rx_base_instalada rx on fe.serial = rx.serial
left join db_estrategia.tbl_slv_info_hubspot ih
  on ih.serial = fe.serial
where 
fe.estado_synergy = 'Active' 
--and date(fe.start_date) < '2023-05-01'
--and fe.uso_historico >= 10
--and n.serial is null
and a.serial is null
and (not ct.CloudTenantCompanyKey is null)
order by fe.uso_historico desc
--limit 100000
