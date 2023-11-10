with series as (
  select distinct
      trim(b.RFC) rfc
      , trim(b.Email_1) email
      , b.CLASIFICACION_RFC
      , b.CLASIFICACION_CANAL
      , trim(b.NUMSERIE) NUMSERIE
      -- , b.TIMBRA_ILIMITADO_12M
      -- , b.TIMBRA_PAQUETES_12M
      -- , b.CLASIFICACION_SERIE
      -- , b.TIPO_SERVICIO
      -- , b.PRODUCTO
      -- , b.PROVEEDOR_PRODUCTO
      , b.PERIODICIDAD
      , b.ACTUALIZACION_CFDI
      , b.USUARIOS
      , b.VERSION_SISTEMA
      , date(b.FECHA_ULT_ACTIVACION) FECHA_ULT_ACTIVACION
      , date(b.FECHA_COMPRA) FECHA_COMPRA
      , date(b.FECHA_INI) FECHA_INI
      , date(b.FECHA_FIN) FECHA_FIN
      , b.RAZON_SOCIAL
      -- , b.MARCA_DISTRIBUIDOR
      , b.NUM_DISTRIBUIDOR
      , b.RFC_DISTRIBUIDOR
      , b.NUM_POLIZA
      -- , b.ESTADO_POLIZA

  from db_aspel.tbl_slv_aspel_detalle_series_qm b
  where 
  b.CLASIFICACION_SERIE = 'Activas'
  and b.PRODUCTO = 'SIIGO_NUBE'
)


, previa_cmp as (
  select * from (
  select 
        s.*
        , case when not nullif(trim(cmp.cmp_code),'') is null then true else false end cruce_synergy
        , trim(cmp.cmp_code) serial
        , cmp.cmp_wwn customer_id_synergy
        , cmp.textfield5 plan_synergy
        , cmp.cmp_e_mail email_synergy
        , cmp.cmp_tel telefono_synergy
        , cmp.cmp_fcity ciudad_synergy
        , row_number() over(partition by trim(s.rfc) order by trim(cmp.cmp_code) asc) rown_cmp
        
  from series s
  left join db_synergy.tbl_brz_cicmpy cmp
    on trim(s.rfc) = trim(cmp.debcode)
    and trim(cmp.cmp_fctry) = 'MX'
  ) where rown_cmp = 1
)

, contratos as (
  select cs.serial
          , cs.item_code
          , cs.ITEM_DESCRIPTION
  from
  (
    select 
          trim(cs.serial) serial
          , cs.ITEM_CODE
          , cs.ITEM_DESCRIPTION
          , row_number() over(partition by trim(cs.serial) order by date(cs.start_date) desc) rown_contratos
    from previa_cmp cmp
    inner join db_synergy.tbl_slv_contratos_synergy cs
      on cmp.serial = trim(cs.SERIAL)
    where 
    cs.ASSORTMENT_CODE = 951
    and cs.ESTADO_PRODUCTO = 'Activo'
    and not cs.ITEM_CODE in ('9510003000010')
  ) cs
  where cs.rown_contratos = 1
)

, acompanamiento as (
    select 
          distinct cs.serial
    from 
    previa_cmp cmp
    inner join db_synergy.tbl_slv_contratos_synergy cs
      on cmp.serial = trim(cs.SERIAL)
    where 
    cs.ASSORTMENT_CODE = 951
    and cs.ESTADO_PRODUCTO = 'Activo'
    and cs.ITEM_CODE in ('9510003000010')
)

, cmp as (
  select cmp.*
        , cs.item_code cod_producto_contratos
        , cs.ITEM_DESCRIPTION producto_contratos
        , case when not a.serial is null then true else false end pago_acompanamiento
  
  from previa_cmp cmp
  left join contratos cs 
    on cmp.serial = cs.serial
  left join acompanamiento a
    on cmp.serial = a.serial
)

, tcn as (
  select * from (
    select 
          cmp.*
          , case when not tcn.hid is null then true else false end con_tcn
          , tcn.id id_tcn
          , tcn.hid hid_tcn
          , hr.fullname asesor_tcn
          , hr.mail email_asesor_tcn
          , hrl.fullname supervisor_tcn
          , hrl.mail email_supervisor_tcn
          , tcn.FreeTextField_01 producto_tcn
          , date(tcn.syscreated) inicio_tcn
          , tcn.Realized cierre_tcn
          , datediff(coalesce(date(tcn.Realized),date(now())),date(tcn.syscreated)) dias_tcn
          , case
              when tcn.FreeIntField_05 = 14 then 'Normal en Marcha'
              when tcn.FreeIntField_05 = 18 then 'Sin Instalar'
              when tcn.FreeIntField_05 = 21 then 'Sin Iniciar'
              when tcn.FreeIntField_05 = 22 then 'Sin Iniciar - Pendiente Comercial'
              when tcn.FreeIntField_05 = 23 then 'En marcha - Avance Lento'
              when tcn.FreeIntField_05 = 24 then 'En marcha - Va rapido'
              when tcn.FreeIntField_05 = 25 then 'En marcha - Cerca del cierre'
              when tcn.FreeIntField_05 = 26 then 'Cerrado - Usabilidad comprobada total'
              when tcn.FreeIntField_05 = 29 then 'Cerrado - Usabilidad comprobada parcial'
              when tcn.FreeIntField_05 = 32 then 'Cerrado - Usabilidad comprobada x Cliente'
              when tcn.FreeIntField_05 = 31 then 'no acepta conversión / Fe Gratis'
              when tcn.FreeIntField_05 = 20 then 'Cambio de producto / Retoma'
              when tcn.FreeIntField_05 = 19 then 'Inicio Futuro'
              when tcn.FreeIntField_05 = 33 then 'Inicio Futuro Parametrizado'
              when tcn.FreeIntField_05 = 27 then 'Ilocalizado'
              when tcn.FreeIntField_05 = 28 then 'Anulado'
              when tcn.FreeIntField_05 = 17 then 'Perdida x Servicio o Estabilidad'
              when tcn.FreeIntField_05 = 34 then 'Perdida x Precio'
              when tcn.FreeIntField_05 = 35 then 'Perdida x Empresa en Liquidación'
              when tcn.FreeIntField_05 = 30 then 'Perdida x Funcionalidad'
              when tcn.FreeIntField_05 = 36 then 'Perdida x Cliente (incumple citas o no avanza)'
              when tcn.FreeIntField_05 = 37 then 'Pendiente Desarrollo'
              when tcn.FreeIntField_05 = 38 then 'En Marcha - Seleccionó Distribuidor '
              when tcn.FreeIntField_05 = 39 then 'Cerrado - Usabilidad comprobada Distribuidor'
              when tcn.FreeIntField_05 = 40 then 'Cierre usabilidad autónoma'
              when tcn.FreeIntField_05 = 41 then 'Gestión UCI'
          end estado_tcn
          , tcn.FreeNumberField_01 avance_tcn
          , tcn.FreeTextField_04 modulo_actual_tcn
          , date(tcn.StartDate) inicio_onboarding_tcn
          , date(tcn.EndDate) cierre_estimado_onboarding_tcn
          , tcn.FreeDateField_02 inicio_futuro_tcn
          , tcn.FreeNumberField_04 csat_tcn
          , row_number() over(partition by cmp.rfc order by date(tcn.syscreated) desc) rown_tcn

    from 
    cmp 
    left join db_synergy.tbl_brz_absences tcn
      on cmp.customer_id_synergy = tcn.CustomerID
      and tcn.type = 2101
      and not tcn.FreeTextField_01 in ('NOI','COI','SAE')
    left join db_synergy.tbl_brz_humres hr
      on tcn.EmpID = hr.res_id
    left join db_synergy.tbl_brz_humres hrl
      on hr.repto_id = hrl.res_id
  ) where rown_tcn = 1
)

, previa_rastros as (
    select 
          rastro.RelatedRequestID
          , rastro.id id_rastro
          , rastro.hid hid_rastro
          , date(rastro.syscreated) fecha_rastro
          , rastro.hours horas_rastro
          , rastro.FreeTextField_01 tipo_rastro
          , coalesce(lag(date(rastro.syscreated)) over(partition by rastro.RelatedRequestID order by date(rastro.syscreated)),tcn.inicio_tcn) fecha_rastro_anterior
          , row_number() over(partition by rastro.RelatedRequestID order by date(rastro.syscreated) desc) rown_rastro

    from 
    tcn
    inner join db_synergy.tbl_brz_absences rastro
      on tcn.id_tcn = rastro.RelatedRequestID
      and rastro.Type = 2200
)

, rastros as (
  select 
        r.RelatedRequestID padre_rastro
        , r.fecha_rastro ultima_sesion
        , r.tipo_rastro tipo_ultima_sesion
        , ra.cant_sesiones
        , ra.horas_sesiones
        , ra.dias_entre_sesiones

  from 
  (
      select *
      from previa_rastros pr
      where pr.rown_rastro = 1
  ) r
  left join (
      select pr.RelatedRequestID
              , count(pr.RelatedRequestID) cant_sesiones
              , sum(pr.horas_rastro) horas_sesiones
              , avg(date_diff(pr.fecha_rastro, pr.fecha_rastro_anterior)) dias_entre_sesiones
      from previa_rastros pr
      group by 1
  ) ra on r.RelatedRequestID = ra.RelatedRequestID
)

, final_tcn as (
  select tcn.*
        , case when not rastros.padre_rastro is null then true else false end con_sesiones
        , rastros.ultima_sesion
        , rastros.tipo_ultima_sesion
        , rastros.cant_sesiones
        , round(rastros.horas_sesiones,2) horas_sesiones
        , round(rastros.dias_entre_sesiones,1) dias_entre_sesiones

  from tcn 
  left join rastros
    on tcn.id_tcn = rastros.padre_rastro
)

-- select *
-- from final_tcn

, market as (
select distinct
      ct.CloudTenantID cloudtenantid_market
      , mt.CloudMultiTenantName multitnenat_market
      , ct.TenantID tenantid_market
      , ct.CloudTenantCompanyKey companykey_market
      , ct.PlanType plantype_market
      , case 
            when ct.PlanType =  24 then 'Siigo Nube Total Inicio'
            when ct.PlanType =  23 then 'Siigo Nube Total Avanzado'
            when ct.PlanType =  14 then 'Siigo Nube Total Premium'
            when ct.PlanType =  17 then 'Siigo Nube Gestión Premium'
            when ct.PlanType =  21 then 'Siigo Nube Gestión Avanzado'
            when ct.PlanType =  22 then 'Siigo Nube Gestión Inicio'
            when ct.PlanType =  20 then 'Siigo Nube Facturación'
            when ct.PlanType =  20 then 'Siigo Nube Facturación Duo'
        else 'Sin Identificar' end tipo_producto_market
      , date(ct.CreateByDate) creacion_market
      , date(ct.UpdateByDate) actualizacion_market
      , coalesce(nullif(trim(ct.serial),''),nullif(trim(c.serial),'')) serial_market
      , coalesce(nullif(trim(ct.TenantNit),''),nullif(trim(ct.nit),'') ,nullif(trim(c.nit),'')) rfc_market
      , ct.state estado_market

  from db_cloudcontrolmexico.tbl_brz_cloudtenant ct
  left join db_cloudcontrolmexico.tbl_brz_cloudmultitenant mt
    on ct.CloudMultiTenantCode = mt.CloudMultiTenantID
  left join db_nubemultitenantmexico.tbl_brz_company c
    on ct.TenantID = c.TenantID
    and c.CloudMultiTenantName = mt.CloudMultiTenantName
)

, utilizacion as (
  select 
      m.cloudtenantid_market
      , m.multitnenat_market
      , m.tenantid_market
      , m.companykey_market
      , m.plantype_market
      , m.tipo_producto_market
      , m.creacion_market
      , m.actualizacion_market
      , m.serial_market
      , m.rfc_market
      , m.estado_market
      
      , max(date(ac.CreatedByDate)) ultimo_uso

      , count(ac.ACEntryID) uso_historico
      , count(case when date(ac.CreatedByDate) between dateadd(now(),-60 ) and date(now()) then ac.ACEntryID else null end) uso_60
      , count(case when date(ac.CreatedByDate) between dateadd(now(),-90 ) and date(now()) then ac.ACEntryID else null end) uso_90
      , count(case when date(ac.CreatedByDate) between dateadd(now(),-180 ) and date(now()) then ac.ACEntryID else null end) uso_180
      , count(case when date(ac.CreatedByDate) between dateadd(now(),-270 ) and date(now()) then ac.ACEntryID else null end) uso_270

      , count(case when left(date(ac.CreatedByDate),7) in ('2023-03', '2023-04') then ac.ACEntryID else null end) uso_60_abr_2023
      , count(case when left(date(ac.CreatedByDate),7) in ('2023-04', '2023-05') then ac.ACEntryID else null end) uso_60_may_2023
      , count(case when left(date(ac.CreatedByDate),7) in ('2023-05', '2023-06') then ac.ACEntryID else null end) uso_60_jun_2023
      , count(case when left(date(ac.CreatedByDate),7) in ('2023-06', '2023-07') then ac.ACEntryID else null end) uso_60_jul_2023
      , count(case when left(date(ac.CreatedByDate),7) in ('2023-07', '2023-08') then ac.ACEntryID else null end) uso_60_ago_2023
      , count(case when left(date(ac.CreatedByDate),7) in ('2023-08', '2023-09') then ac.ACEntryID else null end) uso_60_sep_2023
      , count(case when left(date(ac.CreatedByDate),7) in ('2023-09', '2023-10') then ac.ACEntryID else null end) uso_60_oct_2023
      , count(case when left(date(ac.CreatedByDate),7) in ('2023-10', '2023-11') then ac.ACEntryID else null end) uso_60_nov_2023

  from 
  market m
  left join db_nubemultitenantmexico.tbl_brz_acentry ac
   on m.tenantid_market = ac.TenantID and m.multitnenat_market = ac.CloudMultiTenantName

  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
-- select *
-- from utilizacion

, market_rfc as (
  select 
        u.*
        , row_number() over (partition by u.rfc_market order by ultimo_uso desc) rown_rfc

  from utilizacion u
  
  where not nullif(u.rfc_market,'') is null
)

, market_serial as (
  select 
        cloudtenantid_market cloudtenantid_market_serial
        , multitnenat_market multitnenat_market_serial
        , tenantid_market tenantid_market_serial
        , companykey_market companykey_market_serial
        , plantype_market plantype_market_serial
        , tipo_producto_market tipo_producto_market_serial
        , creacion_market creacion_market_serial
        , actualizacion_market actualizacion_market_serial
        , serial_market serial_market_serial
        , rfc_market rfc_market_serial
        , estado_market estado_market_serial
        , uso_historico uso_historico_serial
        , ultimo_uso ultimo_uso_serial
        , uso_60 uso_60_serial
        , uso_90 uso_90_serial
        , uso_180 uso_180_serial
        , uso_270 uso_270_serial
        , uso_60_abr_2023 uso_60_abr_2023_serial
        , uso_60_may_2023 uso_60_may_2023_serial
        , uso_60_jun_2023 uso_60_jun_2023_serial
        , uso_60_jul_2023 uso_60_jul_2023_serial
        , uso_60_ago_2023 uso_60_ago_2023_serial
        , uso_60_sep_2023 uso_60_sep_2023_serial
        , uso_60_oct_2023 uso_60_oct_2023_serial
        , uso_60_nov_2023 uso_60_nov_2023_serial
        , row_number() over (partition by u.serial_market order by ultimo_uso desc) rown_serial

  from utilizacion u
  
  where not nullif(u.serial_market,'') is null
)

, cruce_market_rfc as (
  select tcn.*
        , mrfc.*
  
  from final_tcn tcn
  left join market_rfc mrfc 
    on tcn.rfc = mrfc.rfc_market
    and mrfc.rown_rfc = 1
)

, cruce_market_serial as (
  select mrfc.*
        , mserial.*
  
  from cruce_market_rfc mrfc
  left join market_serial mserial 
    on mrfc.serial = mserial.serial_market_serial
    and mserial.rown_serial = 1
)
, semifinal as (
  select 
        rfc
        , email
        , clasificacion_rfc estado
        , clasificacion_canal
        , numserie serie
        , periodicidad
        , producto_contratos
        , pago_acompanamiento
        , actualizacion_cfdi
        , usuarios
        , version_sistema
        , fecha_ult_activacion
        , fecha_compra
        , fecha_ini
        , left(fecha_ini,7) anno_mes_inicio
        , fecha_fin
        , left(fecha_fin,7) anno_mes_fin
        , razon_social
        , num_distribuidor
        , rfc_distribuidor
        , num_poliza
        , cruce_synergy
        , serial
        , customer_id_synergy
        , plan_synergy
        , email_synergy
        , telefono_synergy
        , ciudad_synergy
        , con_tcn
        , id_tcn
        , hid_tcn
        , asesor_tcn
        , email_asesor_tcn
        , supervisor_tcn
        , email_supervisor_tcn
        , producto_tcn
        , inicio_tcn
        , cierre_tcn
        , dias_tcn
        , estado_tcn
        , avance_tcn
        , modulo_actual_tcn
        , inicio_onboarding_tcn
        , cierre_estimado_onboarding_tcn
        , inicio_futuro_tcn
        , csat_tcn
        , con_sesiones
        , ultima_sesion
        , tipo_ultima_sesion
        , cant_sesiones
        , horas_sesiones
        , dias_entre_sesiones
        
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60,0) else coalesce(uso_60_serial,0) end uso_60
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_90,0) else coalesce(uso_90_serial,0) end uso_90
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_180,0) else coalesce(uso_180_serial,0) end uso_180
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_270,0) else coalesce(uso_270_serial,0) end uso_270

        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null and cloudtenantid_market is null and cloudtenantid_market_serial is null then 'No Cruza' when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then 'RFC' else 'Serial' end cruce_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(cloudtenantid_market,cloudtenantid_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then cloudtenantid_market else cloudtenantid_market_serial end cloudtenantid_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(tenantid_market,tenantid_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then tenantid_market else tenantid_market_serial end tenantid_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(companykey_market,companykey_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then companykey_market else companykey_market_serial end companykey_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(plantype_market,plantype_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then plantype_market else plantype_market_serial end plantype_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(tipo_producto_market,tipo_producto_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then tipo_producto_market else tipo_producto_market_serial end tipo_producto_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(creacion_market,creacion_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then creacion_market else creacion_market_serial end creacion_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(actualizacion_market,actualizacion_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then actualizacion_market else actualizacion_market_serial end actualizacion_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(serial_market,serial_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then serial_market else serial_market_serial end serial_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(rfc_market,rfc_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then rfc_market else rfc_market_serial end rfc_market
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then coalesce(estado_market,estado_market_serial) when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then estado_market else estado_market_serial end estado_market

        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then null when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then ultimo_uso else ultimo_uso_serial end ultimo_uso
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then null when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then uso_historico else uso_historico_serial end uso_historico
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_abr_2023,0) else coalesce(uso_60_abr_2023_serial,0) end uso_60_abr_2023
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_may_2023,0) else coalesce(uso_60_may_2023_serial,0) end uso_60_may_2023
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_jun_2023,0) else coalesce(uso_60_jun_2023_serial,0) end uso_60_jun_2023
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_jul_2023,0) else coalesce(uso_60_jul_2023_serial,0) end uso_60_jul_2023
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_ago_2023,0) else coalesce(uso_60_ago_2023_serial,0) end uso_60_ago_2023
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_sep_2023,0) else coalesce(uso_60_sep_2023_serial,0) end uso_60_sep_2023
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_oct_2023,0) else coalesce(uso_60_oct_2023_serial,0) end uso_60_oct_2023
        , case when coalesce(ultimo_uso,ultimo_uso_serial) is null then 0 when coalesce(ultimo_uso,date('1999-12-01')) >= coalesce(ultimo_uso_serial,date('1999-12-01')) then coalesce(uso_60_nov_2023,0) else coalesce(uso_60_nov_2023_serial,0) end uso_60_nov_2023

  from cruce_market_serial
)

, final as (
  select 
            rfc
            , estado
            , serie
            , periodicidad
            , case 
                  when coalesce(producto_contratos, tipo_producto_market) ilike '%Facturación%' then 'Facturación'
                  when coalesce(producto_contratos, tipo_producto_market) ilike '%Gestión%' then 'Gestión'
                  when coalesce(producto_contratos, tipo_producto_market) ilike '%Total%' then 'Total'
            end tier_producto
            , coalesce(producto_contratos, tipo_producto_market) producto
            , pago_acompanamiento
            --, actualizacion_cfdi
            , usuarios
            --, version_sistema
            , fecha_ult_activacion
            , fecha_compra
            , fecha_ini
            , anno_mes_inicio
            , fecha_fin
            , anno_mes_fin
            , razon_social
            , coalesce(nullif(email,''), nullif(email_synergy,'')) email
            , telefono_synergy

            , clasificacion_canal
            , num_distribuidor
            , rfc_distribuidor
            , num_poliza

            , cruce_synergy
            , serial
            , customer_id_synergy
            , plan_synergy
            , ciudad_synergy
            , con_tcn
            , id_tcn
            , hid_tcn
            , asesor_tcn
            , email_asesor_tcn
            , supervisor_tcn
            , email_supervisor_tcn
            , producto_tcn
            , inicio_tcn
            , cierre_tcn
            , dias_tcn
            , estado_tcn
            , avance_tcn
            , modulo_actual_tcn
            , inicio_onboarding_tcn
            , cierre_estimado_onboarding_tcn
            , inicio_futuro_tcn
            , csat_tcn

            , con_sesiones
            , ultima_sesion
            , tipo_ultima_sesion
            , cant_sesiones
            , horas_sesiones
            , dias_entre_sesiones
            
            
            , cruce_market
            , cloudtenantid_market
            , tenantid_market
            , companykey_market
            , plantype_market
            , creacion_market
            , actualizacion_market
            , serial_market
            , rfc_market
            , estado_market

            , ultimo_uso
            
            , uso_60
            , uso_90
            , uso_180
            , uso_270
            , uso_historico

            , case when coalesce(uso_60,0) > 0 then 1 else 0 end con_uso_60
            , case when coalesce(uso_90,0) > 0 then 1 else 0 end con_uso_90
            , case when coalesce(uso_180,0) > 0 then 1 else 0 end con_uso_180
            , case when coalesce(uso_270,0) > 0 then 1 else 0 end con_uso_270
            , case when coalesce(uso_historico,0) > 0 then 1 else 0 end con_uso_historico

            , uso_60_abr_2023
            , uso_60_may_2023
            , uso_60_jun_2023
            , uso_60_jul_2023
            , uso_60_ago_2023
            , uso_60_sep_2023
            , uso_60_oct_2023
            , uso_60_nov_2023
            
            , case when coalesce(uso_60_abr_2023,0) > 0 then 1 else 0 end con_uso_60_abr_2023
            , case when coalesce(uso_60_may_2023,0) > 0 then 1 else 0 end con_uso_60_may_2023
            , case when coalesce(uso_60_jun_2023,0) > 0 then 1 else 0 end con_uso_60_jun_2023
            , case when coalesce(uso_60_jul_2023,0) > 0 then 1 else 0 end con_uso_60_jul_2023
            , case when coalesce(uso_60_ago_2023,0) > 0 then 1 else 0 end con_uso_60_ago_2023
            , case when coalesce(uso_60_sep_2023,0) > 0 then 1 else 0 end con_uso_60_sep_2023
            , case when coalesce(uso_60_oct_2023,0) > 0 then 1 else 0 end con_uso_60_oct_2023
            , case when coalesce(uso_60_nov_2023,0) > 0 then 1 else 0 end con_uso_60_nov_2023

  from semifinal
)

select 
      *
from final
