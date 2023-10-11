select distinct
    b.serial
    , b.nit
    , b.sucursal
    , b.debtor_code
    , b.producto_renovacion
    , b.cohorte_renovacion
    , b.valor_renovacion
    , b.uso
    , b.tipo
    , b.antiguedad
    , f.estado_synergy
    , f.ampliacion_vigencia
    , coalesce(f.`_2023`,0) `2023`
    , coalesce(f.Pagos_2023,0) `$$ 2023`
    , coalesce(contactos.lead_telefono,rx.contacto_principal, rx.contacto_1, rx.contacto_2, rx.contacto_3) telefono_comercial
    , coalesce(contactos.lead_email, rx.email) email_comercial
    , coalesce(contactos.lead_nombre,rx.nombre_principal, rx.nombre_1, rx.nombre_2, rx.nombre_3) nombre_comercial
    , fe.estado_synergy
    , concat(
            SUBSTRING(fe.prolongation_date, 1,7)
            ,'-01'
    ) renovacion_actual
    , fe.estado implementacion
    , nc.hid id_nc
    , nc.motivo motivo_nc
    , cm.hid id_compromiso
    , cm.compromiso
    , cm.fec_compromiso
    , SUBSTRING(cm.RequestComments,1,50) request_comments
    , SUBSTRING(cm.WorkflowComments,1,50) workflow_comments

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
            when sin_uso >= 1 then 'Sin Uso'
        end uso
        , case when b.estado_synergy = 'Blocked' then 'PPTO 2022' else 'PPTO 2023' end tipo
    FROM 
    db_servicios.tbl_slv_clientes_ppto_fe b
) b
left join db_servicios.tbl_slv_fe_facturacion f
  on trim(b.serial) = trim(f.serial)
left join (
  select * 
  from 
  (
    SELECT 
        trim(cmp.cmp_code) serial
        , trim(cmp.debcode) nit
        , a.*
        , row_number() over(partition by id_cuenta order by fec_creacion desc) rown
    FROM
    (
        SELECT 
            ID_CUENTA,
            LEAD_TELEFONO,
            LEAD_EMAIL,
            LEAD_NOMBRE,
            AVANCE_NEGOCIACION,
            CIERRE,
            date(FEC_CREACION) fec_creacion
      from
      db_comercial.tbl_slv_seguimientos_comerciales
      where cierre = 'Venta' and (not LEAD_TELEFONO in ('3444444444','344444444','1','3 1 5 5 7','0','3111111','3')) and (not LEAD_TELEFONO is null)
      union all
      SELECT 
            ID_CUENTA,
            LEAD_TELEFONO,
            LEAD_EMAIL,
            LEAD_NOMBRE,
            AVANCE_NEGOCIACION,
            CIERRE,
            date(FEC_CREACION) fec_creacion
      from
      db_comercial.vw_slv_seguimientos_aliados
      where cierre = 'Venta' and (not LEAD_TELEFONO in ('3444444444','344444444','1','3 1 5 5 7','0','3111111','3')) and (not LEAD_TELEFONO is null)
    ) a
    INNER JOIN db_synergy.tbl_brz_cicmpy cmp 
      on UPPER(a.ID_CUENTA) = UPPER(cmp.cmp_wwn)
  ) a
  where a.rown = 1
) contactos
  on trim(b.serial) = contactos.serial
left join db_estrategia.tbl_brz_rx_base_instalada rx 
  on trim(b.serial) = rx.serial
left join db_servicios.tbl_slv_fe_activos fe on trim(b.serial) = trim(fe.serial)
left join (
  select * from
  (
    select A.FreeTextField_01 motivo
          , A.HID
          , trim(C.cmp_code) serial
          , row_number() OVER (PARTITION BY trim(C.cmp_code) ORDER BY date(A.syscreated) desc) rown 
    from db_synergy.tbl_brz_absences A
    left join db_synergy.tbl_brz_cicmpy C ON UPPER(C.cmp_wwn) = UPPER(A.CustomerID)
    where 
    a.type = 4020
    and date(A.syscreated) >= '2023-01-01'
  )
  where rown = 1 
) nc on b.serial = nc.serial
left join (
  select * from
  (
    select A.FreeTextField_01 compromiso
          , A.FreeDateField_01 fec_compromiso
          , A.HID
          , A.RequestComments
          , A.WorkflowComments
          , trim(C.cmp_code) serial
          , row_number() OVER (PARTITION BY trim(C.cmp_code) ORDER BY date(A.syscreated) desc) rown 
    from db_synergy.tbl_brz_absences A
    left join db_synergy.tbl_brz_cicmpy C ON UPPER(C.cmp_wwn) = UPPER(A.CustomerID)
    where 
    a.type = 4035
    and date(A.syscreated) >= '2023-01-01'
  )
  where rown = 1 
) cm on b.serial = cm.serial

where
--b.uso != 'Sin Uso'
--(b.cohorte_renovacion IN ('2023-01-01','2023-02-01') and coalesce(f.`_2023`,0) = 0)
--or (b.cohorte_renovacion = '2023-12-01' and b.tipo = 'PPTO 2022' and coalesce(f.`_2022`,0) = 0)
b.cohorte_renovacion in ('2023-03-01')
--and coalesce(f.`_2023`,0) = 0
--and nc.hid is null

order by 8 asc, 7 desc
