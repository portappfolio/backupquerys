with tbl_brz_customer_fe_facturacion_clientes_renovacion AS (
    SELECT 
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
        , case when b.estado_synergy = 'Blocked' then 'PPTO 2022' else 'PPTO 2023' end tipo
        , coalesce(av.ampliacion_vigencia, 0) ampliacion_vigencia

    FROM 
    db_servicios.tbl_slv_clientes_ppto_fe b
    left join (
        select 
            trim (cmp.cmp_code) serial 
            , sum(CAST(ab.FreeTextField_05 as INTEGER)) ampliacion_vigencia
        FROM 
        db_synergy.tbl_brz_absences ab
        left join db_synergy.tbl_brz_cicmpy cmp
        on UPPER(ab.CustomerID) = UPPER(cmp.cmp_wwn)
        where 
        ab.Type = 4015
        and ab.FreeTextField_04 = 'Vigencia'
        and date(ab.syscreated) >= '2022-12-09'
        and not ab.FreeTextField_05 is null
        group by 1
    ) av on b.serial = av.serial
)

, tbl_brz_customer_fe_facturacion_facturas_synergy as (
  SELECT 
    trim(f.serial) serial
    , '' nit
    , '' sucursal
    , 'F94' tipo_factura
    , c.cod_producto cod_producto
    , c.producto producto
    , cast(c.valor AS INTEGER) facturado
    , coalesce(f.documento_factura, f.nuestra_referencia) consecutivo
    , case
        when c.cod_producto = '9380002000100' then 'Downsell'
        when c.cod_producto = '9380002000101' then 'Downsell'
        when c.cod_producto = '9380002000128' then 'Downsell'
        when c.cod_producto = '9380002000141' then 'Downsell'
        when c.cod_producto = '9380002000145' then 'Downsell'
        when c.cod_producto = '9380002000125' then 'Downsell'
        when c.cod_producto = '9380002000127' then 'Downsell'
        when c.cod_producto = '9380002000117' then 'Downsell'
        when c.cod_producto = '9380002000126' then 'Downsell'
        when c.cod_producto = '9380002000118' then 'Downsell'
        when c.cod_producto = '9380002000094' then 'Downsell'
        when c.cod_producto = '9380002000140' then 'Downsell'
        when c.cod_producto = '9380002000143' then 'Downsell'
        when c.cod_producto = '9380002000130' then 'Downsell'
        when c.cod_producto = '9380002000162' then 'Downsell'
        when c.cod_producto = '9380002000121' then 'Downsell'
        when c.cod_producto = '9380002000124' then 'Downsell'
        when c.cod_producto = '9380002000122' then 'Downsell'
        when c.cod_producto = '9380002000123' then 'Downsell'
        when c.cod_producto = '9380002000120' then 'Downsell'
        when c.cod_producto = '9380002000142' then 'Downsell'
        when c.cod_producto = '9380002000144' then 'Downsell'
        when c.cod_producto = '9490001000009' then 'Downsell'
        when c.cod_producto = '9490001000008' then 'Downsell'
        when c.cod_producto = '9380002000132' then 'Downsell'
        when c.cod_producto = '9490001000007' then 'Downsell'
        when c.cod_producto = '9380002000095' then 'Renovacion'
        when c.cod_producto = '9380002000096' then 'Renovacion'
        when c.cod_producto = '9380002000097' then 'Renovacion'
        when c.cod_producto = '9380002000115' then 'Renovacion'
        when c.cod_producto = '9380002000116' then 'Renovacion'
        when c.cod_producto = '9380002000099' then 'Renovacion'
        when c.cod_producto = '9380002000098' then 'Renovacion'
        when c.cod_producto = '9380002000047' then 'Renovacion'
        when c.cod_producto = '9380002000034' then 'Renovacion'
        when c.cod_producto = '9380002000046' then 'Renovacion'
        when c.cod_producto = '9380002000139' then 'Renovacion'
        when c.cod_producto = '9380002000033' then 'Renovacion'
        when c.cod_producto = '9380002000012' then 'Renovacion'
        when c.cod_producto = '9380002000048' then 'Renovacion'
        when c.cod_producto = '9380002000138' then 'Renovacion'
        when c.cod_producto = '9380002000104' then 'Renovacion'
        when c.cod_producto = '9380002000013' then 'Renovacion'
        when c.cod_producto = '9380001000044' then 'Renovacion'
        when c.cod_producto = '9380001000039' then 'Renovacion'
        when c.cod_producto = '9380001000038' then 'Renovacion'
        when c.cod_producto = '9380001000043' then 'Renovacion'
        when c.cod_producto = '9380001000041' then 'Renovacion'
        when c.cod_producto = '9380001000040' then 'Renovacion'
        when c.cod_producto = '9380001000045' then 'Renovacion'
        when c.cod_producto = '9380001000042' then 'Renovacion'
        when c.cod_producto = '9380001000046' then 'Renovacion'
        when c.cod_producto = '9380002000009' then 'Renovacion'
        when c.cod_producto = '9380002000045' then 'Renovacion'
        when c.cod_producto = '9410001000130' then 'Renovacion'
        when c.cod_producto = '9410001000131' then 'Renovacion'
        when c.cod_producto = '9900001000127' then 'Renovacion'
        when c.cod_producto = '9410001000166' then 'Upgrade'
        when c.cod_producto = '9410001000136' then 'Upgrade'
        when c.cod_producto = '9410001000133' then 'Upgrade'
        when c.cod_producto = '9410001000124' then 'Upgrade'
        when c.cod_producto = '9410001000190' then 'Upgrade'
        when c.cod_producto = '9410001000187' then 'Upgrade'
        when c.cod_producto = '9410001000123' then 'Upgrade'
        when c.cod_producto = '9410001000163' then 'Upgrade'
        when c.cod_producto = '9410001000139' then 'Upgrade'
        when c.cod_producto = '9410001000167' then 'Upgrade'
        when c.cod_producto = '9410001000135' then 'Upgrade'
        when c.cod_producto = '9410001000132' then 'Upgrade'
        when c.cod_producto = '9410001000126' then 'Upgrade'
        when c.cod_producto = '9410001000191' then 'Upgrade'
        when c.cod_producto = '9410001000188' then 'Upgrade'
        when c.cod_producto = '9410001000125' then 'Upgrade'
        when c.cod_producto = '9410001000162' then 'Upgrade'
        when c.cod_producto = '9410001000138' then 'Upgrade'
        when c.cod_producto = '9410001000165' then 'Upgrade'
        when c.cod_producto = '9410001000137' then 'Upgrade'
        when c.cod_producto = '9410001000134' then 'Upgrade'
        when c.cod_producto = '9410001000122' then 'Upgrade'
        when c.cod_producto = '9410001000189' then 'Upgrade'
        when c.cod_producto = '9410001000186' then 'Upgrade'
        when c.cod_producto = '9410001000121' then 'Upgrade'
        when c.cod_producto = '9410001000140' then 'Upgrade'
        when c.cod_producto = '9410001000164' then 'Upgrade'
        when c.cod_producto = '9410001000119' then 'Upgrade'
        when c.cod_producto = '9410001000116' then 'Upgrade'
        when c.cod_producto = '9410001000120' then 'Upgrade'
        when c.cod_producto = '9410001000144' then 'Upgrade'
        when c.cod_producto = '9410001000146' then 'Upgrade'
        when c.cod_producto = '9410001000145' then 'Upgrade'
        when c.cod_producto = '9410001000143' then 'Upgrade'
        when c.cod_producto = '9380002000069' then 'Upsell'
        when c.cod_producto = '9380002000050' then 'Upsell'
        when c.cod_producto = '9380002000108' then 'Upsell'
        when c.cod_producto = '9380002000051' then 'Upsell'
        when c.cod_producto = '9380002000071' then 'Upsell'
        when c.cod_producto = '9380002000065' then 'Upsell'
        when c.cod_producto = '9380002000037' then 'Upsell'
        when c.cod_producto = '9380002000114' then 'Upsell'
        when c.cod_producto = '9380002000110' then 'Upsell'
        when c.cod_producto = '9380002000039' then 'Upsell'
        when c.cod_producto = '9380002000067' then 'Upsell'
        when c.cod_producto = '9380002000136' then 'Upsell'
        when c.cod_producto = '9380002000068' then 'Upsell'
        when c.cod_producto = '9380002000063' then 'Upsell'
        when c.cod_producto = '9380002000036' then 'Upsell'
        when c.cod_producto = '9380002000038' then 'Upsell'
        when c.cod_producto = '9380002000134' then 'Upsell'
        when c.cod_producto = '9380002000112' then 'Upsell'
        when c.cod_producto = '9380002000107' then 'Upsell'
        when c.cod_producto = '9380002000113' then 'Upsell'
        when c.cod_producto = '9380002000109' then 'Upsell'
        when c.cod_producto = '9380002000111' then 'Upsell'
        when c.cod_producto = '9380002000135' then 'Upsell'
        when c.cod_producto = '9380002000106' then 'Upsell'
        when c.cod_producto = '9380002000035' then 'Upsell'
        when c.cod_producto = '9380002000070' then 'Upsell'
        when c.cod_producto = '9380002000064' then 'Upsell'
        when c.cod_producto = '9380002000133' then 'Upsell'
        when c.cod_producto = '9380002000061' then 'Upsell'
        when c.cod_producto = '9380002000066' then 'Upsell'
        when c.cod_producto = '9380002000129' then 'Upsell'
        when c.cod_producto = '9380002000119' then 'Upsell'
        when c.cod_producto = '9380002000062' then 'Upsell'
        when c.cod_producto = '9380002000137' then 'Upsell'
        when c.cod_producto = '9380002000072' then 'Upsell'
        when c.cod_producto = '9380002000161' then 'Upsell'
        when c.cod_producto = '9380002000131' then 'Upsell'
        when c.cod_producto = '9380002000146' then 'Upsell'
        when c.cod_producto = '9380002000040' then 'Upsell'
        when c.cod_producto = '9700001000334' then 'Upsell'
        when c.cod_producto = '20001000059' then 'Upsell'
    end tipo_operacion
    , date(f.fec_creacion) fecha
    , 1 pago
    , cast((c.precio_venta - c.valor)AS INTEGER) descuento

    FROM db_comercial.vw_slv_facturacion f
    left join db_comercial.vw_slv_cotizaciones c
        on f.COTIZACION_NO = c.COTIZACION_NO
    left join (select id_facturacion from db_finanzas.tbl_slv_maestra_nc_anulaciones where not tipo_nc_total_parcial like '%PARCIAL%') nc
        on nc.id_facturacion  = f.id_facturacion
            
    where 
    c.cod_producto in (
        '20001000059',
        '9380001000038',
        '9380001000039',
        '9380001000040',
        '9380001000041',
        '9380001000042',
        '9380001000043',
        '9380001000044',
        '9380001000045',
        '9380001000046',
        '9380002000009',
        '9380002000012',
        '9380002000013',
        '9380002000033',
        '9380002000034',
        '9380002000035',
        '9380002000036',
        '9380002000037',
        '9380002000038',
        '9380002000039',
        '9380002000040',
        '9380002000045',
        '9380002000046',
        '9380002000047',
        '9380002000048',
        '9380002000050',
        '9380002000051',
        '9380002000061',
        '9380002000062',
        '9380002000063',
        '9380002000064',
        '9380002000065',
        '9380002000066',
        '9380002000067',
        '9380002000068',
        '9380002000069',
        '9380002000070',
        '9380002000071',
        '9380002000072',
        '9380002000094',
        '9380002000095',
        '9380002000096',
        '9380002000097',
        '9380002000098',
        '9380002000099',
        '9380002000100',
        '9380002000101',
        '9380002000104',
        '9380002000106',
        '9380002000107',
        '9380002000108',
        '9380002000109',
        '9380002000110',
        '9380002000111',
        '9380002000112',
        '9380002000113',
        '9380002000114',
        '9380002000115',
        '9380002000116',
        '9380002000117',
        '9380002000118',
        '9380002000119',
        '9380002000120',
        '9380002000121',
        '9380002000122',
        '9380002000123',
        '9380002000124',
        '9380002000125',
        '9380002000126',
        '9380002000127',
        '9380002000128',
        '9380002000129',
        '9380002000130',
        '9380002000131',
        '9380002000132',
        '9380002000133',
        '9380002000134',
        '9380002000135',
        '9380002000136',
        '9380002000137',
        '9380002000138',
        '9380002000139',
        '9380002000140',
        '9380002000141',
        '9380002000142',
        '9380002000143',
        '9380002000144',
        '9380002000145',
        '9380002000146',
        '9380002000161',
        '9380002000162',
        '9380003000009',
        '9380003000012',
        '9380003000013',
        '9380003000033',
        '9380003000034',
        '9380003000045',
        '9380003000046',
        '9380003000047',
        '9380003000048',
        '9380003000104',
        '9410001000116',
        '9410001000119',
        '9410001000120',
        '9410001000121',
        '9410001000122',
        '9410001000123',
        '9410001000124',
        '9410001000125',
        '9410001000126',
        '9410001000130',
        '9410001000131',
        '9410001000132',
        '9410001000133',
        '9410001000134',
        '9410001000135',
        '9410001000136',
        '9410001000137',
        '9410001000138',
        '9410001000139',
        '9410001000140',
        '9410001000143',
        '9410001000144',
        '9410001000145',
        '9410001000146',
        '9410001000162',
        '9410001000163',
        '9410001000164',
        '9410001000165',
        '9410001000166',
        '9410001000167',
        '9410001000186',
        '9410001000187',
        '9410001000188',
        '9410001000189',
        '9410001000190',
        '9410001000191',
        '9490001000007',
        '9490001000008',
        '9490001000009',
        '9700001000334',
        '9900001000127',
        '9380002000001',
        '9380002000041',
        '9380002000042',
        '9380002000049',
        '9380002000103',
        '9380002000105',
        '9490001000011'
    )
    and c.valor > 1
    and date(f.fec_creacion)  >= '2022-12-09'
    and nc.id_facturacion is null
    and f.id_autor != 1
)

, tbl_brz_customer_fe_facturacion_notas_credito as (
  SELECT REPLACE(TRIM(NRO_FACTURA),'-','') Factura
        ,ID_NOTA_CREDITO
        ,VALOR_PRODUCTO
        ,TIPO_NC
    FROM db_finanzas.vw_slv_notas_credito_modifica FD
    WHERE 1=1
        AND NOT LEFT(REPLACE(TRIM(NOTA_CREDITO_NO),'-',''),4) = 'J105'
    UNION ALL
    SELECT REPLACE(TRIM(NRO_FACTURA),'-','') Factura
        ,ID_NOTA_CREDITO
        ,VALOR_PRODUCTO
        ,TIPO_NC
    FROM db_finanzas.vw_slv_notas_credito_no_modifica FD
    WHERE 1=1
        and MOTIVO != 'Descuento'
        AND NOT LEFT(REPLACE(TRIM(NOTA_CREDITO_NO),'-',''),4) = 'J105'
)

, tbl_brz_customer_fe_facturacion_descuentos as (
  SELECT 
    REPLACE(TRIM(NRO_FACTURA),'-','') Factura
    ,ID_NOTA_CREDITO
    ,VALOR_PRODUCTO valor
    ,TIPO_NC
  FROM db_finanzas.vw_slv_notas_credito_no_modifica FD
  WHERE 1=1
      and MOTIVO = 'Descuento'
      AND NOT LEFT(REPLACE(TRIM(NOTA_CREDITO_NO),'-',''),4) = 'J105'
)

, tbl_brz_customer_fe_facturacion_pagos as (
  select
        distinct REPLACE(TRIM(b.subject), '-', '') consecutivo
    from
    db_synergy.tbl_brz_absences ab
    left join db_synergy.tbl_brz_bacodiscussions b on UPPER(ab.freeGuidField_01) = UPPER(b.ID)
    where
    ab.type = 4017
    and date(ab.syscreated) >= '2022-12-09'
    UNION
    select
        distinct REPLACE(TRIM(b.subject), '-', '') consecutivo
    from
    db_synergy.tbl_brz_absences ab
    left join db_synergy.tbl_brz_bacodiscussions b on UPPER(ab.freeGuidField_01) = UPPER(b.ID)
    where
    ab.type = 4063
    and date(ab.syscreated) >= '2022-12-09'
)

, tbl_brz_customer_fe_facturas_netsuite as (
  select  
        '' serial
        , trim(d.nit) nit
        , trim(d.sucursal) sucursal
        , 'F95' tipo_factura
        , d.producto cod_producto
        , pr.producto producto
        , cast(d.valor AS INTEGER) facturado 
        , d.nro_factura consecutivo
        , 'Renovacion' tipo_operacion
        , date(concat(left(d.anno_mes,4),'-',d.mes,'-',d.day)) fecha
        , case when not p.consecutivo is null then 1 else 0 end  pago 
        , cast(de.valor AS INTEGER) descuento

    from db_finanzas.tbl_slv_daily d
    left join db_params.tbl_brz_params_productos pr
        on d.producto = pr.codigo_producto
    left join tbl_brz_customer_fe_facturacion_notas_credito nc 
        on d.nro_factura = nc.factura
    left join tbl_brz_customer_fe_facturacion_pagos p
        on p.consecutivo = d.nro_factura
    left join tbl_brz_customer_fe_facturacion_descuentos de
        on de.factura = d.nro_factura
    where d.Comprobante = '95'
    and date(concat(left(d.anno_mes,4),'-',d.mes,'-',d.day))  >= '2022-12-09'
    and d.producto in (
        '20001000059',
        '9380001000038',
        '9380001000039',
        '9380001000040',
        '9380001000041',
        '9380001000042',
        '9380001000043',
        '9380001000044',
        '9380001000045',
        '9380001000046',
        '9380002000009',
        '9380002000012',
        '9380002000013',
        '9380002000033',
        '9380002000034',
        '9380002000035',
        '9380002000036',
        '9380002000037',
        '9380002000038',
        '9380002000039',
        '9380002000040',
        '9380002000045',
        '9380002000046',
        '9380002000047',
        '9380002000048',
        '9380002000050',
        '9380002000051',
        '9380002000061',
        '9380002000062',
        '9380002000063',
        '9380002000064',
        '9380002000065',
        '9380002000066',
        '9380002000067',
        '9380002000068',
        '9380002000069',
        '9380002000070',
        '9380002000071',
        '9380002000072',
        '9380002000094',
        '9380002000095',
        '9380002000096',
        '9380002000097',
        '9380002000098',
        '9380002000099',
        '9380002000100',
        '9380002000101',
        '9380002000104',
        '9380002000106',
        '9380002000107',
        '9380002000108',
        '9380002000109',
        '9380002000110',
        '9380002000111',
        '9380002000112',
        '9380002000113',
        '9380002000114',
        '9380002000115',
        '9380002000116',
        '9380002000117',
        '9380002000118',
        '9380002000119',
        '9380002000120',
        '9380002000121',
        '9380002000122',
        '9380002000123',
        '9380002000124',
        '9380002000125',
        '9380002000126',
        '9380002000127',
        '9380002000128',
        '9380002000129',
        '9380002000130',
        '9380002000131',
        '9380002000132',
        '9380002000133',
        '9380002000134',
        '9380002000135',
        '9380002000136',
        '9380002000137',
        '9380002000138',
        '9380002000139',
        '9380002000140',
        '9380002000141',
        '9380002000142',
        '9380002000143',
        '9380002000144',
        '9380002000145',
        '9380002000146',
        '9380002000161',
        '9380002000162',
        '9380003000009',
        '9380003000012',
        '9380003000013',
        '9380003000033',
        '9380003000034',
        '9380003000045',
        '9380003000046',
        '9380003000047',
        '9380003000048',
        '9380003000104',
        '9410001000116',
        '9410001000119',
        '9410001000120',
        '9410001000121',
        '9410001000122',
        '9410001000123',
        '9410001000124',
        '9410001000125',
        '9410001000126',
        '9410001000130',
        '9410001000131',
        '9410001000132',
        '9410001000133',
        '9410001000134',
        '9410001000135',
        '9410001000136',
        '9410001000137',
        '9410001000138',
        '9410001000139',
        '9410001000140',
        '9410001000143',
        '9410001000144',
        '9410001000145',
        '9410001000146',
        '9410001000162',
        '9410001000163',
        '9410001000164',
        '9410001000165',
        '9410001000166',
        '9410001000167',
        '9410001000186',
        '9410001000187',
        '9410001000188',
        '9410001000189',
        '9410001000190',
        '9410001000191',
        '9490001000007',
        '9490001000008',
        '9490001000009',
        '9700001000334',
        '9900001000127',
        '9380002000001',
        '9380002000041',
        '9380002000042',
        '9380002000049',
        '9380002000103',
        '9380002000105',
        '9490001000011'
    )
    and nc.factura is null
    union all 
    select '' serial
            , trim(fd.nit) nit
            , trim(fd.sucursal) sucursal
            , 'F95' tipo_factura
            , fd.producto cod_producto
            , pr2.producto producto
            , cast(fd.valor AS INTEGER) facturado
            , fd.nro_factura consecutivo
            , 'Renovacion' tipo_operacion
            , date(concat(left(fd.anno_mes,4),'-',fd.mes,'-',fd.day)) fecha
            , case when not p2.consecutivo is null then 1 else 0 end pago 
            , cast(de2.valor AS INTEGER) descuento
    from db_finanzas.tbl_slv_daily fd
    left join db_params.tbl_brz_params_productos pr2
        on fd.producto = pr2.codigo_producto
    inner JOIN tbl_brz_customer_fe_facturacion_clientes_renovacion r
        ON fd.NIT = r.nit and fd.sucursal = r.sucursal
    left join tbl_brz_customer_fe_facturacion_notas_credito nc2
        on fd.nro_factura = nc2.factura
    left join tbl_brz_customer_fe_facturacion_pagos p2
        on p2.consecutivo = fd.nro_factura
    left join tbl_brz_customer_fe_facturacion_descuentos de2
        on de2.factura = fd.nro_factura
    where fd.Comprobante = '95'
    and date(concat(left(fd.anno_mes,4),'-',fd.mes,'-',fd.day))  >= '2022-12-09'
    and fd.producto is null
    and nc2.factura is null
)

, tbl_brz_customer_fe_facturacion_cotizaciones as (
  select 
  trim(c.serial) serial
  , trim(c.identificacion)  nit
  , ''  sucursal
  , 'F93-C' tipo_factura
  , trim(c.cod_producto) cod_producto
  , trim(c.producto)  producto
  , cast(c.valor AS INTEGER) facturado
  , c.consecutivo
  , case
    when c.cod_producto = '9380002000100' then 'Downsell'
    when c.cod_producto = '9380002000101' then 'Downsell'
    when c.cod_producto = '9380002000128' then 'Downsell'
    when c.cod_producto = '9380002000141' then 'Downsell'
    when c.cod_producto = '9380002000145' then 'Downsell'
    when c.cod_producto = '9380002000125' then 'Downsell'
    when c.cod_producto = '9380002000127' then 'Downsell'
    when c.cod_producto = '9380002000117' then 'Downsell'
    when c.cod_producto = '9380002000126' then 'Downsell'
    when c.cod_producto = '9380002000118' then 'Downsell'
    when c.cod_producto = '9380002000094' then 'Downsell'
    when c.cod_producto = '9380002000140' then 'Downsell'
    when c.cod_producto = '9380002000143' then 'Downsell'
    when c.cod_producto = '9380002000130' then 'Downsell'
    when c.cod_producto = '9380002000162' then 'Downsell'
    when c.cod_producto = '9380002000121' then 'Downsell'
    when c.cod_producto = '9380002000124' then 'Downsell'
    when c.cod_producto = '9380002000122' then 'Downsell'
    when c.cod_producto = '9380002000123' then 'Downsell'
    when c.cod_producto = '9380002000120' then 'Downsell'
    when c.cod_producto = '9380002000142' then 'Downsell'
    when c.cod_producto = '9380002000144' then 'Downsell'
    when c.cod_producto = '9490001000009' then 'Downsell'
    when c.cod_producto = '9490001000008' then 'Downsell'
    when c.cod_producto = '9380002000132' then 'Downsell'
    when c.cod_producto = '9490001000007' then 'Downsell'
    when c.cod_producto = '9380002000095' then 'Renovacion'
    when c.cod_producto = '9380002000096' then 'Renovacion'
    when c.cod_producto = '9380002000097' then 'Renovacion'
    when c.cod_producto = '9380002000115' then 'Renovacion'
    when c.cod_producto = '9380002000116' then 'Renovacion'
    when c.cod_producto = '9380002000099' then 'Renovacion'
    when c.cod_producto = '9380002000098' then 'Renovacion'
    when c.cod_producto = '9380002000047' then 'Renovacion'
    when c.cod_producto = '9380002000034' then 'Renovacion'
    when c.cod_producto = '9380002000046' then 'Renovacion'
    when c.cod_producto = '9380002000139' then 'Renovacion'
    when c.cod_producto = '9380002000033' then 'Renovacion'
    when c.cod_producto = '9380002000012' then 'Renovacion'
    when c.cod_producto = '9380002000048' then 'Renovacion'
    when c.cod_producto = '9380002000138' then 'Renovacion'
    when c.cod_producto = '9380002000104' then 'Renovacion'
    when c.cod_producto = '9380002000013' then 'Renovacion'
    when c.cod_producto = '9380001000044' then 'Renovacion'
    when c.cod_producto = '9380001000039' then 'Renovacion'
    when c.cod_producto = '9380001000038' then 'Renovacion'
    when c.cod_producto = '9380001000043' then 'Renovacion'
    when c.cod_producto = '9380001000041' then 'Renovacion'
    when c.cod_producto = '9380001000040' then 'Renovacion'
    when c.cod_producto = '9380001000045' then 'Renovacion'
    when c.cod_producto = '9380001000042' then 'Renovacion'
    when c.cod_producto = '9380001000046' then 'Renovacion'
    when c.cod_producto = '9380002000009' then 'Renovacion'
    when c.cod_producto = '9380002000045' then 'Renovacion'
    when c.cod_producto = '9410001000130' then 'Renovacion'
    when c.cod_producto = '9410001000131' then 'Renovacion'
    when c.cod_producto = '9900001000127' then 'Renovacion'
    when c.cod_producto = '9410001000166' then 'Upgrade'
    when c.cod_producto = '9410001000136' then 'Upgrade'
    when c.cod_producto = '9410001000133' then 'Upgrade'
    when c.cod_producto = '9410001000124' then 'Upgrade'
    when c.cod_producto = '9410001000190' then 'Upgrade'
    when c.cod_producto = '9410001000187' then 'Upgrade'
    when c.cod_producto = '9410001000123' then 'Upgrade'
    when c.cod_producto = '9410001000163' then 'Upgrade'
    when c.cod_producto = '9410001000139' then 'Upgrade'
    when c.cod_producto = '9410001000167' then 'Upgrade'
    when c.cod_producto = '9410001000135' then 'Upgrade'
    when c.cod_producto = '9410001000132' then 'Upgrade'
    when c.cod_producto = '9410001000126' then 'Upgrade'
    when c.cod_producto = '9410001000191' then 'Upgrade'
    when c.cod_producto = '9410001000188' then 'Upgrade'
    when c.cod_producto = '9410001000125' then 'Upgrade'
    when c.cod_producto = '9410001000162' then 'Upgrade'
    when c.cod_producto = '9410001000138' then 'Upgrade'
    when c.cod_producto = '9410001000165' then 'Upgrade'
    when c.cod_producto = '9410001000137' then 'Upgrade'
    when c.cod_producto = '9410001000134' then 'Upgrade'
    when c.cod_producto = '9410001000122' then 'Upgrade'
    when c.cod_producto = '9410001000189' then 'Upgrade'
    when c.cod_producto = '9410001000186' then 'Upgrade'
    when c.cod_producto = '9410001000121' then 'Upgrade'
    when c.cod_producto = '9410001000140' then 'Upgrade'
    when c.cod_producto = '9410001000164' then 'Upgrade'
    when c.cod_producto = '9410001000119' then 'Upgrade'
    when c.cod_producto = '9410001000116' then 'Upgrade'
    when c.cod_producto = '9410001000120' then 'Upgrade'
    when c.cod_producto = '9410001000144' then 'Upgrade'
    when c.cod_producto = '9410001000146' then 'Upgrade'
    when c.cod_producto = '9410001000145' then 'Upgrade'
    when c.cod_producto = '9410001000143' then 'Upgrade'
    when c.cod_producto = '9380002000069' then 'Upsell'
    when c.cod_producto = '9380002000050' then 'Upsell'
    when c.cod_producto = '9380002000108' then 'Upsell'
    when c.cod_producto = '9380002000051' then 'Upsell'
    when c.cod_producto = '9380002000071' then 'Upsell'
    when c.cod_producto = '9380002000065' then 'Upsell'
    when c.cod_producto = '9380002000037' then 'Upsell'
    when c.cod_producto = '9380002000114' then 'Upsell'
    when c.cod_producto = '9380002000110' then 'Upsell'
    when c.cod_producto = '9380002000039' then 'Upsell'
    when c.cod_producto = '9380002000067' then 'Upsell'
    when c.cod_producto = '9380002000136' then 'Upsell'
    when c.cod_producto = '9380002000068' then 'Upsell'
    when c.cod_producto = '9380002000063' then 'Upsell'
    when c.cod_producto = '9380002000036' then 'Upsell'
    when c.cod_producto = '9380002000038' then 'Upsell'
    when c.cod_producto = '9380002000134' then 'Upsell'
    when c.cod_producto = '9380002000112' then 'Upsell'
    when c.cod_producto = '9380002000107' then 'Upsell'
    when c.cod_producto = '9380002000113' then 'Upsell'
    when c.cod_producto = '9380002000109' then 'Upsell'
    when c.cod_producto = '9380002000111' then 'Upsell'
    when c.cod_producto = '9380002000135' then 'Upsell'
    when c.cod_producto = '9380002000106' then 'Upsell'
    when c.cod_producto = '9380002000035' then 'Upsell'
    when c.cod_producto = '9380002000070' then 'Upsell'
    when c.cod_producto = '9380002000064' then 'Upsell'
    when c.cod_producto = '9380002000133' then 'Upsell'
    when c.cod_producto = '9380002000061' then 'Upsell'
    when c.cod_producto = '9380002000066' then 'Upsell'
    when c.cod_producto = '9380002000129' then 'Upsell'
    when c.cod_producto = '9380002000119' then 'Upsell'
    when c.cod_producto = '9380002000062' then 'Upsell'
    when c.cod_producto = '9380002000137' then 'Upsell'
    when c.cod_producto = '9380002000072' then 'Upsell'
    when c.cod_producto = '9380002000161' then 'Upsell'
    when c.cod_producto = '9380002000131' then 'Upsell'
    when c.cod_producto = '9380002000146' then 'Upsell'
    when c.cod_producto = '9380002000040' then 'Upsell'
    when c.cod_producto = '9700001000334' then 'Upsell'
    when c.cod_producto = '20001000059' then 'Upsell'
  end tipo_operacion
  , date(c.fec_creacion) fecha
  , 1 pago
  , cast((c.precio_venta - c.valor) AS INTEGER) descuento

from (select *, concat('C',cotizacion_no) consecutivo from db_comercial.vw_slv_cotizaciones) c
inner join tbl_brz_customer_fe_facturacion_pagos p on c.consecutivo = p.consecutivo
where 
trim(c.cod_producto) in (
    '20001000059',
    '9380001000038',
    '9380001000039',
    '9380001000040',
    '9380001000041',
    '9380001000042',
    '9380001000043',
    '9380001000044',
    '9380001000045',
    '9380001000046',
    '9380002000009',
    '9380002000012',
    '9380002000013',
    '9380002000033',
    '9380002000034',
    '9380002000035',
    '9380002000036',
    '9380002000037',
    '9380002000038',
    '9380002000039',
    '9380002000040',
    '9380002000045',
    '9380002000046',
    '9380002000047',
    '9380002000048',
    '9380002000050',
    '9380002000051',
    '9380002000061',
    '9380002000062',
    '9380002000063',
    '9380002000064',
    '9380002000065',
    '9380002000066',
    '9380002000067',
    '9380002000068',
    '9380002000069',
    '9380002000070',
    '9380002000071',
    '9380002000072',
    '9380002000094',
    '9380002000095',
    '9380002000096',
    '9380002000097',
    '9380002000098',
    '9380002000099',
    '9380002000100',
    '9380002000101',
    '9380002000104',
    '9380002000106',
    '9380002000107',
    '9380002000108',
    '9380002000109',
    '9380002000110',
    '9380002000111',
    '9380002000112',
    '9380002000113',
    '9380002000114',
    '9380002000115',
    '9380002000116',
    '9380002000117',
    '9380002000118',
    '9380002000119',
    '9380002000120',
    '9380002000121',
    '9380002000122',
    '9380002000123',
    '9380002000124',
    '9380002000125',
    '9380002000126',
    '9380002000127',
    '9380002000128',
    '9380002000129',
    '9380002000130',
    '9380002000131',
    '9380002000132',
    '9380002000133',
    '9380002000134',
    '9380002000135',
    '9380002000136',
    '9380002000137',
    '9380002000138',
    '9380002000139',
    '9380002000140',
    '9380002000141',
    '9380002000142',
    '9380002000143',
    '9380002000144',
    '9380002000145',
    '9380002000146',
    '9380002000161',
    '9380002000162',
    '9380003000009',
    '9380003000012',
    '9380003000013',
    '9380003000033',
    '9380003000034',
    '9380003000045',
    '9380003000046',
    '9380003000047',
    '9380003000048',
    '9380003000104',
    '9410001000116',
    '9410001000119',
    '9410001000120',
    '9410001000121',
    '9410001000122',
    '9410001000123',
    '9410001000124',
    '9410001000125',
    '9410001000126',
    '9410001000130',
    '9410001000131',
    '9410001000132',
    '9410001000133',
    '9410001000134',
    '9410001000135',
    '9410001000136',
    '9410001000137',
    '9410001000138',
    '9410001000139',
    '9410001000140',
    '9410001000143',
    '9410001000144',
    '9410001000145',
    '9410001000146',
    '9410001000162',
    '9410001000163',
    '9410001000164',
    '9410001000165',
    '9410001000166',
    '9410001000167',
    '9410001000186',
    '9410001000187',
    '9410001000188',
    '9410001000189',
    '9410001000190',
    '9410001000191',
    '9490001000007',
    '9490001000008',
    '9490001000009',
    '9700001000334',
    '9900001000127',
    '9380002000001',
    '9380002000041',
    '9380002000042',
    '9380002000049',
    '9380002000103',
    '9380002000105',
    '9490001000011'
)
and c.valor > 1
and date(c.fec_creacion)  >= '2022-12-09'
)

, tbl_brz_customer_fe_tienda as (
select 
    trim(serial) serial
    , '' nit
    , '' sucursal
    , 'F93'tipo_factura
    , cod_producto cod_producto
    , producto  producto
    , cast(valor_producto AS INTEGER) facturado
    , documento_factura consecutivo
    , case
        when cod_producto = '9380002000100' then 'Downsell'
        when cod_producto = '9380002000101' then 'Downsell'
        when cod_producto = '9380002000128' then 'Downsell'
        when cod_producto = '9380002000141' then 'Downsell'
        when cod_producto = '9380002000145' then 'Downsell'
        when cod_producto = '9380002000125' then 'Downsell'
        when cod_producto = '9380002000127' then 'Downsell'
        when cod_producto = '9380002000117' then 'Downsell'
        when cod_producto = '9380002000126' then 'Downsell'
        when cod_producto = '9380002000118' then 'Downsell'
        when cod_producto = '9380002000094' then 'Downsell'
        when cod_producto = '9380002000140' then 'Downsell'
        when cod_producto = '9380002000143' then 'Downsell'
        when cod_producto = '9380002000130' then 'Downsell'
        when cod_producto = '9380002000162' then 'Downsell'
        when cod_producto = '9380002000121' then 'Downsell'
        when cod_producto = '9380002000124' then 'Downsell'
        when cod_producto = '9380002000122' then 'Downsell'
        when cod_producto = '9380002000123' then 'Downsell'
        when cod_producto = '9380002000120' then 'Downsell'
        when cod_producto = '9380002000142' then 'Downsell'
        when cod_producto = '9380002000144' then 'Downsell'
        when cod_producto = '9490001000009' then 'Downsell'
        when cod_producto = '9490001000008' then 'Downsell'
        when cod_producto = '9380002000132' then 'Downsell'
        when cod_producto = '9490001000007' then 'Downsell'
        when cod_producto = '9380002000095' then 'Renovacion'
        when cod_producto = '9380002000096' then 'Renovacion'
        when cod_producto = '9380002000097' then 'Renovacion'
        when cod_producto = '9380002000115' then 'Renovacion'
        when cod_producto = '9380002000116' then 'Renovacion'
        when cod_producto = '9380002000099' then 'Renovacion'
        when cod_producto = '9380002000098' then 'Renovacion'
        when cod_producto = '9380002000047' then 'Renovacion'
        when cod_producto = '9380002000034' then 'Renovacion'
        when cod_producto = '9380002000046' then 'Renovacion'
        when cod_producto = '9380002000139' then 'Renovacion'
        when cod_producto = '9380002000033' then 'Renovacion'
        when cod_producto = '9380002000012' then 'Renovacion'
        when cod_producto = '9380002000048' then 'Renovacion'
        when cod_producto = '9380002000138' then 'Renovacion'
        when cod_producto = '9380002000104' then 'Renovacion'
        when cod_producto = '9380002000013' then 'Renovacion'
        when cod_producto = '9380001000044' then 'Renovacion'
        when cod_producto = '9380001000039' then 'Renovacion'
        when cod_producto = '9380001000038' then 'Renovacion'
        when cod_producto = '9380001000043' then 'Renovacion'
        when cod_producto = '9380001000041' then 'Renovacion'
        when cod_producto = '9380001000040' then 'Renovacion'
        when cod_producto = '9380001000045' then 'Renovacion'
        when cod_producto = '9380001000042' then 'Renovacion'
        when cod_producto = '9380001000046' then 'Renovacion'
        when cod_producto = '9380002000009' then 'Renovacion'
        when cod_producto = '9380002000045' then 'Renovacion'
        when cod_producto = '9410001000130' then 'Renovacion'
        when cod_producto = '9410001000131' then 'Renovacion'
        when cod_producto = '9900001000127' then 'Renovacion'
        when cod_producto = '9410001000166' then 'Upgrade'
        when cod_producto = '9410001000136' then 'Upgrade'
        when cod_producto = '9410001000133' then 'Upgrade'
        when cod_producto = '9410001000124' then 'Upgrade'
        when cod_producto = '9410001000190' then 'Upgrade'
        when cod_producto = '9410001000187' then 'Upgrade'
        when cod_producto = '9410001000123' then 'Upgrade'
        when cod_producto = '9410001000163' then 'Upgrade'
        when cod_producto = '9410001000139' then 'Upgrade'
        when cod_producto = '9410001000167' then 'Upgrade'
        when cod_producto = '9410001000135' then 'Upgrade'
        when cod_producto = '9410001000132' then 'Upgrade'
        when cod_producto = '9410001000126' then 'Upgrade'
        when cod_producto = '9410001000191' then 'Upgrade'
        when cod_producto = '9410001000188' then 'Upgrade'
        when cod_producto = '9410001000125' then 'Upgrade'
        when cod_producto = '9410001000162' then 'Upgrade'
        when cod_producto = '9410001000138' then 'Upgrade'
        when cod_producto = '9410001000165' then 'Upgrade'
        when cod_producto = '9410001000137' then 'Upgrade'
        when cod_producto = '9410001000134' then 'Upgrade'
        when cod_producto = '9410001000122' then 'Upgrade'
        when cod_producto = '9410001000189' then 'Upgrade'
        when cod_producto = '9410001000186' then 'Upgrade'
        when cod_producto = '9410001000121' then 'Upgrade'
        when cod_producto = '9410001000140' then 'Upgrade'
        when cod_producto = '9410001000164' then 'Upgrade'
        when cod_producto = '9410001000119' then 'Upgrade'
        when cod_producto = '9410001000116' then 'Upgrade'
        when cod_producto = '9410001000120' then 'Upgrade'
        when cod_producto = '9410001000144' then 'Upgrade'
        when cod_producto = '9410001000146' then 'Upgrade'
        when cod_producto = '9410001000145' then 'Upgrade'
        when cod_producto = '9410001000143' then 'Upgrade'
        when cod_producto = '9380002000069' then 'Upsell'
        when cod_producto = '9380002000050' then 'Upsell'
        when cod_producto = '9380002000108' then 'Upsell'
        when cod_producto = '9380002000051' then 'Upsell'
        when cod_producto = '9380002000071' then 'Upsell'
        when cod_producto = '9380002000065' then 'Upsell'
        when cod_producto = '9380002000037' then 'Upsell'
        when cod_producto = '9380002000114' then 'Upsell'
        when cod_producto = '9380002000110' then 'Upsell'
        when cod_producto = '9380002000039' then 'Upsell'
        when cod_producto = '9380002000067' then 'Upsell'
        when cod_producto = '9380002000136' then 'Upsell'
        when cod_producto = '9380002000068' then 'Upsell'
        when cod_producto = '9380002000063' then 'Upsell'
        when cod_producto = '9380002000036' then 'Upsell'
        when cod_producto = '9380002000038' then 'Upsell'
        when cod_producto = '9380002000134' then 'Upsell'
        when cod_producto = '9380002000112' then 'Upsell'
        when cod_producto = '9380002000107' then 'Upsell'
        when cod_producto = '9380002000113' then 'Upsell'
        when cod_producto = '9380002000109' then 'Upsell'
        when cod_producto = '9380002000111' then 'Upsell'
        when cod_producto = '9380002000135' then 'Upsell'
        when cod_producto = '9380002000106' then 'Upsell'
        when cod_producto = '9380002000035' then 'Upsell'
        when cod_producto = '9380002000070' then 'Upsell'
        when cod_producto = '9380002000064' then 'Upsell'
        when cod_producto = '9380002000133' then 'Upsell'
        when cod_producto = '9380002000061' then 'Upsell'
        when cod_producto = '9380002000066' then 'Upsell'
        when cod_producto = '9380002000129' then 'Upsell'
        when cod_producto = '9380002000119' then 'Upsell'
        when cod_producto = '9380002000062' then 'Upsell'
        when cod_producto = '9380002000137' then 'Upsell'
        when cod_producto = '9380002000072' then 'Upsell'
        when cod_producto = '9380002000161' then 'Upsell'
        when cod_producto = '9380002000131' then 'Upsell'
        when cod_producto = '9380002000146' then 'Upsell'
        when cod_producto = '9380002000040' then 'Upsell'
        when cod_producto = '9700001000334' then 'Upsell'
        when cod_producto = '20001000059' then 'Upsell'
    end tipo_operacion
    , Fecha_Facturacion fecha
    , 1 pago
    , 0 descuento

from 
    db_servicios.tbl_slv_facturacion_tienda_siigo
where 
Fecha_Facturacion >= '2022-12-09'
and cod_producto in (
    '20001000059',
    '9380001000038',
    '9380001000039',
    '9380001000040',
    '9380001000041',
    '9380001000042',
    '9380001000043',
    '9380001000044',
    '9380001000045',
    '9380001000046',
    '9380002000009',
    '9380002000012',
    '9380002000013',
    '9380002000033',
    '9380002000034',
    '9380002000035',
    '9380002000036',
    '9380002000037',
    '9380002000038',
    '9380002000039',
    '9380002000040',
    '9380002000045',
    '9380002000046',
    '9380002000047',
    '9380002000048',
    '9380002000050',
    '9380002000051',
    '9380002000061',
    '9380002000062',
    '9380002000063',
    '9380002000064',
    '9380002000065',
    '9380002000066',
    '9380002000067',
    '9380002000068',
    '9380002000069',
    '9380002000070',
    '9380002000071',
    '9380002000072',
    '9380002000094',
    '9380002000095',
    '9380002000096',
    '9380002000097',
    '9380002000098',
    '9380002000099',
    '9380002000100',
    '9380002000101',
    '9380002000104',
    '9380002000106',
    '9380002000107',
    '9380002000108',
    '9380002000109',
    '9380002000110',
    '9380002000111',
    '9380002000112',
    '9380002000113',
    '9380002000114',
    '9380002000115',
    '9380002000116',
    '9380002000117',
    '9380002000118',
    '9380002000119',
    '9380002000120',
    '9380002000121',
    '9380002000122',
    '9380002000123',
    '9380002000124',
    '9380002000125',
    '9380002000126',
    '9380002000127',
    '9380002000128',
    '9380002000129',
    '9380002000130',
    '9380002000131',
    '9380002000132',
    '9380002000133',
    '9380002000134',
    '9380002000135',
    '9380002000136',
    '9380002000137',
    '9380002000138',
    '9380002000139',
    '9380002000140',
    '9380002000141',
    '9380002000142',
    '9380002000143',
    '9380002000144',
    '9380002000145',
    '9380002000146',
    '9380002000161',
    '9380002000162',
    '9380003000009',
    '9380003000012',
    '9380003000013',
    '9380003000033',
    '9380003000034',
    '9380003000045',
    '9380003000046',
    '9380003000047',
    '9380003000048',
    '9380003000104',
    '9410001000116',
    '9410001000119',
    '9410001000120',
    '9410001000121',
    '9410001000122',
    '9410001000123',
    '9410001000124',
    '9410001000125',
    '9410001000126',
    '9410001000130',
    '9410001000131',
    '9410001000132',
    '9410001000133',
    '9410001000134',
    '9410001000135',
    '9410001000136',
    '9410001000137',
    '9410001000138',
    '9410001000139',
    '9410001000140',
    '9410001000143',
    '9410001000144',
    '9410001000145',
    '9410001000146',
    '9410001000162',
    '9410001000163',
    '9410001000164',
    '9410001000165',
    '9410001000166',
    '9410001000167',
    '9410001000186',
    '9410001000187',
    '9410001000188',
    '9410001000189',
    '9410001000190',
    '9410001000191',
    '9490001000007',
    '9490001000008',
    '9490001000009',
    '9700001000334',
    '9900001000127',
    '9380002000001',
    '9380002000041',
    '9380002000042',
    '9380002000049',
    '9380002000103',
    '9380002000105',
    '9490001000011'
)
)

, tbl_brz_customer_union_facturacion as (
select *
    , row_number() over(partition by concat(consecutivo,cod_producto,serial, nit, fecha) order by facturado desc ) row
from
(
    select * 
    from tbl_brz_customer_fe_facturacion_facturas_synergy
    union all 
    select * 
    from tbl_brz_customer_fe_facturas_netsuite
    union all 
    select * 
    from tbl_brz_customer_fe_tienda
    union all 
    select *
    from tbl_brz_customer_fe_facturacion_cotizaciones
) a
)

, tbl_brz_customer_detalle as (
select 
    cr.*
    , fe.estado_synergy
    , u.tipo_factura
    , u.cod_producto
    , u.producto
    , case when u.tipo_operacion in ('Upsell','Upgrade') then cr.valor_renovacion else u.facturado end facturado
    , u.consecutivo
    , u.fecha
    , u.tipo_operacion 
    , u.pago
    , u.descuento
    , row_number() over(partition by cr.serial order by u.fecha) row_pago
    
from 
tbl_brz_customer_fe_facturacion_clientes_renovacion cr
left join (select distinct serial, estado_synergy FROM db_servicios.tbl_slv_fe_activos) fe
    on cr.serial = fe.serial
left join tbl_brz_customer_union_facturacion u
    on u.serial = cr.serial
    or (cr.nit = u.nit and cr.sucursal = u.sucursal)
    or (cr.nit = u.nit)

where 
u.row = 1
and (
    pago = 1
    or fe.estado_synergy = 'Active'
)
)

, tbl_brz_customer_fe_acumulado as (
select 
    serial
    , nit
    , sucursal
    , debtor_code
    , producto_renovacion
    , cohorte_renovacion
    , valor_renovacion
    , tipo
    , antiguedad
    , estado_synergy
    , cast(u.ampliacion_vigencia as INTEGER) ampliacion_vigencia
    , count(case when tipo = 'PPTO 2022' and row_pago = 1 then serial else null end) cant_pagos_2022
    , sum(case when tipo = 'PPTO 2022' and row_pago = 1 then facturado else 0 end) pagos_2022
    , count(case when tipo = 'PPTO 2022' and row_pago > 1 then serial when tipo = 'PPTO 2023' then serial else null end) cant_pagos_2023
    , sum(case when tipo = 'PPTO 2022' and row_pago > 1 then facturado when tipo = 'PPTO 2023' then facturado else 0 end) pagos_2023
    , count(case when tipo = 'PPTO 2022' and row_pago > 1 and u.tipo_operacion = 'Upgrade' then serial when tipo = 'PPTO 2023' and u.tipo_operacion = 'Upgrade' then serial else null end) cant_pagos_2023_upgrade
    , sum(case when tipo = 'PPTO 2022' and row_pago > 1 and u.tipo_operacion = 'Upgrade'  then facturado when tipo = 'PPTO 2023' and u.tipo_operacion = 'Upgrade'  then facturado else 0 end) pagos_2023_upgrade
    , count(case when tipo = 'PPTO 2022' and row_pago > 1 and u.tipo_operacion in ('Upsell','Downsell') then serial when tipo = 'PPTO 2023' and u.tipo_operacion in ('Upsell','Downsell') then serial else null end) cant_pagos_2023_diferente
    , sum(case when tipo = 'PPTO 2022' and row_pago > 1 and u.tipo_operacion in ('Upsell','Downsell') then facturado when tipo = 'PPTO 2023' and u.tipo_operacion  in ('Upsell','Downsell') then facturado else 0 end) pagos_2023_diferente
    , count(case when tipo = 'PPTO 2022' and row_pago > 1 and u.tipo_operacion = 'Renovacion' then serial when tipo = 'PPTO 2023' and u.tipo_operacion = 'Renovacion' then serial else null end) cant_pagos_2023_renov
    , sum(case when tipo = 'PPTO 2022' and row_pago > 1 and u.tipo_operacion = 'Renovacion' then facturado when tipo = 'PPTO 2023' and u.tipo_operacion = 'Renovacion' then facturado else 0 end) pagos_2023_renov

from tbl_brz_customer_detalle u

group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)

, tbl_slv_fe_facturacion as (
select 
    *
    , _2023 - (_2023_Up + _2023_Df)  _2023_Rn
    , Pagos_2023 - (Pagos_2023_Up + Pagos_2023_Df) Pagos_2023_Rn
from (
    select 
        serial
        , nit
        , sucursal
        , debtor_code
        , producto_renovacion
        , cohorte_renovacion
        , valor_renovacion
        , tipo
        , antiguedad
        , estado_synergy
        , ampliacion_vigencia
        , case when coalesce(cant_pagos_2022,0) >= 1 then 1 else 0 end _2022        
        , case when coalesce(pagos_2022,0) > valor_renovacion then valor_renovacion else pagos_2022 end Pagos_2022

        , case 
            when coalesce(cant_pagos_2023,0) >= 1 then 1 
            when estado_synergy = 'Active' and dateadd(date(cohorte_renovacion),(ampliacion_vigencia * 30)) <= dateadd(date(now()), - 20) then 1
        else 0 end _2023
        , case 
            when coalesce(pagos_2023,0) > valor_renovacion then valor_renovacion 
            when coalesce(pagos_2023,0) = 0 and  estado_synergy = 'Active' and dateadd(date(cohorte_renovacion),(ampliacion_vigencia * 30)) <= dateadd(date(now()), - 20) then valor_renovacion
        else pagos_2023 end Pagos_2023

        , case when coalesce(cant_pagos_2023_upgrade,0) >= 1 then 1 else 0 end _2023_Up
        , case when coalesce(pagos_2023_upgrade,0) > valor_renovacion then valor_renovacion else pagos_2023_upgrade end Pagos_2023_Up

        , case when coalesce(cant_pagos_2023_upgrade,0) >= 1 then 0 when coalesce(cant_pagos_2023_diferente,0) >= 1 then 1 else 0 end _2023_Df
        , case when coalesce(pagos_2023_upgrade,0) > valor_renovacion then 0 when coalesce(pagos_2023_upgrade,0) + coalesce(pagos_2023_diferente,0) > valor_renovacion then valor_renovacion - coalesce(pagos_2023_upgrade,0) else pagos_2023_diferente end Pagos_2023_Df

    from tbl_brz_customer_fe_acumulado
) a
)

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
  left join tbl_slv_fe_facturacion f
    on trim(b.serial) = trim(f.serial)
) a
group by 1, 2, 3, 4, 5