select distinct 
c.CloudTenantCompanyKey
, c.Serial
, c.Nit
, c.TenantNit
, c.CompanyFullName
, ac.*
, erpdt.*
, error.*
, pt.*

from 
db_nubemultitenant.tbl_brz_acentry ac
left join db_nubemultitenant.tbl_brz_erpdocumenttype erpdt
  on erpdt.ERPDocumentTypeID = ac.ERPDocumentTypeCode
  and erpdt.CloudMultiTenantName = ac.CloudMultiTenantName
  and erpdt.TenantId = ac.TenantId
left join db_cloudcontrol.tbl_brz_cloudmultitenant mt
on mt.CloudMultiTenantName = erpdt.CloudMultiTenantName
LEFT JOIN db_cloudcontrol.tbl_brz_cloudtenant C 
  ON erpdt.TenantID = C.TenantID 
  and mt.cloudMultitenantId = C.cloudmultitenantcode
left join (
  select
      pe.CompanyKey
      , concat(pe.ErpPrefix,'-',pe.Consecutive) consecutivo
      , pe.DocumentState_Name
      , pe.DocumentState__id
      , pe.IsValid
      , pee.ErrorCode
      , pee.ErrorMessage
      , from_unixtime(PE.IssueDate_date / 1000 + 8 * 60 * 60, 'yyyy-MM-dd') Fecha
  from db_femultitenant.tbl_brz_posentry pe 
  INNER JOIN db_femultitenant.tbl_brz_posentryerror pee on pe.oid = pee.oid
  where IsValid = false and pe.ProductType = 1 and pee.ErrorMessage not like '%Notificación%'
) error on c.CloudTenantCompanyKey = error.CompanyKey and error.consecutivo = ac.DocName
left join (
  select distinct
        CompanyKey
        ,concat(pe.ErpPrefix,'-',pe.Consecutive) consecutivo
        , pe.DocumentState_Name
        , pe.IsValid
  from db_femultitenant.tbl_brz_posentry pe
) pt
on c.CloudTenantCompanyKey = pt.CompanyKey and pt.consecutivo = ac.DocName
where 
ac.DocClass = 'FV'
and erpdt.ElectronicInvoiceType in (10,11)
and date(ac.CreatedByDate) >= '2023-05-01'
and (not ac.TerminalPOSGUID is null)
and ac.CUFE is null
and (not erpdt.DIANResolution is null)

