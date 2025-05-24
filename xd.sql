with under_prep_agg as (
select a.src_instrument_id, b.reference 
from polar.dim_underlyings a 
left join polar.dim_instrument b on a.src_underlying_id=b.src_instrument_id where valid_to is null),

under_agg as (select src_instrument_id, listagg(distinct reference,',' on overflow truncate) within group (order by reference) underlyings from under_prep_agg group by src_instrument_id),
scope as ( -- first define the scope of orders and products
    select /*+ materialize */
       b.anna_order_id,
       b.order_exclusion,
       b.client_contact_display,
       fct.dim_trade_date,
       round((fct.dim_trade_date-pro.issue_date)/30) trade_date_in_month,
       fct.dim_sc_product_id,
       iss.issuer_group_1,
       iss.issuer_group_2,
       scos.order_source,
       min(fct.fx_rate) fx_rate,
       sum(fct.sales_credit_amt_chf) sales_credit_amt_chf,
       sum(fct.turnover_split_sum_chf) turnover_amt_chf,
       sum(total_nominal_units*tradecount_split) total_nominal_units_split_sum,
       min(total_nominal_units) total_nominal_units,
       pro.product_identifier sicovam,
       pro.investment_currency,
       pro.reporting_quotation_type,
       pro.isin,
       pro.issue_date,
       pro.issue_price,
       pro.redemption_date,
       pro.name,
       pro.fin_pt_level1,
       pro.fin_pt_level2,
       pro.fin_pt_level3,
       pro.product_status,
       odt.nisec,
       odt.buysell,
       sc_type.sc_type_group,
       dm_prod.sophis_reference,
       dm_prod.guarantor,
       fct.price_factor,
       cli.report_client_name,
       cli.repo_client_type,
       cli.crm_country client_country,
       sp.it_initials,
       sp.full_name,
       sp.location_name,
       sp.team_name,
       sp.team_head_it_initials,
       fct.sales_fee,
       fct.sales_correction,
       fct.business_type,
       fct.dim_sc_client_id,
       fct.ebo_price,
       tro.it_initials trd_owner_it_initials,
       tro.full_name trd_owner_full_name
       ,round(sum(turnover_split_sum_ccy),2)          turnover_ccy
       ,round(sum(turnover_split_sum_chf),2)          turnover_chf
       ,sum(case when dst.sc_type_group = 'Economic' then sales_credit_amt_ccy else 0 end)  total_economic_sc_ccy
       ,sum(case when dst.sc_type_group = 'Economic' then sales_credit_amt_chf else 0 end)  total_economic_sc_chf
       ,fct.creation_date                        
       ,odt.anna_order_type
       ,cli.CRM_LTQ_CLIENT_RISK_CLASSIFICATION
    from polar.fct_sc_salescredit fct
    inner join polar.dim_sc_order_id b
    on fct.dim_sc_order_id = b.id
    inner join polar.dim_sc_order_status st
    on st.dim_order_status_id = fct.dim_order_status_id
    inner join polar.dim_sc_sales_person sp
    on fct.dim_sc_recipient_id = sp.dim_sales_person_id and location_name = 'Milan'
    inner join polar.dim_sc_product pro
    on fct.dim_sc_product_id = pro.dim_sc_product_id
    inner join polar.dim_sc_sales_person tro
    on tro.dim_sales_person_id = fct.dim_trade_owner_id
    left join polar.dim_sc_issuer iss
    on iss.dim_sc_issuer_id=fct.dim_sc_issuer_id
    left join polar.dim_sc_order_type odt
    on odt.dim_order_type_id=fct.dim_order_type_id
    left join polar.dim_sc_type sc_type
    on sc_type.dim_sc_type_id=fct.dim_sc_type_id
    left join polar.dim_sc_product dm_prod
    on dm_prod.dim_sc_product_id=fct.dim_sc_product_id
    left join polar.dim_sc_client cli
    on fct.dim_sc_client_id=cli.dim_client_id
    left join polar.dim_sc_order_source scos
    on fct.dim_order_source_id=scos.dim_order_source_id
    left join polar.dim_sc_type dst
    on dst.dim_sc_type_id = fct.dim_sc_type_id
    where fct.valid_to is null
    and st.reporting_order_status = 'OK'
    and fct.dim_trade_date >= date '2025-01-01'
    and sc_type.SC_TYPE_GROUP <> 'External'
    and sc_type.SC_TYPE_GROUP <> 'External Client' 
    group by    b.anna_order_id,
                b.order_exclusion,
                b.client_contact_display,
                fct.dim_trade_date,
                fct.creation_date,                        
                odt.anna_order_type,
                cli.CRM_LTQ_CLIENT_RISK_CLASSIFICATION,
                round((fct.dim_trade_date-pro.issue_date)/30),
                fct.dim_sc_product_id,
                iss.issuer_group_1,
                iss.issuer_group_2,
                scos.order_source,
                pro.product_identifier,
                pro.investment_currency,
                pro.reporting_quotation_type,
                pro.isin,
                pro.fin_pt_level1,
                pro.fin_pt_level2,
                pro.fin_pt_level3,
                pro.product_status,
                pro.issue_date,
                pro.issue_price,
                pro.redemption_date,
                pro.name,
                odt.nisec,
                odt.buysell,
                sc_type.sc_type_group,
                dm_prod.sophis_reference,
                dm_prod.guarantor,
                fct.price_factor,
                cli.report_client_name,
                cli.repo_client_type,
                cli.crm_country,
                sp.it_initials,
                sp.full_name,
                sp.location_name,
                sp.team_name,
                sp.team_head_it_initials,
                fct.sales_fee,
                fct.sales_correction,
                fct.business_type,
                fct.dim_sc_client_id,
                fct.ebo_price,
                tro.it_initials,
                tro.full_name
)


,ext as (
    -- All completed product extracts since last full extract
    select *
    from incore.sta_extract
    where extract_type='PRODUCT'
      and comp=1
      and ext_id >= (select max(ext_id) from incore.sta_extract where extract_type = 'PRODUCT' and incremental=0 and comp=1)
)
,pro as (
select
    pro_p.ext_id
    ,pro_p.productid
    ,pro_p.sicovam
    ,pro_c.rate
    ,pro_p.listed
    ,pro_p.tcm
    ,pro_p.denomination
    ,pro_p.private_placement
    ,pro_p.early_redemption_exp_date
    ,pro_p.early_redemption_date
    ,pro_p.final_fixing_date
    ,pro_p.initial_fixing_date
    ,pro_p.redemption_date
    ,pro_p.first_trading_date
     -- For each productid we find max EXT_ID
    ,row_number() over (partition by pro_p.sicovam order by pro_p.ext_id desc) rn
from incore.pro_product pro_p
join incore.pro_gua_fix_rate_coupon pro_c on pro_p.productid = pro_c.productid and pro_p.ext_id=pro_c.ext_id
join incore.pro_product_market pro_m on pro_p.productid = pro_m.productid and pro_p.ext_id=pro_m.ext_id
where pro_p.ext_id in (select ext_id from ext) -- One product can exist in multiple extracts
)
, fixed_rate as (select *
from pro
where rn=1)

, distribution as (
select s.anna_order_id
          ,s.order_exclusion
          ,s.client_contact_display
          ,s.dim_trade_date
          ,s.sicovam
          ,s.isin
          ,s.issue_date
          ,s.redemption_date
          ,s.name
          ,s.trade_date_in_month
          ,s.issuer_group_1
          ,s.issuer_group_2
          ,s.order_source
          ,s.reporting_quotation_type
          ,s.investment_currency
          ,s.issue_price
          ,s.fx_rate
          ,s.sales_credit_amt_chf
          ,s.turnover_amt_chf
          ,s.total_nominal_units
          ,s.total_nominal_units_split_sum
          ,s.nisec
          ,s.buysell
          ,s.fin_pt_level1
          ,s.fin_pt_level2
          ,s.fin_pt_level3
          ,s.sc_type_group
          ,s.sophis_reference
          ,s.guarantor
          ,s.price_factor
          ,f.rate
          ,underl.underlyings
          ,case when trunc(s.dim_trade_date)=trunc(sysdate) then 'Today'
           when trunc(s.dim_trade_date)=trunc(sysdate-1) then 'Yesterday'
           when trunc(s.dim_trade_date)<trunc(sysdate-1) then 'Before'
           end today_yesterday
          ,s.product_status
          ,case when s.nisec='SEC' and s.buysell='LTQ BUY' then s.turnover_amt_chf else 0 end buy_back
          ,case when s.guarantor is null then s.issuer_group_2 else concat(concat(concat(s.issuer_group_2,'('),s.guarantor),')') end issuer_guarantor
          ,s.report_client_name
          ,s.repo_client_type
          ,s.client_country
          ,s.it_initials
          ,s.full_name
          ,s.location_name
          ,s.team_name
          ,s.team_head_it_initials
          ,s.sales_fee
          ,s.sales_correction
          ,s.business_type
          ,s.dim_sc_client_id
          ,s.ebo_price
          ,s.trd_owner_it_initials
          ,s.trd_owner_full_name
          ,f.listed
          ,f.tcm
          ,f.denomination
          ,f.private_placement
          ,f.early_redemption_exp_date
          ,f.early_redemption_date
          ,f.final_fixing_date
          ,f.initial_fixing_date
          ,f.first_trading_date
          ,s.CRM_LTQ_CLIENT_RISK_CLASSIFICATION
          ,s.turnover_ccy
          ,s.turnover_chf
          ,s.total_economic_sc_ccy
          ,s.total_economic_sc_chf
          ,s.creation_date                        
          ,s.anna_order_type
    from scope s
    left join under_agg underl
    on s.sicovam=to_char(underl.src_instrument_id)
    left join fixed_rate f
    on s.sicovam=to_char(f.sicovam))


    select
d.ANNA_ORDER_ID
,d.DIM_TRADE_DATE trade_date
,d.SICOVAM
,d.isin
,d.ORDER_SOURCE
,round(d.SALES_CREDIT_AMT_CHF,0) as SALES_CREDIT_AMT_CHF
,d.total_nominal_units
, round(d.TURNOVER_AMT_CHF,0) as TURNOVER_AMT_CHF
,d.NISEC
,d.BUYSELL
,d.BUSINESS_TYPE
,d.SC_TYPE_GROUP
,d.REPORTING_QUOTATION_TYPE
,case when d.REPORTING_QUOTATION_TYPE = 'IN_PERCENT' then d.PRICE_FACTOR*100 else d.PRICE_FACTOR end as Trade_price
,d.DIM_SC_CLIENT_ID
,d.CRM_LTQ_CLIENT_RISK_CLASSIFICATION
,case 
  when (d.REPORT_CLIENT_NAME is null and d.issuer_group_1 = 'OTC') then 'OTC' 
  else (case when d.REPORT_CLIENT_NAME is null then 'Retail' else d.REPORT_CLIENT_NAME end) end as REPORT_CLIENT_NAME
,d.CLIENT_CONTACT_DISPLAY
,d.IT_INITIALS
,d.FULL_NAME
,d.LOCATION_NAME
,d.TEAM_NAME
,d.TEAM_HEAD_IT_INITIALS
,d.listed
,d.tcm
,d.denomination
,d.private_placement
,d.early_redemption_exp_date
,d.early_redemption_date
,d.final_fixing_date
,d.initial_fixing_date
,d.redemption_date
,d.first_trading_date
,d.CRM_LTQ_CLIENT_RISK_CLASSIFICATION
,d.turnover_ccy
,d.turnover_chf
,d.total_economic_sc_ccy
,d.total_economic_sc_chf
,d.creation_date                        
,d.anna_order_type
,d.fx_rate
,d.INVESTMENT_CURRENCY
,adb.order_trade_date
from distribution d
left join incore.adb_orderscrecord adb on d.anna_order_id = adb.orderid