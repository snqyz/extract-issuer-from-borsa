-- Removed CTE: under_prep_agg
-- Removed CTE: under_agg

-- scope CTE:
--  - /*+ materialize */ hint is commented out for testing. Reinstate if beneficial.
--  - Joins to polar.dim_sc_type consolidated.
--  - Joins to polar.dim_sc_product consolidated (dm_prod removed, columns taken from pro).
WITH scope
     AS (SELECT
        --+ No_materialize */ /* Original: /*+ materialize */ - Commented out for testing, reinstate if it improves performance */
        b.anna_order_id,
        b.order_exclusion,
        b.client_contact_display,
        fct.dim_trade_date,
        Round(( fct.dim_trade_date - pro.issue_date ) / 30) trade_date_in_month,
        fct.dim_sc_product_id,
        iss.issuer_group_1,
        iss.issuer_group_2,
        scos.order_source,
        Min(fct.fx_rate)                                    fx_rate,
        SUM(fct.sales_credit_amt_chf)                       sales_credit_amt_chf
        ,
        SUM(fct.turnover_split_sum_chf)
        turnover_amt_chf,
        -- Renamed from original's implicit turnover_amt_chf to be clearer if needed
        SUM(total_nominal_units * tradecount_split)
        total_nominal_units_split_sum,
        Min(total_nominal_units)                            total_nominal_units,
        pro.product_identifier                              sicovam,
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
        sc_type.sc_type_group,-- From consolidated sc_type join
        pro.sophis_reference,-- MOVED: Originally from dm_prod, now from pro
        pro.guarantor,-- MOVED: Originally from dm_prod, now from pro
        fct.price_factor,
        cli.report_client_name,
        cli.repo_client_type,
        cli.crm_country                                     client_country,
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
        tro.it_initials
        trd_owner_it_initials,
        tro.full_name                                       trd_owner_full_name,
        Round(SUM(fct.turnover_split_sum_ccy), 2)           turnover_ccy
        -- Corrected alias if original had turnover_split_sum_ccy
        ,
        Round(SUM(fct.turnover_split_sum_chf), 2)           turnover_chf
        -- This is the one used as d.turnover_chf later
        ,
        SUM(CASE
              WHEN sc_type.sc_type_group = 'Economic' THEN
              fct.sales_credit_amt_ccy
              ELSE 0
            END)
        total_economic_sc_ccy
        -- CHANGED: dst.sc_type_group to sc_type.sc_type_group
        ,
        SUM(CASE
              WHEN sc_type.sc_type_group = 'Economic' THEN
              fct.sales_credit_amt_chf
              ELSE 0
            END)
        total_economic_sc_chf
        -- CHANGED: dst.sc_type_group to sc_type.sc_type_group
        ,
        fct.creation_date,
        odt.anna_order_type,
        cli.crm_ltq_client_risk_classification
         FROM   polar.fct_sc_salescredit fct
                inner join polar.dim_sc_order_id b
                        ON fct.dim_sc_order_id = b.id
                inner join polar.dim_sc_order_status st
                        ON st.dim_order_status_id = fct.dim_order_status_id
                inner join polar.dim_sc_sales_person sp
                        ON fct.dim_sc_recipient_id = sp.dim_sales_person_id
                           AND sp.location_name = 'Milan'
                -- Added sp. prefix for clarity
                inner join polar.dim_sc_product pro
                        ON fct.dim_sc_product_id = pro.dim_sc_product_id
                inner join polar.dim_sc_sales_person tro
                        ON tro.dim_sales_person_id = fct.dim_trade_owner_id
                left join polar.dim_sc_issuer iss
                       ON iss.dim_sc_issuer_id = fct.dim_sc_issuer_id
                left join polar.dim_sc_order_type odt
                       ON odt.dim_order_type_id = fct.dim_order_type_id
                left join polar.dim_sc_type sc_type
                       -- CONSOLIDATED: This is the single join for sc_type
                       ON sc_type.dim_sc_type_id = fct.dim_sc_type_id
                -- REMOVED: left join polar.dim_sc_product dm_prod on dm_prod.dim_sc_product_id=fct.dim_sc_product_id
                left join polar.dim_sc_client cli
                       ON fct.dim_sc_client_id = cli.dim_client_id
                left join polar.dim_sc_order_source scos
                       ON fct.dim_order_source_id = scos.dim_order_source_id
         -- REMOVED: left join polar.dim_sc_type dst on dst.dim_sc_type_id = fct.dim_sc_type_id
         WHERE  fct.valid_to IS NULL
                AND st.reporting_order_status = 'OK'
                AND fct.dim_trade_date >= DATE '2025-01-01'
                AND sc_type.sc_type_group <> 'External'
                AND sc_type.sc_type_group <> 'External Client'
         GROUP  BY b.anna_order_id,
                   b.order_exclusion,
                   b.client_contact_display,
                   fct.dim_trade_date,
                   fct.creation_date,
                   odt.anna_order_type,
                   cli.crm_ltq_client_risk_classification,
                   Round(( fct.dim_trade_date - pro.issue_date ) / 30),
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
                   pro.sophis_reference,
                   -- MOVED: Originally dm_prod.sophis_reference
                   pro.guarantor,-- MOVED: Originally dm_prod.guarantor
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
                   tro.full_name),
     ext
     AS (
        -- All completed product extracts since last full extract
        SELECT ext_id -- CHANGED: SELECT * to SELECT ext_id
         FROM   incore.sta_extract
         WHERE  extract_type = 'PRODUCT'
                AND comp = 1
                AND ext_id >= (SELECT Max(ext_id)
                               FROM   incore.sta_extract
                               WHERE  extract_type = 'PRODUCT'
                                      AND incremental = 0
                                      AND comp = 1)),
     pro
     AS (SELECT pro_p.ext_id,
                pro_p.productid,
                pro_p.sicovam,
                pro_c.rate,
                pro_p.listed,
                pro_p.tcm,
                pro_p.denomination,
                pro_p.private_placement,
                pro_p.early_redemption_exp_date,
                pro_p.early_redemption_date,
                pro_p.final_fixing_date,
                pro_p.initial_fixing_date,
                pro_p.redemption_date
                -- This redemption_date is from incore.pro_product
                ,
                pro_p.first_trading_date
                -- For each productid we find max EXT_ID
                ,
                Row_number()
                  over (
                    PARTITION BY pro_p.sicovam
                    ORDER BY pro_p.ext_id DESC) rn
         FROM   incore.pro_product pro_p
                join incore.pro_gua_fix_rate_coupon pro_c
                  ON pro_p.productid = pro_c.productid
                     AND pro_p.ext_id = pro_c.ext_id
                join incore.pro_product_market pro_m
                  ON pro_p.productid = pro_m.productid
                     AND pro_p.ext_id = pro_m.ext_id
         WHERE  pro_p.ext_id IN (SELECT ext_id
                                 FROM   ext)
        -- One product can exist in multiple extracts
        ),
     fixed_rate
     AS (SELECT pro.ext_id,
                -- Explicitly listing columns instead of SELECT * to be clear
                pro.productid,
                pro.sicovam,
                pro.rate,
                pro.listed,
                pro.tcm,
                pro.denomination,
                pro.private_placement,
                pro.early_redemption_exp_date,
                pro.early_redemption_date,
                pro.final_fixing_date,
                pro.initial_fixing_date,
                pro.redemption_date,
                pro.first_trading_date,
                pro.rn
         FROM   pro
         WHERE  rn = 1),
     distribution
     AS (SELECT s.anna_order_id,
                s.order_exclusion,
                s.client_contact_display,
                s.dim_trade_date,
                s.sicovam,
                s.isin,
                s.issue_date,
                s.redemption_date
                -- This is s.redemption_date from scope (polar.dim_sc_product)
                ,
                s.name,
                s.trade_date_in_month,
                s.issuer_group_1,
                s.issuer_group_2,
                s.order_source,
                s.reporting_quotation_type,
                s.investment_currency,
                s.issue_price,
                s.fx_rate,
                s.sales_credit_amt_chf,
                s.turnover_amt_chf -- This is the unrounded sum from scope
                ,
                s.total_nominal_units,
                s.total_nominal_units_split_sum,
                s.nisec,
                s.buysell,
                s.fin_pt_level1,
                s.fin_pt_level2,
                s.fin_pt_level3,
                s.sc_type_group,
                s.sophis_reference,
                s.guarantor,
                s.price_factor,
                f.rate
                -- ,underl.underlyings -- REMOVED: underlyings column
                ,
                CASE
                  WHEN Trunc(s.dim_trade_date) = Trunc(SYSDATE) THEN 'Today'
                  WHEN Trunc(s.dim_trade_date) = Trunc(SYSDATE - 1) THEN
                  'Yesterday'
                  WHEN Trunc(s.dim_trade_date) < Trunc(SYSDATE - 1) THEN
                  'Before'
                END today_yesterday,
                s.product_status,
                CASE
                  WHEN s.nisec = 'SEC'
                       AND s.buysell = 'LTQ BUY' THEN s.turnover_amt_chf
                  ELSE 0
                END buy_back
                -- Using s.turnover_amt_chf (unrounded sum from scope)
                ,
                CASE
                  WHEN s.guarantor IS NULL THEN s.issuer_group_2
                  ELSE
        Concat(Concat(Concat(s.issuer_group_2, '('), s.guarantor), ')')
                END issuer_guarantor,
                s.report_client_name,
                s.repo_client_type,
                s.client_country,
                s.it_initials,
                s.full_name,
                s.location_name,
                s.team_name,
                s.team_head_it_initials,
                s.sales_fee,
                s.sales_correction,
                s.business_type,
                s.dim_sc_client_id,
                s.ebo_price,
                s.trd_owner_it_initials,
                s.trd_owner_full_name,
                f.listed,
                f.tcm,
                f.denomination,
                f.private_placement,
                f.early_redemption_exp_date,
                f.early_redemption_date,
                f.final_fixing_date,
                f.initial_fixing_date,
                f.first_trading_date,
                s.crm_ltq_client_risk_classification,
                s.turnover_ccy,
                s.turnover_chf
                -- This is the rounded sum from scope (round(sum(...),2))
                ,
                s.total_economic_sc_ccy,
                s.total_economic_sc_chf,
                s.creation_date,
                s.anna_order_type
         FROM   scope s
                -- REMOVED: left join under_agg underl on s.sicovam=to_char(underl.src_instrument_id)
                left join fixed_rate f
                       ON s.sicovam = To_char(f.sicovam))
-- Kept to_char here assuming s.sicovam is char and f.sicovam is num. Adjust if types are different/consistent.
SELECT d.anna_order_id,
       d.dim_trade_date                 trade_date,
       d.sicovam,
       d.isin,
       d.order_source,
       Round(d.sales_credit_amt_chf, 0) AS SALES_CREDIT_AMT_CHF,
       d.total_nominal_units,
       Round(d.turnover_amt_chf, 0)     AS TURNOVER_AMT_CHF
       -- d.TURNOVER_AMT_CHF is the unrounded sum from scope
       ,
       d.nisec,
       d.buysell,
       d.business_type,
       d.sc_type_group,
       d.reporting_quotation_type,
       CASE
         WHEN d.reporting_quotation_type = 'IN_PERCENT' THEN
         d.price_factor * 100
         ELSE d.price_factor
       END                              AS Trade_price,
       d.dim_sc_client_id,
       d.crm_ltq_client_risk_classification -- First instance
       ,
       CASE
         WHEN ( d.report_client_name IS NULL
                AND d.issuer_group_1 = 'OTC' ) THEN 'OTC'
         ELSE ( CASE
                  WHEN d.report_client_name IS NULL THEN 'Retail'
                  ELSE d.report_client_name
                END )
       END                              AS REPORT_CLIENT_NAME,
       d.client_contact_display,
       d.it_initials,
       d.full_name,
       d.location_name,
       d.team_name,
       d.team_head_it_initials,
       d.listed,
       d.tcm,
       d.denomination,
       d.private_placement,
       d.early_redemption_exp_date,
       d.early_redemption_date,
       d.final_fixing_date,
       d.initial_fixing_date,
       d.redemption_date
       -- This redemption_date originates from scope.pro.redemption_date
       ,
       d.first_trading_date
       -- ,d.CRM_LTQ_CLIENT_RISK_CLASSIFICATION -- REMOVED: Second instance of this column
       ,
       d.turnover_ccy,
       d.turnover_chf
       -- This d.turnover_chf is the sum rounded to 2 decimal places from scope
       ,
       d.total_economic_sc_ccy,
       d.total_economic_sc_chf,
       d.creation_date,
       d.anna_order_type,
       d.fx_rate,
       d.investment_currency,
       adb.order_trade_date
FROM   distribution d
       left join incore.adb_orderscrecord adb
              ON d.anna_order_id = adb.orderid 



select
d.ANNA_ORDER_ID
,d.DIM_TRADE_DATE
,d.SICOVAM
,pro.investment_currency,
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
d.ORDER_SOURCE
,d.SALES_CREDIT_AMT_CHF
,d.SALES_CREDIT_AMT_CCY
,d.TOTAL_NOMINAL_UNITS_SPLIT_SUM
, d.TURNOVER_AMT_CHF
, d.TURNOVER_AMT_CCY
,d.NISEC
,d.BUYSELL
,d.BUSINESS_TYPE
,d.SC_TYPE_GROUP
,case when d.REPORTING_QUOTATION_TYPE = 'IN_PERCENT' then d.PRICE_FACTOR*100 else d.PRICE_FACTOR end as Trade_price
,d.DIM_SC_CLIENT_ID
,c.CRM_LTQ_CLIENT_RISK_CLASSIFICATION
,case 
  when (d.REPORT_CLIENT_NAME is null and d.issuer_group_1 = 'OTC') then 'OTC' 
  else (case when d.REPORT_CLIENT_NAME is null then 'Retail' else d.REPORT_CLIENT_NAME end) end as REPORT_CLIENT_NAME
,d.CLIENT_CONTACT_DISPLAY
,d.IT_INITIALS
,d.FULL_NAME
,d.LOCATION_NAME
,d.TEAM_NAME
,d.TEAM_HEAD_IT_INITIALS
,adb.order_trade_date
from POLAR.FCT_SC_DISTRIBUTION_MV d
left join POLAR.DIM_SC_CLIENT c
on d.DIM_SC_CLIENT_ID = c.DIM_CLIENT_ID
LEFT JOIN polar.dim_sc_product pro
ON d.SICOVAM = pro.product_identifier
left join incore.adb_orderscrecord adb
ON        d.anna_order_id = adb.orderid
where d.SC_TYPE_GROUP <> 'External'
and d.SC_TYPE_GROUP <> 'External Client'