WITH under_prep_agg AS
(
          SELECT    a.src_instrument_id,
                    b.reference
          FROM      polar.dim_underlyings a
          left join polar.dim_instrument b
          ON        a.src_underlying_id=b.src_instrument_id
          WHERE     valid_to IS NULL), under_agg AS
(
         SELECT   src_instrument_id,
                  listagg(DISTINCT reference,',' on overflow TRUNCATE) within GROUP (ORDER BY reference) underlyings
         FROM     under_prep_agg
         GROUP BY src_instrument_id), scope AS
( -- first define the scope of orders and products
           SELECT
                      /*+ materialize */
                      b.anna_order_id,
                      b.order_exclusion,
                      b.client_contact_display,
                      fct.dim_trade_date,
                      round((fct.dim_trade_date-pro.issue_date)/30) trade_date_in_month,
                      fct.dim_sc_product_id,
                      iss.issuer_group_1,
                      iss.issuer_group_2,
                      scos.order_source,
                      min(fct.fx_rate)                          fx_rate,
                      SUM(fct.sales_credit_amt_chf)             sales_credit_amt_chf,
                      SUM(fct.turnover_split_sum_chf)           turnover_amt_chf,
                      SUM(total_nominal_units*tradecount_split) total_nominal_units_split_sum,
                      min(total_nominal_units)                  total_nominal_units,
                      pro.product_identifier                    sicovam,
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
                      tro.it_initials                      trd_owner_it_initials,
                      tro.full_name                        trd_owner_full_name ,
                      round(SUM(turnover_split_sum_ccy),2) turnover_ccy ,
                      round(SUM(turnover_split_sum_chf),2) turnover_chf ,
                      SUM(
                      CASE
                                 WHEN dst.sc_type_group = 'Economic' THEN sales_credit_amt_ccy
                                 ELSE 0
                      END) total_economic_sc_ccy ,
                      SUM(
                      CASE
                                 WHEN dst.sc_type_group = 'Economic' THEN sales_credit_amt_chf
                                 ELSE 0
                      END) total_economic_sc_chf ,
                      fct.creation_date ,
                      odt.anna_order_type ,
                      cli.crm_ltq_client_risk_classification
           FROM       polar.fct_sc_salescredit fct
           inner join polar.dim_sc_order_id b
           ON         fct.dim_sc_order_id = b.id
           inner join polar.dim_sc_order_status st
           ON         st.dim_order_status_id = fct.dim_order_status_id
           inner join polar.dim_sc_sales_person sp
           ON         fct.dim_sc_recipient_id = sp.dim_sales_person_id
           AND        location_name = 'Milan'
           inner join polar.dim_sc_product pro
           ON         fct.dim_sc_product_id = pro.dim_sc_product_id
           inner join polar.dim_sc_sales_person tro
           ON         tro.dim_sales_person_id = fct.dim_trade_owner_id
           left join  polar.dim_sc_issuer iss
           ON         iss.dim_sc_issuer_id=fct.dim_sc_issuer_id
           left join  polar.dim_sc_order_type odt
           ON         odt.dim_order_type_id=fct.dim_order_type_id
           left join  polar.dim_sc_type sc_type
           ON         sc_type.dim_sc_type_id=fct.dim_sc_type_id
           left join  polar.dim_sc_product dm_prod
           ON         dm_prod.dim_sc_product_id=fct.dim_sc_product_id
           left join  polar.dim_sc_client cli
           ON         fct.dim_sc_client_id=cli.dim_client_id
           left join  polar.dim_sc_order_source scos
           ON         fct.dim_order_source_id=scos.dim_order_source_id
           left join  polar.dim_sc_type dst
           ON         dst.dim_sc_type_id = fct.dim_sc_type_id
           WHERE      fct.valid_to IS NULL
           AND        st.reporting_order_status = 'OK'
           AND        fct.dim_trade_date >= DATE '2025-01-01'
           AND        sc_type.sc_type_group <> 'External'
           AND        sc_type.sc_type_group <> 'External Client'
           GROUP BY   b.anna_order_id,
                      b.order_exclusion,
                      b.client_contact_display,
                      fct.dim_trade_date,
                      fct.creation_date,
                      odt.anna_order_type,
                      cli.crm_ltq_client_risk_classification,
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
                      tro.full_name ) ,ext AS
(
       -- All completed product extracts since last full extract
       SELECT *
       FROM   incore.sta_extract
       WHERE  extract_type='PRODUCT'
       AND    comp=1
       AND    ext_id >=
              (
                     SELECT max(ext_id)
                     FROM   incore.sta_extract
                     WHERE  extract_type = 'PRODUCT'
                     AND    incremental=0
                     AND    comp=1) ) ,pro AS
(
         SELECT   pro_p.ext_id ,
                  pro_p.productid ,
                  pro_p.sicovam ,
                  pro_c.rate ,
                  pro_p.listed ,
                  pro_p.tcm ,
                  pro_p.denomination ,
                  pro_p.private_placement ,
                  pro_p.early_redemption_exp_date ,
                  pro_p.early_redemption_date ,
                  pro_p.final_fixing_date ,
                  pro_p.initial_fixing_date ,
                  pro_p.redemption_date ,
                  pro_p.first_trading_date
                  -- For each productid we find max EXT_ID
                  ,
                  row_number() over (PARTITION BY pro_p.sicovam ORDER BY pro_p.ext_id DESC) rn
         FROM     incore.pro_product pro_p
         join     incore.pro_gua_fix_rate_coupon pro_c
         ON       pro_p.productid = pro_c.productid
         AND      pro_p.ext_id=pro_c.ext_id
         join     incore.pro_product_market pro_m
         ON       pro_p.productid = pro_m.productid
         AND      pro_p.ext_id=pro_m.ext_id
         WHERE    pro_p.ext_id IN
                  (
                         SELECT ext_id
                         FROM   ext) -- One product can exist in multiple extracts
) , fixed_rate AS
(
       SELECT *
       FROM   pro
       WHERE  rn=1) , distribution AS
(
          SELECT    s.anna_order_id ,
                    s.order_exclusion ,
                    s.client_contact_display ,
                    s.dim_trade_date ,
                    s.sicovam ,
                    s.isin ,
                    s.issue_date ,
                    s.redemption_date ,
                    s.name ,
                    s.trade_date_in_month ,
                    s.issuer_group_1 ,
                    s.issuer_group_2 ,
                    s.order_source ,
                    s.reporting_quotation_type ,
                    s.investment_currency ,
                    s.issue_price ,
                    s.fx_rate ,
                    s.sales_credit_amt_chf ,
                    s.turnover_amt_chf ,
                    s.total_nominal_units ,
                    s.total_nominal_units_split_sum ,
                    s.nisec ,
                    s.buysell ,
                    s.fin_pt_level1 ,
                    s.fin_pt_level2 ,
                    s.fin_pt_level3 ,
                    s.sc_type_group ,
                    s.sophis_reference ,
                    s.guarantor ,
                    s.price_factor ,
                    f.rate ,
                    underl.underlyings ,
                    CASE
                              WHEN trunc(s.dim_trade_date)=trunc(SYSDATE) THEN 'Today'
                              WHEN trunc(s.dim_trade_date)=trunc(SYSDATE-1) THEN 'Yesterday'
                              WHEN trunc(s.dim_trade_date)<trunc(SYSDATE-1) THEN 'Before'
                    END today_yesterday ,
                    s.product_status ,
                    CASE
                              WHEN s.nisec='SEC'
                              AND       s.buysell='LTQ BUY' THEN s.turnover_amt_chf
                              ELSE 0
                    END buy_back ,
                    CASE
                              WHEN s.guarantor IS NULL THEN s.issuer_group_2
                              ELSE concat(concat(concat(s.issuer_group_2,'('),s.guarantor),')')
                    END issuer_guarantor ,
                    s.report_client_name ,
                    s.repo_client_type ,
                    s.client_country ,
                    s.it_initials ,
                    s.full_name ,
                    s.location_name ,
                    s.team_name ,
                    s.team_head_it_initials ,
                    s.sales_fee ,
                    s.sales_correction ,
                    s.business_type ,
                    s.dim_sc_client_id ,
                    s.ebo_price ,
                    s.trd_owner_it_initials ,
                    s.trd_owner_full_name ,
                    f.listed ,
                    f.tcm ,
                    f.denomination ,
                    f.private_placement ,
                    f.early_redemption_exp_date ,
                    f.early_redemption_date ,
                    f.final_fixing_date ,
                    f.initial_fixing_date ,
                    f.first_trading_date ,
                    s.crm_ltq_client_risk_classification ,
                    s.turnover_ccy ,
                    s.turnover_chf ,
                    s.total_economic_sc_ccy ,
                    s.total_economic_sc_chf ,
                    s.creation_date ,
                    s.anna_order_type
          FROM      scope s
          left join under_agg underl
          ON        s.sicovam=to_char(underl.src_instrument_id)
          left join fixed_rate f
          ON        s.sicovam=to_char(f.sicovam))
SELECT    d.anna_order_id ,
          d.dim_trade_date trade_date ,
          d.sicovam ,
          d.isin ,
          d.order_source ,
          round(d.sales_credit_amt_chf,0) AS sales_credit_amt_chf ,
          d.total_nominal_units ,
          round(d.turnover_amt_chf,0) AS turnover_amt_chf ,
          d.nisec ,
          d.buysell ,
          d.business_type ,
          d.sc_type_group ,
          d.reporting_quotation_type ,
          CASE
                    WHEN d.reporting_quotation_type = 'IN_PERCENT' THEN d.price_factor*100
                    ELSE d.price_factor
          END AS trade_price ,
          d.dim_sc_client_id ,
          d.crm_ltq_client_risk_classification ,
          CASE
                    WHEN (
                                        d.report_client_name IS NULL
                              AND       d.issuer_group_1 = 'OTC') THEN 'OTC'
                    ELSE (
                              CASE
                                        WHEN d.report_client_name IS NULL THEN 'Retail'
                                        ELSE d.report_client_name
                              END)
          END AS report_client_name ,
          d.client_contact_display ,
          d.it_initials ,
          d.full_name ,
          d.location_name ,
          d.team_name ,
          d.team_head_it_initials ,
          d.listed ,
          d.tcm ,
          d.denomination ,
          d.private_placement ,
          d.early_redemption_exp_date ,
          d.early_redemption_date ,
          d.final_fixing_date ,
          d.initial_fixing_date ,
          d.redemption_date ,
          d.first_trading_date ,
          d.crm_ltq_client_risk_classification ,
          d.turnover_ccy ,
          d.turnover_chf ,
          d.total_economic_sc_ccy ,
          d.total_economic_sc_chf ,
          d.creation_date ,
          d.anna_order_type ,
          d.fx_rate ,
          d.investment_currency ,
          adb.order_trade_date
FROM      distribution d
left join incore.adb_orderscrecord adb
ON        d.anna_order_id = adb.orderid