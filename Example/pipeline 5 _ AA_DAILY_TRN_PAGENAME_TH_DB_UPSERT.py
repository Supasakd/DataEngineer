# Databricks notebook source
# DBTITLE 1,Download Package and Remove Widgets
from datetime import datetime, timedelta, timezone

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Set Parameter
dbutils.widgets.text("dp_data_dt", str(datetime.date(datetime.now().astimezone(timezone(timedelta(hours=7)))) - timedelta(days=1)))
dbutils.widgets.text("trgt_db_nm", "customer_loyalty")
dbutils.widgets.text("trgt_tbl_nm", "AA_DAILY_TRN_PAGENAME")
dbutils.widgets.text("country", "th")
dbutils.widgets.text("COMPANY_CODE", "11")
dbutils.widgets.text("period", "12")
dbutils.widgets.text("year", "2022")

# COMMAND ----------

# DBTITLE 1,Import common transformation functions
# MAGIC %run /UDAP_FW/fw/cmmn/bin/module/fw_cmmn_tnfm_func

# COMMAND ----------

# DBTITLE 1,Job Configurations
# MAGIC %python
# MAGIC # Job specific spark config
# MAGIC spark_configs: Dict[str, str] = {
# MAGIC }
# MAGIC 
# MAGIC # Parameters
# MAGIC dp_data_dt = dbutils.widgets.get("dp_data_dt")
# MAGIC trgt_db_nm = dbutils.widgets.get("trgt_db_nm")
# MAGIC trgt_tbl_nm = dbutils.widgets.get("trgt_tbl_nm")
# MAGIC country = dbutils.widgets.get("country")
# MAGIC COMPANY_CODE = dbutils.widgets.get("COMPANY_CODE")
# MAGIC period = dbutils.widgets.get("period")
# MAGIC year = dbutils.widgets.get("year")

# COMMAND ----------

# DBTITLE 1,Apply Job Configurations
# Apply the job specific spark configurations
apply_spark_configs(spark_configs)

# Retrieve the company code based on the provided country code
company_code = get_company_code(country)
dbutils.widgets.text("company_code", str(company_code))

# Add the company code as a spark runtime config/variable
# Required for the SQL cell to work
spark.conf.set("job.company_code", str(company_code))

# COMMAND ----------

# DBTITLE 1,Set Pre_Action AND Write_Option
# Write Options
pre_action= f"""
  DELETE FROM {trgt_db_nm}.{trgt_tbl_nm}
  WHERE COMPANY_CODE = {company_code} AND left(dp_data_dt,7) = left('{dp_data_dt}',7)
"""

write_options: Dict[str, str] = {
  "replaceWhere": f"COMPANY_CODE = {company_code} AND left(dp_data_dt,7) = left('{dp_data_dt}',7)"
}

# COMMAND ----------

# DBTITLE 1,Describe History
# MAGIC %sql
# MAGIC -- DESCRIBE HISTORY customer_loyalty.AA_DAILY_TRN_PAGENAME_TH_DB_UPSERT;

# COMMAND ----------

# DBTITLE 1,Drop Table
# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS customer_loyalty.AA_DAILY_TRN_PAGENAME;

# COMMAND ----------

# DBTITLE 1,Delete Data on External Location
# MAGIC %python
# MAGIC # dbutils.fs.rm(dir='abfss://data@pvdardlsazc03.dfs.core.windows.net/customer_loyalty/AA_DAILY_TRN_PAGENAME',recurse=True)

# COMMAND ----------

# DBTITLE 1,DDL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customer_loyalty.AA_DAILY_TRN_PAGENAME (
# MAGIC   COMPANY_CODE SMALLINT NOT NULL
# MAGIC   
# MAGIC   ,Hash_ID STRING
# MAGIC   ,month STRING
# MAGIC 
# MAGIC   ,clicks_myCoupon BIGINT
# MAGIC   ,clicks_myCoupon_from_home BIGINT
# MAGIC   ,clicks_myCoupon_from_myLotus BIGINT
# MAGIC   ,clicks_myCoupon_from_myProfile BIGINT
# MAGIC   
# MAGIC   ,clicks_redeemCoin BIGINT
# MAGIC   ,clicks_redeemCoin_from_home BIGINT
# MAGIC   ,clicks_redeemCoin_from_myLotus BIGINT
# MAGIC   ,clicks_redeemCoin_from_myProfile BIGINT
# MAGIC   
# MAGIC   ,clicks_scanToEarnCoin BIGINT
# MAGIC   ,clicks_Coupon_card_list BIGINT
# MAGIC   ,clicks_howToUseAPP BIGINT
# MAGIC   ,clicks_CoinHistory BIGINT
# MAGIC   ,clicks_coinPowerUpRedemption BIGINT
# MAGIC   ,clicks_fastDelivery BIGINT
# MAGIC   ,clicks_nextDayDelivery BIGINT
# MAGIC 
# MAGIC   ,clicks_homeBanner1 BIGINT
# MAGIC   ,clicks_homeBanner2 BIGINT
# MAGIC   ,clicks_homeBanner3 BIGINT
# MAGIC   ,clicks_homeBanner4 BIGINT
# MAGIC   ,clicks_homeBanner5 BIGINT
# MAGIC   ,clicks_homeBanner6 BIGINT
# MAGIC   
# MAGIC   ,DP_DATA_DT DATE
# MAGIC   ,DP_LOAD_TS TIMESTAMP
# MAGIC   
# MAGIC ) USING DELTA PARTITIONED BY (COMPANY_CODE, DP_DATA_DT) TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC ) LOCATION 'abfss://data@pvdardlsazc03.dfs.core.windows.net/${trgt_db_nm}/${trgt_tbl_nm}'
# MAGIC ;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC transformation_sql = f"""
# MAGIC with visit_pagename_prep as 
# MAGIC (select 
# MAGIC     date_time
# MAGIC     ,left(date_time,7) as date
# MAGIC     ,Post_evar1 as Hash_ID
# MAGIC     ,Post_evar2 as currnet_page_name
# MAGIC     ,lag(Post_evar2) OVER (PARTITION BY Post_evar1 ORDER BY date_time asc) as previous_page_name
# MAGIC     
# MAGIC from adobe_analytic.webapp_event 
# MAGIC where left(date_time,7) = concat('{year}','-','{period}') 
# MAGIC and country = '{country}'
# MAGIC )
# MAGIC 
# MAGIC select 
# MAGIC     CAST('{company_code}' AS SHORT) as COMPANY_CODE
# MAGIC     ,Hash_ID
# MAGIC     ,left(date,7) as month 
# MAGIC     
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_myCoupon
# MAGIC     ,count(case when previous_page_name = 'Home' and currnet_page_name = 'x' then Hash_ID end) as clicks_myCoupon_from_home 
# MAGIC     ,count(case when previous_page_name = 'Loyalty Home' and currnet_page_name = 'x' then Hash_ID end) as clicks_myCoupon_from_myLotus 
# MAGIC     ,count(case when previous_page_name = 'My Profile' and currnet_page_name = 'x' then Hash_ID end) as clicks_myCoupon_from_myProfile 
# MAGIC     
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_redeemCoin
# MAGIC     ,count(case when previous_page_name = 'Home' and currnet_page_name = 'x' then Hash_ID end) as clicks_redeemCoin_from_home 
# MAGIC     ,count(case when previous_page_name = 'Loyalty Home' and currnet_page_name = 'x' then Hash_ID end) as clicks_redeemCoin_from_myLotus 
# MAGIC     ,count(case when previous_page_name = 'My Profile' and currnet_page_name = 'x' then Hash_ID end) as clicks_redeemCoin_from_myProfile 
# MAGIC         
# MAGIC     ,count(case when currnet_page_name = 'QR Code' then Hash_ID end) as clicks_scanToEarnCoin
# MAGIC     ,count(case when currnet_page_name = 'Coupon Card List' then Hash_ID end) as clicks_Coupon_card_list
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_howToUseAPP
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_CoinHistory 
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_coinPowerUpRedemption
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_fastDelivery
# MAGIC     ,count(case when currnet_page_name = 'Delivery Method' then Hash_ID end) as clicks_nextDayDelivery
# MAGIC 
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_homeBanner1
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_homeBanner2
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_homeBanner3
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_homeBanner4
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_homeBanner5
# MAGIC     ,count(case when currnet_page_name = 'x' then Hash_ID end) as clicks_homeBanner6
# MAGIC     ,cast('{dp_data_dt}' as date) as DP_DATA_DT
# MAGIC     ,CURRENT_TIMESTAMP() + INTERVAL 7 HOURS AS DP_LOAD_TS
# MAGIC from visit_pagename_prep
# MAGIC group by     
# MAGIC     CAST('{company_code}' AS SHORT)
# MAGIC     ,Hash_ID
# MAGIC     ,left(date,7)
# MAGIC     ,cast('{dp_data_dt}' as date)
# MAGIC     ,CURRENT_TIMESTAMP() + INTERVAL 7 HOURS 
# MAGIC """
# MAGIC 
# MAGIC df = spark.sql(transformation_sql)

# COMMAND ----------

# DBTITLE 1,Export the Data
# MAGIC %python
# MAGIC 
# MAGIC execute_insert(sdf=df, dfw_options=write_options, pre_action=pre_action)
# MAGIC execute_delta_maintenance()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select distinct month from customer_loyalty.AA_DAILY_TRN_PAGENAME
# MAGIC select Hash_ID,count(Hash_ID) from customer_loyalty.AA_DAILY_TRN_PAGENAME where left(dp_data_dt,7) = '2022-11' group by 1 having count(Hash_ID) >1

# COMMAND ----------


