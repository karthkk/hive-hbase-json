See http://wiki.apache.org/hadoop/Hive/HBaseIntegration for
information about the HBase storage handler.

Example Create table command for Session Summary:
CREATE EXTERNAL TABLE session_summary(session_date string,merchant_id string, session_id string, site_cid string, promo string,order_id string,merchant_total_dollars string,runa_total_dollars string, purchased string, sushi string, promo_determination string, products array<string>, event_ids map<string,string>) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = "meta:date,meta:merchant-id,:key,summary:details[site-cid],summary:details[promo],summary:details[cart][order-id],summary:details[cart][merchant-total-dollars],summary:details[cart][runa-total-dollars],summary:details[cart][purchased?],summary:details[sushi],summary:details[promo-determination],summary:details[items],event_ids:") TBLPROPERTIES("hbase.table.name" = "karthik_development_tesla_session_summary");
