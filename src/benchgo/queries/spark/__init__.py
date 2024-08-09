'''
# Unix/Linux sed commands to convert TPC-DS dsqgen output for db2 target to
# something SparkSQL can use. These are applied to all query_*.sql files present
# in child folders before converting to JSON for use by this benchmark tool.
sed -i 's/"30 days"/30_days/g' query_*.sql
sed -i 's/"31-60 days"/31to60_days/g' query_*.sql
sed -i 's/"61-90 days"/61to90_days/g' query_*.sql
sed -i 's/"91-120 days"/91to120_days/g' query_*.sql
sed -i 's/">120 days"/gt120_days/g' query_*.sql
sed -i 's/"order count"/order_count/g' query_*.sql
sed -i 's/"total shipping cost"/total_shipping_cost/g' query_*.sql
sed -i 's/"total net profit"/total_net_profit/g' query_*.sql
sed -i 's/"excess discount amount"/excess_discount_amount/g' query_*.sql
sed -i 's/"Excess Discount Amount"/excess_discount_amount/g' query_*.sql
sed -i 's/fetch first 100 rows only/limit 100/g' query_*.sql
'''
