# POC3_pyspark
 
script to run application is:
spark-submit --master  spark://DELEQ0283000764.corp.capgemini.com:7077 \
/home/wojkamin/my_folder/poc3_folder/poc3.py \
--clients_csv poc3_dataset/clients.csv \
--financial_csv "poc3_dataset/financial.csv" \
--list_countries "Poland" "France"

or

python poc3.py \
--clients_csv poc3_dataset/clients.csv \
--financial_csv "poc3_dataset/financial.csv" \
--list_countries "Poland" "France"

results are stored in folder client_data