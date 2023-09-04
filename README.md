# POC3_pyspark
 
script to run application is:
spark-submit --master  spark://DELEQ0283000764.corp.capgemini.com:7077 \
/home/wojkamin/my_folder/poc3_folder/poc3.py \
--dataset1 poc3_dataset/clients.csv \
--dataset2 "poc3_dataset/financial.csv" \
--list_countries "Poland" "France"

results are stored in folder client_data