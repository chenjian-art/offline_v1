

for i in	price_power  product_info  sales_detail	search_term	traffic_detail
do
	sqoop-import --connect jdbc:mysql://cdh03:3306/electronic \
          --username root \
          --password root \
          --delete-target-dir \
          -m 1 \
          --table $i \
          --target-dir /origin_data/electronic/$i/20250330 \
          -z \
          --compression-codec gzip \
          --null-string '\\N' \
          --null-non-string '\\N' \
          --fields-terminated-by '\t'
done