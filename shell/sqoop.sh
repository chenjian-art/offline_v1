do_date = $1


for i in	spu_info	user_info	sku_info	order_status_log	payment_info	order_refund_info	order_info	order_detail
holiday_year	holiday_info	favor_info	date_info	coupon_use	coupon_info	comment_info	cart_info
base_trademark	base_province	base_region	base_dic	base_category2	base_category3	base_category1
activity_sku	activity_rule	activity_order	activity_info


do
sqoop-import --connect jdbc:mysql://cdh03:3306/gmall \
    --username root  \
	--password root \
	--delete-target-dir \
	-m 1 \
	--table $i \
	--target-dir /origin_data/gmall/$i/$do_date \
	-z \
	--null-string '\\N' \
	--null-non-string '\\N' \
	--fields-terminated-by '\t'
done