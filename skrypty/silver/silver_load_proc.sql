-- addresses cleaning
SELECT    	address_id,
			TRIM(country) AS country,
		    UPPER(LEFT(TRIM(city), 1)) + LOWER(SUBSTRING(TRIM(city), 2, LEN(TRIM(city)))) AS city,
	      	TRIM('ul. ' + UPPER(LEFT(TRIM(SUBSTRING(street, CHARINDEX(' ', street), LEN(street))), 1)) + 
			LOWER(SUBSTRING(TRIM(SUBSTRING(street, CHARINDEX(' ', street), LEN(street))), 2, 100))) AS street,
		    REPLACE(TRIM(postal_code),' ','') AS postal_code,
      		CASE 
      			WHEN street IS NULL OR TRIM(street) = '' THEN 'UNKNOWN'
      			ELSE UPPER(LEFT(LTRIM(REPLACE(TRIM(street), 'ul.', '')), 1)) + LOWER(SUBSTRING(LTRIM(REPLACE(TRIM(street), 'ul.', '')), 2, LEN(street)))
      		END AS street,
      		created_at,
      		updated_at
FROM      	bronze.addresses;

-- customers cleaning
SELECT    	customer_id,
      		CASE
      			WHEN first_name IS NULL OR TRIM(first_name) = '' THEN 'N/A' ELSE
      			UPPER(LEFT(TRIM(first_name), 1)) + LOWER(SUBSTRING(TRIM(first_name), 2, LEN(TRIM(first_name))))
      		END AS first_name,
      		CASE
      			WHEN last_name IS NULL OR TRIM(last_name)  = '' then 'N/A' ELSE
      			UPPER(LEFT(TRIM(last_name), 1)) + LOWER(SUBSTRING(TRIM(last_name), 2, LEN(TRIM(last_name)))) 
      		END AS last_name,
      		TRIM(LOWER(email)) AS email,
      		CASE WHEN date_of_birth > GETDATE() THEN NULL ELSE date_of_birth END AS dob,
      		CASE
      			WHEN gender LIKE '%obieta' OR UPPER(gender) = 'K' THEN 'Kobieta'
      			WHEN gender LIKE '%ezczyzna' OR UPPER(gender) = 'M' THEN 'Mezczyzna'
      		END AS gender,
      		join_date,
      		address_id,
      		created_at,
          	updated_at
FROM      	bronze.customers;

-- categories cleaning
SELECT    	category_id,
      		ISNULL(TRIM(name), 'N/A') AS NAME,
      		parent_category_id,
      		created_at,
      		updated_at
FROM     	bronze.categories;

-- brands cleaning
SELECT    	brand_id,
      		ISNULL(TRIM(name), 'N/A') AS name,
      		created_at,
      		updated_at
FROM      	bronze.brands;

-- products cleaning
SELECT    	product_id,
      		ISNULL(TRIM(name), 'N/A') AS name,
      		ISNULL(TRIM(REPLACE(description, '%', ' ')), 'N/A') AS description,
      		category_id,
      		brand_id,
      		ABS(list_price) AS list_price,
      		created_at,
      		updated_at
FROM		bronze.products;

-- payment methods cleaning
SELECT payment_method_id,
		TRIM(payment_method_name) AS payment_method_name,
		is_active,
		created_at,
		updated_at
FROM bronze.payment_methods;

-- orders cleaning
SELECT		order_id,
			customer_id,
			order_date,
			payment_method_id,
			session_id,
			UPPER(TRIM(order_status)) AS order_status,
			ISNULL(ABS(subtotal_amount), 0) AS subtotal_amount,
			ISNULL(ABS(discount_amount), 0) AS discount_amount,
			ISNULL(ABS(tax_amount), 0) AS tax_amount,
			ISNULL(ABS(shipping_amount), 0) AS shipping_amount,
			ISNULL(ABS(total_amount), 0) AS total_amount,
			--(ISNULL(ABS(subtotal_amount), 0) + ISNULL(ABS(tax_amount), 0) + ISNULL(ABS(shipping_amount), 0)) - ISNULL(ABS(discount_amount), 0) AS total_amount2,
			created_at,
			updated_at
FROM    	bronze.orders;

-- order items cleaning
SELECT		order_item_id,
			order_id,
			product_id,
			quantity,
			ISNULL(ABS(unit_price), 0) AS unit_price,
			ISNULL(ABS(discount_amount), 0) AS discount_amount,
			ISNULL(ABS(tax_amount), 0) AS tax_amount,
			( (quantity * ISNULL(ABS(unit_price), 0)) + ISNULL(ABS(tax_amount), 0) ) - ISNULL(ABS(discount_amount), 0) AS line_total,
			created_at,
			updated_at
FROM		bronze.order_items;

-- returns cleaning
SELECT		return_id,
			order_item_id,
			CASE
				WHEN return_date > GETDATE() THEN NULL
				ELSE return_date
			END AS return_date,
			ISNULL(ABS(quantity_returned), 0) AS quantity_returned,
			ISNULL(ABS(refund_amount), 0) AS refund_amount,
			CASE 
				WHEN reason IS NULL OR TRIM(reason) IN ('', '-') THEN 'N/A' 
				ELSE UPPER(LEFT(TRIM(reason), 1)) + LOWER(SUBSTRING(TRIM(reason), 2, LEN(reason))) 
			END AS reason,
			created_at,
			updated_at
FROM		bronze.order_item_returns;

-- pageviews cleaning
SELECT	website_pageview_id,
		created_at AS pageview_time,
		website_session_id,
		LOWER(TRIM(pageview_url)) AS pageview_url
FROM	bronze.pageviews;

-- sessions cleaning
SELECT	website_session_id,
		created_at AS session_start,
		user_id,
		is_repeat_session,
		ISNULL(LOWER(TRIM(utm_source)), 'direct') AS utm_source,
		ISNULL(TRIM(LOWER(utm_campaign)), 'organic traffic') AS utm_campaign,
		ISNULL(TRIM(utm_content), 'organic') AS utm_content,
		ISNULL(TRIM(LOWER(device_type)), 'N/A') AS device_type,
		TRIM(http_referer) AS http_referer
FROM bronze.website_sessions
