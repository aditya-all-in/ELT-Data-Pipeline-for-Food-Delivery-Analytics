
-- USE WHEN RAW DATA COLS ARE ALL STRING TYPE AND NEED TO BE CASTED TO APPROPRIATE DATA TYPES
-- select 
--     order_item_id::number as order_item_id, 
--     try_to_number(order_id) as order_id, 
--     try_to_number(r_id) as restaurant_id, 
--     f_id::string as f_id, 
--     try_to_decimal(price, 10, 2) as price, 
--     try_to_number(quantity) as quantity, 
--     try_to_decimal(line_amount, 10, 2) as line_amount
-- from {{ source('raw', 'order_items') }}
-- where try_to_number(order_item_id) is not null



-- IF DATA IS ALREADY CASTED TO APPROPRIATE DATA TYPES, USE BELOW
select
    order_item_id,
    order_id,
    r_id as restaurant_id,
    f_id,
    price::number(10,2) as price,
    quantity,
    line_amount::number(10,2) as line_amount

from {{ source('raw', 'order_items') }}
where order_item_id is not null
