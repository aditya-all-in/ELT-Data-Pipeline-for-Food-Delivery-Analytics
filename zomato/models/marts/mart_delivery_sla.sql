-- Delivery SLA (Service Level Agreement)

select 
    city, 
    hour(order_timestamp) as order_hour, 
    count_if(is_delivered) as delivered_orders,
    round(median(delivery_time_min),1) as p50, -- 50th percentile 
    round(percentile_cont(0.9) within group (order by delivery_time_min),1) as p90 -- 90th percentile

from {{ ref('fct_orders') }} 
where is_delivered 
group by 1,2