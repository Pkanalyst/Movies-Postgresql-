Create table DataCo(
Type varchar,
Days_for_shipping_real int,
Days_for_shipment_scheduled int,
Benefit_per_order decimal,
Sales_per_customer decimal,
Delivery_Status varchar,
Late_delivery_risk int,
Category_Id int,
Category_Name varchar,
Customer_City varchar,
Customer_Country varchar,
Customer_Email varchar,
Customer_Fname varchar,
Customer_Id varchar,
Customer_Lname varchar,
Customer_Password varchar,
Customer_Segment varchar,
Customer_State varchar,
Customer_Street varchar,
Customer_Zipcode varchar,
Department_Id varchar,
Department_Name varchar,
Latitude decimal,
Longitude decimal,
Market varchar,
Order_City varchar,
Order_Country varchar,
Order_Customer_Id varchar,
order_date_DateOrders timestamp,
Order_Id varchar,
Order_Item_Cardprod_Id varchar,
Order_Item_Discount decimal,
Order_Item_Discount_Rate decimal,
Order_Item_Id varchar,
Order_Item_Product_Price decimal,
Order_Item_Profit_Ratio decimal,
Order_Item_Quantity int,
Sales decimal,
Order_Item_Total decimal,
Order_Profit_Per_Order decimal,
Order_Region varchar,
Order_State varchar,
Order_Status varchar,
Order_Zipcode varchar,
Product_Card_Id varchar,
Product_Category_Id varchar,
Product_Description varchar,
Product_Image varchar,
Product_Name varchar,
Product_Price decimal,
Product_Status varchar,
shipping_date_DateOrders timestamp,
Shipping_Mode varchar);




SET datestyle TO 'ISO, MDY';

copy DataCo
FROM 'E:/Datasets/DataCo Supply Chain Dataset/DataCo.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');

SET datestyle TO 'ISO, DMY';

select * from DataCo;


 
-- #################################################################################
-- # DATACO SUPPLY CHAIN MANAGEMENT ANALYSIS - SQL PORTFOLIO SCRIPT (LOWERCASE KEYWORDS)
-- #################################################################################

-- Table Used: DataCo (Assumed to be the single, denormalized table)

-- #################################################################################
-- # SECTION 1: BASIC QUERIES (Aggregation & Filtering)
-- #################################################################################

-- Q1.1: What is the total number of orders in the dataset?

select count(distinct Order_Id) as total_orders from DataCo;

-- Q1.2: Calculate the total sales across all order items.

select sum(sales) as total_sales from DataCo;

-- Q1.3: Find the minimum, maximum, and average Order Item Total.

select min(Order_Item_Total) as min_item_total,max(Order_Item_Total) as max_item_total,
    avg(Order_Item_Total) as avg_item_total from DataCo;

-- Q1.4: How many unique customers are in the dataset?

select count(distinct Customer_Id) as unique_customers from DataCo;

-- Q1.5: List the top 10 customer cities by the number of orders placed.

select Customer_City, count(distinct Order_Id) as num_orders from DataCo
group by Customer_City
order by num_orders desc
limit 10;

-- Q1.6: Which product category has the highest average Product Price?

select Category_Name,avg(Product_Price) as avg_price from DataCo
group by Category_Name
order by avg_price desc
limit 1;

-- Q1.7: Find the average real shipping days for orders shipped using the 'Standard Class' Shipping Mode.

select avg(Days_for_shipping_real) as avg_real_shipping_days from DataCo
where Shipping_Mode = 'Standard Class';

-- #################################################################################
-- # SECTION 2: INTERMEDIATE QUERIES (Date Functions, Conditional Logic, & Aggregation)
-- #################################################################################

-- Q2.1: Calculate the total sales for each month in the dataset.

select to_char(order_date_DateOrders, 'YYYY-MM') as order_month,sum(sales) as monthly_sales from DataCo
group by order_month
order by order_month;

-- Q2.2: Determine the percentage of orders that were 'Shipped', 'Complete', or 'On Hold' based on Order Status.

select Order_Status,count(distinct Order_Id) as orders_count,
    (cast(count(distinct Order_Id) as decimal) * 100 / (select count(distinct Order_Id) from DataCo)) as percentage
from DataCo
group by Order_Status;

-- Q2.3: Use a CASE statement to label sales as 'High-Value' (Sales > $500), 'Medium-Value' (Sales $100-$500), 
         'or 'Low-Value' (Sales <= $100). Then, count the orders in each group.'
		 
select
    case
        when sales > 500 then 'High-Value'
        when sales between 100 and 500 then 'Medium-Value'
        else 'Low-Value'
    end as order_value_group,
    count(distinct Order_Id) as orders_count
from DataCo
group by 1;

-- Q2.4: Identify the top 5 customers based on their total sales contribution.

select Customer_Id,sum(sales) as total_customer_sales from DataCo
group by Customer_Id
order by total_customer_sales desc
limit 5;

-- Q2.5: For each Shipping Mode, calculate the average profit and the total number of orders where 
        'late_delivery_risk is 1 (high risk).'
		
select Shipping_Mode,avg(Order_Profit_Per_Order) as avg_profit,count(distinct Order_Id) as total_risky_orders
from DataCo
where Late_delivery_risk = 1
group by Shipping_Mode;

-- Q2.6: Find all products that had a non-zero discount but still resulted in a negative Order Profit Per Order.

select distinct Product_Name from DataCo
where Order_Item_Discount > 0 and Order_Profit_Per_Order < 0;

-- #################################################################################
-- # SECTION 3: ADVANCED QUERIES (Window Functions & CTEs)
-- #################################################################################

-- Q3.1: Use a Window Function (ROW_NUMBER()) to find the best-selling product (by Order Item Total) within each 
         'Department Name.'
		 
with ranked_sales AS (
    select Department_Name,Product_Name,sum(Order_Item_Total) as total_item_sales,
        row_number() over (partition by Department_Name order by sum(Order_Item_Total) desc) as rank_within_dept
    from DataCo
    group by 1, 2
)
select Department_Name,Product_Name,total_item_sales from ranked_sales AS RS
where RS.rank_within_dept = 1;

-- Q3.2: Calculate a running total of sales (Order Item Total) over time, ordered by Order date (DateOrders).

select order_date_DateOrders,Order_Item_Total,
    sum(Order_Item_Total) over (order by order_date_DateOrders asc) as running_total_sales from DataCo
order by order_date_DateOrders;

-- Q3.3: Use a CTE to calculate the Profit Margin (Order Profit Per Order / Order Item Total) for every order. 
        'Then, find the average Profit Margin for each Category Name and Customer Segment.'
		
with profit_margin_data AS (
    select Category_Name,Customer_Segment,(Order_Profit_Per_Order / Order_Item_Total) as profit_margin
    from DataCo
    where Order_Item_Total > 0 -- Avoid division by zero
)
select Category_Name,Customer_Segment,
    avg(PM.profit_margin) as avg_profit_margin
from profit_margin_data AS PM
group by 1, 2
order by avg_profit_margin desc;

-- Q3.4: Find customers who have placed orders from multiple Customer Country locations (Data Quality/Anomaly Check).

select Customer_Id from DataCo
group by Customer_Id
having count(distinct Customer_Country) > 1;

-- Q3.5: Determine the average time difference (in days) between the order date and the shipping date for each Market.

select Market,
    avg(extract(epoch from (shipping_date_DateOrders - order_date_DateOrders)) / (60*60*24)) as avg_shipping_days
from DataCo
where shipping_date_DateOrders is not null
group by Market;

-- #################################################################################
-- # SECTION 4: DATA DEFINITION LANGUAGE (DDL) & CONSTRAINTS
-- #################################################################################

-- Q4.1: Write the SQL (DDL) statement to add a constraint to the DataCo table, ensuring that Order Item Quantity 
        'is always greater than 0.'
		
alter table DataCo
add constraint chk_quantity_positive check (Order_Item_Quantity > 0);

-- Q4.2: Write the DDL to create a normalized Customers table, defining the primary key (Customer_Id), 
        'to demonstrate conceptual data modeling skills.'
		
create table customers (
    customer_id varchar primary key,
    customer_fname varchar,
    customer_lname varchar,
    customer_email varchar,
    customer_segment varchar,
    customer_country varchar,
    customer_city varchar,
    customer_state varchar,
    customer_street varchar,
    customer_zipcode varchar
);

 select * from customers;