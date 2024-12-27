CREATE TABLE public.wh_inventory_analytics (
    -- Original fields
    Date DATE,
    Product_Category VARCHAR(50),
    Sales_Volume INTEGER,
    Price DECIMAL(10,2),
    Promotion INTEGER,
    Store_Location VARCHAR(50),
    Weekday INTEGER,
    Supplier_Cost DECIMAL(10,2),
    Replenishment_Lead_Time INTEGER,
    Stock_Level INTEGER,
    
    -- Calculated fields
    Revenue DECIMAL(12,2),
    Cost DECIMAL(12,2),
    Profit DECIMAL(12,2),
    Profit_Margin DECIMAL(5,2),
    Stock_Status VARCHAR(10),
    Year_Month VARCHAR(7),
    Days_To_Replenish INTEGER,
    
    -- Optional: Add sorting key based on common query patterns
    PRIMARY KEY (Date, Product_Category, Store_Location)
)
DISTSTYLE KEY
DISTKEY (Product_Category)
SORTKEY (Date, Store_Location);
