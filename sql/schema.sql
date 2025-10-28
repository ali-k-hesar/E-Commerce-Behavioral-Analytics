-- =================================================================================
-- STEP 1: CREATE OPTIMIZED SCHEMA
-- =================================================================================

-- Create supporting types
CREATE TYPE public.eval_set_enum AS ENUM ('prior', 'train', 'test');

-- Small Dimension Tables (using SMALLINT for efficiency)
CREATE TABLE public.departments (
    department_id SMALLINT PRIMARY KEY,
    department TEXT NOT NULL
);

CREATE TABLE public.aisles (
    aisle_id SMALLINT PRIMARY KEY,
    aisle TEXT NOT NULL
);

CREATE TABLE public.products (
    product_id INT PRIMARY KEY,
    product_name TEXT NOT NULL,
    aisle_id SMALLINT NOT NULL REFERENCES public.aisles(aisle_id),
    department_id SMALLINT NOT NULL REFERENCES public.departments(department_id)
);

-- Fact Table Structure
CREATE TABLE public.orders (
    order_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    eval_set public.eval_set_enum NOT NULL,
    order_number SMALLINT NOT NULL,
    order_dow SMALLINT NOT NULL,
    order_hour_of_day SMALLINT NOT NULL,
    days_since_prior_order SMALLINT -- Allows NULL for first orders, uses efficient SMALLINT
);

CREATE TABLE public.order_products (
    order_id INT NOT NULL REFERENCES public.orders(order_id), -- FOREIGN KEY
    product_id INT NOT NULL REFERENCES public.products(product_id), -- FOREIGN KEY
    add_to_cart_order SMALLINT NOT NULL,
    reordered BOOLEAN NOT NULL, -- Uses efficient BOOLEAN
    PRIMARY KEY (order_id, product_id)
);


-- =================================================================================
-- STEP 2: LOAD DIMENSION TABLES (Parent Tables)
-- =================================================================================

COPY public.departments FROM 'Datasets\Dataset2-InstaCartOnlineGroceryBasketAnalysisDataset\departments.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

COPY public.aisles FROM 'Datasets\Dataset2-InstaCartOnlineGroceryBasketAnalysisDataset\aisles.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

COPY public.products FROM 'Datasets\Dataset2-InstaCartOnlineGroceryBasketAnalysisDataset\products.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');


-- =================================================================================
-- STEP 3: CLEAN AND LOAD ORDERS TABLE
-- =================================================================================

-- 3.1: Create Staging table to hold raw, possibly dirty data
CREATE TABLE public.orders_stg (
    order_id TEXT, user_id TEXT, eval_set TEXT, order_number TEXT,
    order_dow TEXT, order_hour_of_day TEXT, days_since_prior_order TEXT 
);

-- 3.2: Load Raw Data
COPY public.orders_stg FROM 'Datasets\Dataset2-InstaCartOnlineGroceryBasketAnalysisDataset\orders.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- 3.3: Cleanse and Insert into final table (Handling the "15.0" error)
INSERT INTO public.orders (
    order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order
)
SELECT 
    order_id::INT, 
    user_id::INT, 
    eval_set::public.eval_set_enum, 
    order_number::SMALLINT, 
    order_dow::SMALLINT, 
    order_hour_of_day::SMALLINT, 
    CASE 
        WHEN days_since_prior_order IS NULL OR days_since_prior_order = '' 
        THEN NULL
        ELSE days_since_prior_order::REAL::SMALLINT 
    END AS days_since_prior_order
FROM public.orders_stg;

-- 3.4: Cleanup staging table
DROP TABLE public.orders_stg;


-- =================================================================================
-- STEP 4: LOAD ORDER PRODUCTS (Child Table)
-- =================================================================================

-- Load the PRIOR orders (The source of your original error)
COPY public.order_products FROM 'Datasets\Dataset2-InstaCartOnlineGroceryBasketAnalysisDataset\order_products__prior.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

-- Load the TRAIN orders (Appending the second data file)
COPY public.order_products FROM 'Datasets\Dataset2-InstaCartOnlineGroceryBasketAnalysisDataset\order_products__train.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');


-- =================================================================================
-- STEP 5: CREATE ANALYTIC INDEXES
-- =================================================================================

CREATE INDEX idx_orders_user_id ON public.orders(user_id);
CREATE INDEX idx_order_products_product_id ON public.order_products(product_id);
CREATE INDEX idx_products_aisle_id ON public.products(aisle_id);
CREATE INDEX idx_products_department_id ON public.products(department_id);