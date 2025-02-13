CREATE TABLE product_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    product_id VARCHAR(20),
    category_code VARCHAR(255),
    brand VARCHAR(255),
    price DECIMAL(10,2),
    user_id VARCHAR(20),
    event_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- You can add a foreign key constraint if needed
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX idx_product_orders_product_id ON product_orders (product_id);
CREATE INDEX idx_product_orders_user_id ON product_orders (user_id);