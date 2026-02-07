CREATE TABLE IF NOT EXISTS orders (
  id SERIAL PRIMARY KEY,
  user_id INT NOT NULL,
  amount NUMERIC(10,2) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id);

INSERT INTO orders (user_id, amount) VALUES
  (1, 99.99),
  (1, 19.99),
  (2, 39.99),
  (3, 12.50);
