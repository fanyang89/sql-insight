CREATE DATABASE IF NOT EXISTS app;
USE app;

CREATE TABLE IF NOT EXISTS orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  KEY idx_user (user_id)
);

CREATE TABLE IF NOT EXISTS deadlock_test (
  id INT PRIMARY KEY,
  v INT NOT NULL
);

INSERT INTO orders (user_id, amount) VALUES
  (1, 99.99),
  (1, 19.99),
  (2, 39.99),
  (3, 12.50)
ON DUPLICATE KEY UPDATE amount = VALUES(amount);

INSERT INTO deadlock_test (id, v) VALUES
  (1, 0),
  (2, 0)
ON DUPLICATE KEY UPDATE v = VALUES(v);
