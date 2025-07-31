-- Initialize MySQL database for CDC target
USE targetdb;

CREATE TABLE IF NOT EXISTS users_cdc (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(100),
    email VARCHAR(100),
    operation ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
    old_name VARCHAR(100) NULL,
    old_email VARCHAR(100) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_id (user_id),
    INDEX idx_operation (operation),
    INDEX idx_created_at (created_at)
);
