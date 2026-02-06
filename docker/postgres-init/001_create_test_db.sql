-- Create a separate DB for integration tests
CREATE DATABASE taskharbor_test;

-- Ensure the same user can access it
GRANT ALL PRIVILEGES ON DATABASE taskharbor_test TO taskharbor;