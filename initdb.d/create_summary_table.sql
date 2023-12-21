CREATE TABLE IF NOT EXISTS summary (
    date DATE,
    turbine_id INTEGER,
    power_std_dev FLOAT,
    min_power_output FLOAT,
    max_power_output FLOAT,
    avg_power_output FLOAT
);