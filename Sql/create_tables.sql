CREATE TABLE IF NOT EXISTS daily_cases (
    id INT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE NOT NULL,
    country_name VARCHAR(255) NOT NULL,
    total_cases BIGINT NULL,
    new_cases INT NULL,
    total_deaths BIGINT NULL,
    new_deaths INT NULL,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (country_name, report_date)
);

CREATE TABLE IF NOT EXISTS vaccination_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    report_date DATE NOT NULL,
    country_name VARCHAR(255) NOT NULL,
    total_vaccinations BIGINT NULL,
    people_vaccinated BIGINT NULL,
    people_fully_vaccinated BIGINT NULL,
    etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (country_name, report_date)
);