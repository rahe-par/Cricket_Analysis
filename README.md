# Cricsheet Match Data Analysis Project

A comprehensive cricket data analysis pipeline that scrapes, processes, analyzes, and visualizes cricket match data from Cricsheet.org using Selenium, Python, SQL, and Power BI.

# Project Overview

This project automates the collection and analysis of cricket match data across different formats (Tests, ODIs, T20s). The pipeline includes web scraping, data processing, database management, SQL analysis, and interactive visualizations.

# Tech Stack

- *Web Scraping*: Selenium, ChromeDriver
- *Data Processing*: Python, Pandas, PyYAML
- *Database*: SQLite
- *Visualization*: Matplotlib, Seaborn, Power BI
- *Tools*: VS Code, Git

# Project Structure
cricsheet-analysis/
│
├── cricsheet_data/          # Downloaded and processed match data
│   ├── tests/               # Test match JSON files
│   ├── odis/                # ODI match JSON files  
│   └── t20s/                # T20 match JSON files
│
├── scrape_cricsheet.py      # Main scraping script (Selenium)
├── process_data.py          # Data cleaning and transformation
├── db.py                    # SQLite database creation
├── queries.py               # Analytical SQL queries
├── eda.py                   # Exploratory data analysis visualizations
├── cricket_analytics.db     # SQLite database file
└── README.md              # Project documentation

# Download ChromeDriver
1. Check Chrome version: `chrome://version/`
2. Download matching ChromeDriver from: https://chromedriver.chromium.org/
3. Update path in `scrape_cricsheet.py`


This project is for educational purposes as part of the GUVI Artificial Intelligence and Machine Learning program.

