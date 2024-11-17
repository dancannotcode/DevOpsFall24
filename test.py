import csv
import sqlite3

# File paths
sic_codes_path = 'SIC_Codes.csv'
ticker_ids_path = 'Ticker_IDs.csv'

# Connect to the database
conn = sqlite3.connect('mydatabase.db')
cursor = conn.cursor()
conn.row_factory = sqlite3.Row

# Create tables for SIC_Codes and Ticker_IDs with proper column names
cursor.execute("""
CREATE TABLE IF NOT EXISTS SIC_Codes (
    [SIC Code] TEXT,
    [Office] TEXT,
    [Industry Title] TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Ticker_IDs (
    [index] INTEGER,
    [CIK] TEXT,
    [Tickers] TEXT,
    [Company Name] TEXT,
    [name] TEXT,
    [sic] TEXT,
    [countryma] TEXT,
    [stprma] TEXT,
    [cityba] TEXT,
    [zipba] TEXT,
    [address1] TEXT,
    [address2] TEXT
)
""")

# Read and insert data from SIC_Codes.csv
with open(sic_codes_path, 'r') as sic_file:
    reader = csv.reader(sic_file)
    next(reader)  # Skip the header row
    for row in reader:
        cursor.execute(
            "INSERT INTO SIC_Codes ([SIC Code], [Office], [Industry Title]) VALUES (?, ?, ?)",
            (row[0], row[1], row[2])
        )

# Read and insert data from Ticker_IDs.csv
with open(ticker_ids_path, 'r') as ticker_file:
    reader = csv.reader(ticker_file)
    next(reader)  # Skip the header row
    for row in reader:
        cursor.execute(
            "INSERT INTO Ticker_IDs ([index], [CIK], [Tickers], [Company Name], [name], [sic], [countryma], [stprma], [cityba], [zipba], [address1], [address2]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row
        )

# Perform an outer join manually (SQLite does not natively support FULL OUTER JOIN)
query = """
SELECT DISTINCT
    [Industry Title],
    [SIC Code],
    [Tickers],
    [Company Name]
FROM SIC_Codes
LEFT JOIN Ticker_IDs
ON SIC_Codes.[SIC Code] = Ticker_IDs.[sic]

UNION

SELECT DISTINCT
    [Industry Title],
    [SIC Code],
    [Tickers],
    [Company Name]
FROM Ticker_IDs
LEFT JOIN SIC_Codes
ON SIC_Codes.[SIC Code] = Ticker_IDs.[sic]

"""

# Execute the query
cursor.execute(query)

# Fetch the rows
rows = cursor.fetchall()

# Print column headers
headers = [description[0] for description in cursor.description]
print("\t".join(headers))

# Print each row with column headers (access by index since UNION returns a tuple)
for row in rows:
    print("\t".join([str(row[index]) if row[index] is not None else "NULL" for index in range(len(headers))]))

# Commit changes and close the connection
conn.commit()
conn.close()
