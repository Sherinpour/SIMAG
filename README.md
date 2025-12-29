# SIMAG - Smart Name Matching API

An intelligent system for finding similar names in databases using fuzzy matching algorithms and Persian text processing.

## 📋 Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Using as API](#using-as-api)
  - [Using as Script](#using-as-script)
- [Matching Parameters](#matching-parameters)
- [Project Structure](#project-structure)
- [API Documentation](#api-documentation)
- [Troubleshooting](#troubleshooting)

---

## Introduction

SIMAG is an advanced system for finding similar names in databases using fuzzy matching algorithms. The system has Persian text processing capabilities and can compare names considering various factors such as last name, first name, organization, position, and mobile number.

### Use Cases

- Identifying duplicate records in databases
- Finding similar names with different spellings
- Data deduplication
- Analysis and reporting of similar data

---

## Features

✅ **Persian Text Processing**: Uses `hazm` library for Persian text normalization  
✅ **Smart Fuzzy Matching**: Uses `rapidfuzz` for finding similar names  
✅ **Configurable Weighting**: Ability to adjust the weight of each factor in similarity calculation  
✅ **Database Connection**: Direct connection to SQL Server and data retrieval  
✅ **RESTful API**: Service delivery through FastAPI  
✅ **File Support**: Ability to process Excel and CSV files  
✅ **Performance Optimization**: Optimized for small to medium datasets  

---

## Prerequisites

- Python 3.10 or higher
- SQL Server (for API usage)
- ODBC Driver 17 for SQL Server (for database connection)

### Installing ODBC Driver

**Windows:**
```bash
# Download from Microsoft
# https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

**Linux (Ubuntu/Debian):**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

**Linux (RHEL/CentOS):**
```bash
sudo su
curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
exit
sudo ACCEPT_EULA=Y yum install -y msodbcsql17
```

---

## Installation

### 1. Clone or Download the Project

```bash
cd /path/to/project
```

### 2. Create Virtual Environment

```bash
python -m venv venv
```

### 3. Activate Virtual Environment

**Windows:**
```bash
venv\Scripts\activate
```

**Linux/Mac:**
```bash
source venv/bin/activate
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Configuration

### Database Settings

Create a `.env` file in the project root and enter database connection information:

```env
DB_SERVER=your_server_name
DB_NAME=your_database_name
DB_USERNAME=your_username
DB_PASSWORD=your_password
```

**Example:**
```env
DB_SERVER=localhost
DB_NAME=GEMS
DB_USERNAME=sa
DB_PASSWORD=YourPassword123
```

---

## Usage

### Using as API

#### 1. Start the Server

```bash
python main.py
```

Or with uvicorn:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The server will be available at `http://localhost:8000`.

#### 2. API Documentation

After starting the server, you can view the interactive API documentation at the following addresses:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

#### 3. Send Request

**Example with curl:**
```bash
curl -X POST "http://localhost:8000/find-similar-names" \
  -H "Content-Type: application/json" \
  -d '{
    "id": 123,
    "name_threshold": 0.78,
    "last_weight": 0.40,
    "first_weight": 0.10,
    "org_weight": 0.30,
    "post_weight": 0.15,
    "mobile_weight": 0.05,
    "min_freq": 3
  }'
```

**Example with Python:**
```python
import requests

url = "http://localhost:8000/find-similar-names"
payload = {
    "id": 123,
    "name_threshold": 0.78,
    "last_weight": 0.40,
    "first_weight": 0.10,
    "org_weight": 0.30,
    "post_weight": 0.15,
    "mobile_weight": 0.05,
    "min_freq": 3
}

response = requests.post(url, json=payload)
result = response.json()

print(f"Total pairs found: {result['total_pairs']}")
for pair in result['pairs']:
    print(f"{pair['name1']} <-> {pair['name2']}: {pair['similarity_score']}%")
```

**Example with JavaScript (fetch):**
```javascript
fetch('http://localhost:8000/find-similar-names', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    id: 123,
    name_threshold: 0.78,
    last_weight: 0.40,
    first_weight: 0.10,
    org_weight: 0.30,
    post_weight: 0.15,
    mobile_weight: 0.05,
    min_freq: 3
  })
})
.then(response => response.json())
.then(data => {
  console.log(`Total pairs: ${data.total_pairs}`);
  data.pairs.forEach(pair => {
    console.log(`${pair.name1} <-> ${pair.name2}: ${pair.similarity_score}%`);
  });
});
```

### Using as Script

You can use `smart_name_matcher2.py` directly to process Excel or CSV files:

```bash
python smart_name_matcher2.py input.xlsx --output_similar output.xlsx
```

#### Command Line Parameters

```bash
python smart_name_matcher2.py <input_file> [OPTIONS]

Required:
  input_file              Path to input file (CSV or Excel). Must contain 'FirstName' and 'LastName' columns.

Optional:
  --output_similar PATH   Output path for similar names file (default: final_smart_similar_names.xlsx)
  --name_threshold FLOAT Similarity threshold for considering names similar (0.0-1.0) (default: 0.78)
  --last_weight FLOAT    Weight for last name in scoring (default: 0.40)
  --first_weight FLOAT   Weight for first name in scoring (default: 0.10)
  --org_weight FLOAT     Weight for organization in scoring (default: 0.30)
  --post_weight FLOAT    Weight for post/position in scoring (default: 0.15)
  --mobile_weight FLOAT  Weight for mobile number in scoring (default: 0.05)
  --min_freq INT         Minimum frequency for extracting stop first names (default: 3)
  --stop_penalty FLOAT   Penalty multiplier for common first names (0.0-1.0) (default: 0.75)
  --use_bank_bonus BOOL  Whether to use bank bonus in scoring (True/False) (default: True)
```

#### Usage Examples

```bash
# Use with default settings
python smart_name_matcher2.py data.xlsx

# Set higher similarity threshold
python smart_name_matcher2.py data.xlsx --name_threshold 0.85

# Set custom weights
python smart_name_matcher2.py data.xlsx \
  --name_threshold 0.80 \
  --last_weight 0.50 \
  --first_weight 0.20 \
  --org_weight 0.20 \
  --post_weight 0.05 \
  --mobile_weight 0.05

# Disable bank bonus
python smart_name_matcher2.py data.xlsx --use_bank_bonus False

# Process CSV file
python smart_name_matcher2.py data.csv --output_similar results.csv
```

---

## Matching Parameters

The system uses a composite scoring algorithm that considers the following factors:

### Main Factors

| Factor | Default Weight | Description |
|--------|----------------|-------------|
| **Last Name** | 0.40 | Most important factor in matching |
| **Organization** | 0.30 | Organization name similarity |
| **Post/Position** | 0.15 | Post/organizational position similarity |
| **First Name** | 0.10 | First name similarity |
| **Mobile Number** | 0.05 | Mobile number similarity |

### Additional Features

- **Bank Bonus**: If bank names are similar (≥80%), an additional 0.05 points are added
- **Common Name Penalty**: Common names (with frequency ≥3) are penalized with a factor of 0.75
- **Conditional Post**: Post similarity is only calculated when organization similarity ≥70%
- **Mobile Threshold**: Mobile number is only considered if similarity ≥80%

### Score Calculation Formula

```
Final Score = (last_name_weight × last_name_similarity) +
              (first_name_weight × first_name_similarity) +
              (org_weight × org_similarity) +
              (post_weight × post_similarity) +
              (mobile_weight × mobile_similarity) +
              bank_bonus
```

---

## Project Structure

```
SIMAG/
│
├── main.py                      # Main FastAPI file
├── smart_name_matcher2.py       # Name processing and matching engine
├── requirements.txt             # Project dependencies
├── .env                         # Database settings (create this)
├── README.md                    # This file
│
└── __pycache__/                 # Compiled Python files
```

### File Descriptions

- **main.py**: Contains API endpoints and database connection
- **smart_name_matcher2.py**: Contains `SmartNameProcessor` class and name matching logic
- **requirements.txt**: List of all required dependencies

---

## API Documentation

### Endpoint: `POST /find-similar-names`

Find similar names based on Event ID

#### Request Body

```json
{
  "id": 123,
  "name_threshold": 0.78,
  "last_weight": 0.40,
  "first_weight": 0.10,
  "org_weight": 0.30,
  "post_weight": 0.15,
  "mobile_weight": 0.05,
  "min_freq": 3
}
```

#### Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id` | integer | ✅ | - | Event ID |
| `name_threshold` | float | ❌ | 0.78 | Similarity threshold (0.0-1.0) |
| `last_weight` | float | ❌ | 0.40 | Last name weight |
| `first_weight` | float | ❌ | 0.10 | First name weight |
| `org_weight` | float | ❌ | 0.30 | Organization weight |
| `post_weight` | float | ❌ | 0.15 | Post/position weight |
| `mobile_weight` | float | ❌ | 0.05 | Mobile number weight |
| `min_freq` | integer | ❌ | 3 | Minimum frequency for common names |

#### Response

```json
{
  "total_pairs": 5,
  "pairs": [
    {
      "name1": "Ali Ahmadi",
      "post1": "Manager",
      "org1": "National Bank",
      "org_type1": "Bank",
      "company1": "Company A",
      "holding1": "Holding A",
      "mobile1": "09123456789",
      "name2": "Ali Ahmadi",
      "post2": "CEO",
      "org2": "National Bank of Iran",
      "org_type2": "Bank",
      "company2": "Company A",
      "holding2": "Holding A",
      "mobile2": "09123456789",
      "similarity_score": 95.5
    }
  ]
}
```

#### Status Codes

- `200 OK`: Request successful
- `400 Bad Request`: Error in input parameters
- `500 Internal Server Error`: Server or database error

---

## Important Notes

### Required Database Columns

The `vw_Guest_AI` view must include the following columns:

- `ID` (Identifier)
- `FirstName` (First Name)
- `LastName` (Last Name)
- `BankTitle` (Bank Title)
- `Post` (Position)
- `OrganizationTitle` (Organization Title)
- `OrganizationTypeTitle` (Organization Type Title)
- `CompanyTitle` (Company Title)
- `HoldingTitle` (Holding Title)
- `MobileNumber` (Mobile Number)
- `IsHead` (Is Head/Manager)
- `EventId` (Event ID)

### Required Input File Columns

For direct script usage, the input file must include the following columns:

**Required:**
- `FirstName`
- `LastName`

**Optional (but recommended):**
- `OrganizationTitle`
- `BankTitle`
- `Post`
- `MobileNumber`
- `OrganizationTypeTitle`
- `CompanyTitle`
- `HoldingTitle`
- `IsHead`

---

## Troubleshooting

### Issue: Database Connection Error

**Solution:**
1. Check that the `.env` file is created and contains correct information
2. Make sure ODBC Driver 17 is installed
3. Verify that SQL Server is accessible
4. Check that the Event ID exists in the database

### Issue: Persian Text Processing Error

**Solution:**
1. Make sure the `hazm` library is properly installed:
   ```bash
   pip install hazm
   ```
2. Check that the input file is read with the correct encoding

### Issue: Empty Results Returned

**Solution:**
1. Decrease the similarity threshold (`name_threshold`)
2. Check that data exists in the database
3. Verify that the Event ID is correct

---

## Development and Contribution

To develop and improve the project:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## License

This project is released under the MIT License.

---

## Support

For questions and issues, please create an Issue in the repository.

---

## Changelog

### Version 1.0.0
- Initial release
- RESTful API support
- Persian text processing
- Smart fuzzy matching
- SQL Server connection
- Excel and CSV file support

---

**Made with ❤️ for intelligent Persian name processing**
