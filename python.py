import sqlite3
import pandas as pd
import numpy as np
import names


# Funkcja do generowania losowych miesięcznych wartości, które sumują się do wartości rocznej
def generate_monthly_values(total, n_months=12):
    random_values = np.random.rand(n_months)
    random_values /= random_values.sum()
    monthly_values = random_values * total
    return np.round(monthly_values, 2)


# Funkcja do generowania unikalnego imienia i nazwiska na podstawie płci
def generate_unique_name(gender, existing_names):
    while True:
        if gender == 'M':
            first_name = names.get_first_name(gender='male')
        else:
            first_name = names.get_first_name(gender='female')
        last_name = names.get_last_name()
        full_name = (first_name, last_name)
        if full_name not in existing_names:
            return full_name


# Połączenie z bazą danych SQLite
conn = sqlite3.connect('data_warehouse.db')
cursor = conn.cursor()

# Tworzenie tabeli Date
cursor.execute('''
CREATE TABLE IF NOT EXISTS Date (
    Date_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Month INTEGER,
    Year INTEGER,
    Quarter INTEGER
);
''')

# Wypełnianie tabeli Date
for year in range(2023, 2024):
    for month in range(1, 13):
        quarter = (month - 1) // 3 + 1
        cursor.execute('INSERT INTO Date (Month, Year, Quarter) VALUES (?, ?, ?)', (month, year, quarter))

# Wczytanie danych z pliku CSV
source_data = pd.read_csv('Employee_Salaries_-_2023.csv')

# Tworzenie tabeli Department
cursor.execute('''
CREATE TABLE IF NOT EXISTS Department (
    Department_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Department_Abbreviation VARCHAR(50),
    Department_Name VARCHAR(255),
    Division VARCHAR(255),
    UNIQUE (Department_Abbreviation, Department_Name, Division)
);
''')

# Wypełnianie tabeli Department
departments = source_data[['Department_abbreviation', 'Department_Name', 'Division']].drop_duplicates()
for i, row in departments.iterrows():
    cursor.execute('''
    INSERT INTO Department (Department_Abbreviation, Department_Name, Division)
    VALUES (?, ?, ?)
    ''', (row['Department_abbreviation'], row['Department_Name'], row['Division']))

# Tworzenie tabeli Employee
cursor.execute('''
CREATE TABLE IF NOT EXISTS Employee (
    Employee_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Gender CHAR(1),
    First_Name VARCHAR(255),
    Last_Name VARCHAR(255),
    UNIQUE (Gender, First_Name, Last_Name)
);
''')

# Tworzenie tabeli Salary_tmp do unikalnych wpisów
cursor.execute('''
CREATE TABLE IF NOT EXISTS Salary_tmp (
    Salary_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Base_Salary DECIMAL(10, 2),
    Overtime_Pay DECIMAL(10, 2),
    Longevity_Pay DECIMAL(10, 2),
    Grade VARCHAR(10),
    UNIQUE (Base_Salary, Overtime_Pay, Longevity_Pay, Grade)
);
''')

# Tworzenie tabeli Salary_Fact
cursor.execute('''
CREATE TABLE IF NOT EXISTS Salary_Fact (
    Salary_Fact_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Date_ID INTEGER,
    Employee_ID INTEGER,
    Department_ID INTEGER,
    Salary_ID INTEGER,
    Amount DECIMAL(10, 2),
    Minimum_Salary DECIMAL(10, 2),
    Average_Salary DECIMAL(10, 2),
    Maximum_Salary DECIMAL(10, 2),
    FOREIGN KEY (Date_ID) REFERENCES Date(Date_ID),
    FOREIGN KEY (Employee_ID) REFERENCES Employee(Employee_ID),
    FOREIGN KEY (Department_ID) REFERENCES Department(Department_ID),
    FOREIGN KEY (Salary_ID) REFERENCES Salary(Salary_ID)
);
''')

# Zbiór istniejących imion i nazwisk, aby uniknąć duplikatów
existing_names = set()

# Wypełnianie tabel Department, Employee, Salary_tmp danymi ze źródła
for index, row in source_data.iterrows():
    # Wstawianie do tabeli Employee
    first_name, last_name = generate_unique_name(row['Gender'], existing_names)
    existing_names.add((first_name, last_name))
    cursor.execute('''
    INSERT OR IGNORE INTO Employee (Gender, First_Name, Last_Name)
    VALUES (?, ?, ?)
    ''', (row['Gender'], first_name, last_name))

    # Wstawianie do tabeli Salary_tmp
    cursor.execute('''
    INSERT OR IGNORE INTO Salary_tmp (Base_Salary, Overtime_Pay, Longevity_Pay, Grade)
    VALUES (?, ?, ?, ?)
    ''', (round(float(row['Base_Salary']), 2), round(float(row['Overtime_Pay']), 2), round(float(row['Longevity_Pay']), 2), row['Grade']))

# Pobieranie ID wstawionych rekordów
department_ids = cursor.execute('SELECT Department_ID FROM Department').fetchall()
employee_ids = cursor.execute('SELECT Employee_ID FROM Employee').fetchall()
salary_tmp_ids = cursor.execute('SELECT Salary_ID, Base_Salary, Overtime_Pay, Longevity_Pay, Grade FROM Salary_tmp').fetchall()
date_ids = cursor.execute('SELECT Date_ID FROM Date').fetchall()

# Tworzenie słownika do mapowania Salary_tmp na nowe Salary_ID
salary_map = {}

# Tworzenie tabeli Salary z unikalnymi rekordami i ciągłymi ID
cursor.execute('DROP TABLE IF EXISTS Salary')
cursor.execute('''
CREATE TABLE Salary (
    Salary_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Base_Salary DECIMAL(10, 2),
    Overtime_Pay DECIMAL(10, 2),
    Longevity_Pay DECIMAL(10, 2),
    Grade VARCHAR(10),
    UNIQUE (Base_Salary, Overtime_Pay, Longevity_Pay, Grade)
);
''')

for row in salary_tmp_ids:
    cursor.execute('''
    INSERT INTO Salary (Base_Salary, Overtime_Pay, Longevity_Pay, Grade)
    VALUES (?, ?, ?, ?)
    ''', (row[1], row[2], row[3], row[4]))
    new_salary_id = cursor.lastrowid
    salary_map[(row[1], row[2], row[3], row[4])] = new_salary_id

# Sprawdzenie liczby wstawionych rekordów do tabeli Employee
print(f"Number of records in Employee table: {len(employee_ids)}")

# Przetwarzanie każdego rekordu z danych źródłowych
for index, row in source_data.iterrows():
    base_salary_monthly = generate_monthly_values(float(row['Base_Salary']))
    overtime_pay_monthly = round(float(row['Overtime_Pay']) / 12, 2)
    longevity_pay_monthly = round(float(row['Longevity_Pay']) / 12, 2)

    for month in range(1, 13):
        date_id = date_ids[month - 1][0]
        employee_id = employee_ids[index % len(employee_ids)][0]
        department_id = department_ids[index % len(department_ids)][0]
        grade = row['Grade'] if pd.notnull(row['Grade']) else None
        salary_id = salary_map[(round(float(row['Base_Salary']), 2), round(float(row['Overtime_Pay']), 2), round(float(row['Longevity_Pay']), 2), grade)]

        amount = round(base_salary_monthly[month - 1] + overtime_pay_monthly + longevity_pay_monthly, 2)

        cursor.execute('''
        INSERT INTO Salary_Fact (
            Date_ID, Employee_ID, Department_ID, Salary_ID, Amount, Minimum_Salary, Average_Salary, Maximum_Salary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            date_id, employee_id, department_id, salary_id,
            amount, round(base_salary_monthly.min(), 2), round(base_salary_monthly.mean(), 2),
            round(base_salary_monthly.max(), 2)
        ))

# Zapisanie zmian do bazy danych
conn.commit()

# Eksportowanie każdej tabeli do osobnego pliku CSV z przecinkami jako separatorami dziesiętnymi
tables = ['Date', 'Department', 'Employee', 'Salary', 'Salary_Fact']

for table in tables:
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df.to_csv(f"{table}.csv", index=False, decimal=',')

# Zamknięcie połączenia
conn.close()

print("Tabele zostały utworzone, wypełnione danymi i zapisane do plików CSV.")
