import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands            
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
     try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            header = next(reader)  
            print(f"Header: {header}")  
            if header != ["firstName", "lastName"]:  
                print("Skipping file: Incorrect headers")
                return
            cursor.execute("DELETE FROM users")  
            userId = 1  
            valid_count = 0  
            for row in reader:
                row = [col.strip() for col in row]  
                if len(row) != 2:
                    print(f"Skipping row (incorrect column count): {row}")
                    continue
                firstName, lastName = row[0], row[1]
                if firstName and lastName: 
                    cursor.execute("INSERT INTO users (userId, firstName, lastName) VALUES (?, ?, ?)",
                                   (userId, firstName, lastName))
                    userId += 1  
                    valid_count += 1
                else:
                    print(f"Skipping row (empty first/last name): {row}")
        conn.commit()
        print(f"Users data loaded successfully. Inserted {valid_count} records.")
     except Exception as e:
        print(f"Error loading users: {e}")

def load_and_clean_call_logs(file_path):
     try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  
            for row in reader:
                if len(row) != 5:  
                    print(f"Skipping row (incorrect column count): {row}")
                    continue
                try:
                    phoneNumber = row[0]
                    startTime, endTime = int(row[1]), int(row[2])
                    direction = row[3].strip()
                    userId = int(row[4])
                    if phoneNumber and direction:  
                        cursor.execute("INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId) VALUES (?, ?, ?, ?, ?)",
                                       (phoneNumber, startTime, endTime, direction, userId))
                except ValueError as e:
                    print(f"Skipping row (ValueError: {e}): {row}")
        conn.commit()
     except Exception as e:
        print(f"Error loading call logs: {e}")
    
# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    try:
        cursor.execute('''
            SELECT userId, 
                   AVG(endTime - startTime) AS avgDuration, 
                   COUNT(*) AS numCalls 
            FROM callLogs 
            GROUP BY userId
        ''')
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['userId', 'avgDuration', 'numCalls'])
            for row in cursor.fetchall():
                writer.writerow(row)
        print(f"User analytics written to {csv_file_path}.")
    except Exception as e:
        print(f"Error writing user analytics: {e}")


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
     try:
        cursor.execute('''
            SELECT * FROM callLogs 
            ORDER BY userId, startTime
        ''')
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])
            for row in cursor.fetchall():
                writer.writerow(row)
        print(f"Ordered calls written to {csv_file_path}.")
     except Exception as e:
        print(f"Error writing ordered calls: {e}")


# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
