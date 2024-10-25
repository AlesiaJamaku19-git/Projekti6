import pymysql
import csv
import os

# 1. MySQL connection setup
def create_connection(host, user, password, database=None):
    connection = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        cursorclass=pymysql.cursors.DictCursor
    )
    return connection

# 2. Function to create databases and tables with VARCHAR(8000)
def setup_databases_and_tables(conn):
    queries = [
        # Create databases
        "CREATE DATABASE IF NOT EXISTS raw;",
        "CREATE DATABASE IF NOT EXISTS stage;",
        "CREATE DATABASE IF NOT EXISTS hist;",

        # Use the hist database for hist layer table creation
        "USE hist;",

        # Create audit log table for triggers
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            action VARCHAR(8000),
            table_name VARCHAR(8000),
            action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,

        # Create job_fact table
        """
        CREATE TABLE IF NOT EXISTS job_fact (
            job_id INT AUTO_INCREMENT PRIMARY KEY,
            job_link VARCHAR(8000)
        );
        """,

        # Create dim_skill table
        """
        CREATE TABLE IF NOT EXISTS dim_skill (
            skill_id INT AUTO_INCREMENT PRIMARY KEY,
            skill_name VARCHAR(8000),
            version INT DEFAULT 1,
            start_date DATE,
            end_date DATE DEFAULT NULL,
            is_active TINYINT(1) DEFAULT 1
        );
        """,

        # Create job_skill_fact table
        """
        CREATE TABLE IF NOT EXISTS job_skill_fact (
            job_id INT,
            skill_id INT,
            FOREIGN KEY (job_id) REFERENCES job_fact(job_id),
            FOREIGN KEY (skill_id) REFERENCES dim_skill(skill_id)
        );
        """,
    ]

    with conn.cursor() as cursor:
        for query in queries:
            cursor.execute(query)
        conn.commit()

    # Move trigger creation here to ensure that the dim_skill table exists first
    create_triggers(conn)

def create_triggers(conn):
    trigger_queries = [
        # Trigger to set start_date to CURDATE() if NULL during insertion in dim_skill
        """
        CREATE TRIGGER before_insert_dim_skill
        BEFORE INSERT ON dim_skill
        FOR EACH ROW
        BEGIN
            IF NEW.start_date IS NULL THEN
                SET NEW.start_date = CURDATE();
            END IF;
        END;
        """,

        # Create triggers on job_fact for INSERT, UPDATE, DELETE actions
        """
        CREATE TRIGGER after_insert_job_fact
        AFTER INSERT ON job_fact
        FOR EACH ROW
        BEGIN
            INSERT INTO audit_log (action, table_name) VALUES ('INSERT', 'job_fact');
        END;
        """,

        """
        CREATE TRIGGER after_update_job_fact
        AFTER UPDATE ON job_fact
        FOR EACH ROW
        BEGIN
            INSERT INTO audit_log (action, table_name) VALUES ('UPDATE', 'job_fact');
        END;
        """,

        """
        CREATE TRIGGER after_delete_job_fact
        AFTER DELETE ON job_fact
        FOR EACH ROW
        BEGIN
            INSERT INTO audit_log (action, table_name) VALUES ('DELETE', 'job_fact');
        END;
        """
    ]

    with conn.cursor() as cursor:
        for query in trigger_queries:
            cursor.execute(query)
        conn.commit()

# 3. Stored Procedures: SCD Update, Add Job, and Reporting
def setup_stored_procedures(conn):
    procedures = [
        # SCD Type 2 Update for Skills
        """
        CREATE PROCEDURE update_skill(IN skill_name VARCHAR(8000), IN new_name VARCHAR(8000))
        BEGIN
            -- Deactivate the old skill
            UPDATE dim_skill
            SET is_active = 0, end_date = CURDATE()
            WHERE skill_name = skill_name AND is_active = 1;

            -- Insert the updated skill as a new version
            INSERT INTO dim_skill (skill_name, version, start_date, is_active)
            VALUES (new_name, 1, CURDATE(), 1);
        END;
        """,

        # Add New Job and Skills
        """
        CREATE PROCEDURE add_new_job(IN job_link VARCHAR(8000), IN skill_list TEXT)
        BEGIN
            -- Insert into job_fact
            INSERT INTO job_fact (job_link) VALUES (job_link);
            SET @job_id = LAST_INSERT_ID();
            
            -- Split and insert skills into dim_skill and job_skill_fact
            SET @skill = '';
            SET @skills_remaining = skill_list;

            WHILE LENGTH(@skills_remaining) > 0 DO
                SET @skill = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(skill_list, ',', numbers.n), ',', -1));
                SET @skills_remaining = SUBSTRING(@skills_remaining, LENGTH(@skill) + 2);
                
                INSERT IGNORE INTO dim_skill (skill_name) VALUES (@skill);
                SET @skill_id = LAST_INSERT_ID();

                INSERT INTO job_skill_fact (job_id, skill_id) VALUES (@job_id, @skill_id);
            END WHILE;
        END;
        """,

        # Generate Report: Job Count by Skill
        """
        CREATE PROCEDURE report_job_count_by_skill()
        BEGIN
            SELECT dim_skill.skill_name, COUNT(job_skill_fact.job_id) AS job_count
            FROM dim_skill
            JOIN job_skill_fact ON dim_skill.skill_id = job_skill_fact.skill_id
            GROUP BY dim_skill.skill_name;
        END;
        """
    ]

    with conn.cursor() as cursor:
        for procedure in procedures:
            cursor.execute(procedure)
        conn.commit()

# 4. Load CSV into the raw table
def from_csv_to_raw(conn, csv_file_path):
    # Create raw table
    create_raw_table_query = """
    CREATE TABLE IF NOT EXISTS raw.job_data_raw (
        job_link VARCHAR(8000),
        job_skills VARCHAR(8000)
    );
    """

    with conn.cursor() as cursor:
        cursor.execute(create_raw_table_query)
        conn.commit()

    # with open(csv_file_path, 'r') as file:

    with open(csv_file_path, mode='r', encoding='utf-8', errors='ignore') as file:
        reader = csv.DictReader(file)
        rows = [(row['job_link'], row['job_skills'])  for row in reader if len(row['job_skills']) <= 8000]
        
    query = "INSERT INTO raw.job_data_raw (job_link, job_skills) VALUES (%s, %s);"
    
    with conn.cursor() as cursor:
            cursor.executemany(query, rows)
            conn.commit()

# 5. Transfer and Normalize Data from Raw to Stage
def from_raw_to_stage(conn):
    # Create stage table
    create_stage_table_query = """
    CREATE TABLE IF NOT EXISTS stage.job_data_stage (
        job_link VARCHAR(8000),
        skill VARCHAR(8000)
    );
    """

    with conn.cursor() as cursor:
        cursor.execute(create_stage_table_query)
        conn.commit()

    # Transform data from raw and insert it into the stage table
    query = """
    INSERT INTO stage.job_data_stage (job_link, skill)
    SELECT job_link, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(job_skills, ',', numbers.n), ',', -1)) AS skill
    FROM raw.job_data_raw
    JOIN (SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) numbers
    ON CHAR_LENGTH(job_skills) - CHAR_LENGTH(REPLACE(job_skills, ',', '')) >= numbers.n - 1;
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()

# 6. Transfer Stage Tables to Hist Layer with Stored Procedure
def from_stage_to_hist(conn):
    query = """
    INSERT INTO hist.job_fact (job_link)
    SELECT DISTINCT job_link FROM stage.job_data_stage;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)

    query = """
    INSERT INTO hist.job_skill_fact (job_id, skill_id)
    SELECT j.job_id, s.skill_id
    FROM hist.job_fact j
    JOIN stage.job_data_stage s ON j.job_link = s.job_link;
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()

# 7. Full ETL Pipeline
def full_etl_pipeline(host, user, password, csv_file_path):
    # Step 1: Create a connection without specifying a database
    conn = create_connection(host, user, password)

    # Step 2: Set up the databases, tables, and stored procedures
    setup_databases_and_tables(conn)
    setup_stored_procedures(conn)

    # Step 3: Load CSV data into the raw table
    from_csv_to_raw(conn, csv_file_path)

    # Step 4: Transform and move data to the stage table
    from_raw_to_stage(conn)

    # Step 5: Move data from stage to hist
    from_stage_to_hist(conn)

    # Close the connection
    conn.close()

# Example usage:
if __name__ == "__main__":
    # MySQL connection details
    HOST = "localhost"
    USER = "root"
    PASSWORD = "2024"
    

    # Get the directory path where the script is executing
    script_path = os.path.dirname(os.path.abspath(__file__))

    # Specify the CSV file name
    csv_filename = "job-skills.csv"  

    # Construct the full path to the CSV file
    CSV_FILE_PATH = os.path.join(script_path, csv_filename)
    
    print("Path of the file is :", CSV_FILE_PATH)
    
    # Run the ETL pipeline
    full_etl_pipeline(HOST, USER, PASSWORD, CSV_FILE_PATH)
