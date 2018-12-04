import mysql.connector
from mysql.connector import errorcode

conn = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    password = 'carmine',
    database = 'flux'
)

print('Connection successful')
print('Database statistics: ', conn.cmd_statistics())


def create_db(db_name):
    try:

        cursor = conn.cursor()
        query = '''
                CREATE DATABASE IF NOT EXISTS {db};
                '''.format(db=db_name)
        cursor.execute(query)
        
    except Exception as err:
        return err

    finally:
        conn.close()


def create_table(tbl_name):

    try:
        cursor = conn.cursor(tbl_name)
        query = '''
                CREATE TABLE {tbl} (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(255) NOT NULL,
                    age INT NOT NULL
                );
                '''.format(tbl=tbl_name)
        cursor.execute(query)
        
    except Exception as err:
        return err

    finally:
        conn.close()


# SQL Formulas: SQL formulas have placeholders '%s' for dynamic values
sql_formula = 'INSERT INTO students (name, age) VALUES (%s, %s, %s)'
student_01 = ['saur', 40]
student_02 = ('aeon', 45)
student_list = [
    ('john', 23),
    ('kate', 28),
    ('jill', 38),
    ('jane', 29),
    ('paul', 42)
]


def insert_student(st):

    try:
        cursor = conn.cursor()
        sql_formula = "INSERT INTO students (name, age) VALUES (%s, %s)"
        cursor.execute(sql_formula, st)

    except Exception as err:
        return err
    
    finally:
        conn.commit()


# mysql.connector.executemany(): Lets you run a SQL query over an iterator

def bulk_insert_students(ls):

    try:
        cursor = conn.cursor()
        sql_formula = "INSERT INTO students (name, age) VALUES (%s, %s)"
        cursor.executemany(sql_formula, ls)

    except Exception as err:
        return err

    finally:
        conn.commit()


def print_students():

    try:
        cursor = conn.cursor()
        sql_formula = "SELECT * FROM students"
        cursor.execute(sql_formula)
        data = cursor.fetchall()
        
        for row in data:
            print(row)

    except Exception as err:
        return err

    finally:
        pass


bulk_insert_students(student_list)

print_students()

conn.close()