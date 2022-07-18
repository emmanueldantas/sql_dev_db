import pyodbc

def transpose(rows):
    return [column for column in zip(*rows)]

def values_as_string(rows):
    return [map(str, row) for row in rows]


def fetch(connectionString, commandString):
    cnxn = pyodbc.connect(connectionString)
    
    cursor = cnxn.cursor()
    cursor.execute(commandString)

    rows = cursor.fetchall()
    rows = values_as_string(rows)
    data_table = transpose(rows)

    return data_table