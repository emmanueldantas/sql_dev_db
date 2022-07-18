def postgre_command(columns, table):
    return f'select {columns} from {table}'.replace('[','').replace("'",'').replace(']','')

def sqlserver_command(records):
    return f"exec dbo.sp_sync_pipe @data= '{records}'"