import clr
import json
import os
from command_strings import *
from preprocessing import clean_data_plataforma, clean_data_pipedrive, merge_bases
from fetch_data import *

clr.AddReference('System.Data')
from System.Data import SqlClient

def insert_sql_server(sqlCommandString, sqlConnectionString):
    output_string = ''
    transactionName = 'pipe_sync'    
    sqlDbConnection = SqlClient.SqlConnection(sqlConnectionString)
    sqlDbConnection.Open()

    transaction = sqlDbConnection.BeginTransaction(transactionName)

    command = sqlDbConnection.CreateCommand()
    command.Connection = sqlDbConnection
    command.Transaction = transaction
    command.CommandText = sqlCommandString

    try:
        output_string += str(command.ExecuteNonQuery())
        transaction.Commit()
    
    except Exception as e:
        output_string += transactionName + '\n'
        output_string += e.Message + '\n' * 2
    
        try:
            transaction.Rollback()
      
        except Exception as e:
            output_string += f'{transactionName} Rollback' + '\n'
            output_string += e.Message + '\n' * 2
  
    sqlDbConnection.Close()
    return output_string


#Inicia conexão ao postgre
postConnectionString_plataforma = os.environ['postgre_connection_string_plataforma']
postConnectionString_pipedrive = os.environ['postgre_connection_string_pipedrive']

# Obtém tabelas com os dados do pipe de 2 bancos diferentes
data_table_plataforma = fetch(
    postConnectionString_plataforma,  
    postgre_command(
        columns=['id', 'idpipe', 'cliente', 'geracao_total', 'kwp', 'distribuidora',  'tipooferta', 'etapa', 'funil', 'dataganho'],
        table='begyn1.vw_basegeral_eng'
    )
)

data_table_pipedrive = fetch(
    postConnectionString_pipedrive,  
    postgre_command(
        columns=['id', 'title', 'value', '"50b1fffdfe0575276a5d03c80014aa41c14c4369"', '"40a0d047040b5cd0cc54962eecb1b6d8ced865c4"'],
        table='public.carganegocios3'
    )
)

#Limpa os dados da tabela e retorna um dataframe
df_plataforma = clean_data_plataforma(data_table_plataforma)
df_pipedrive = clean_data_pipedrive(data_table_pipedrive)

df = merge_bases(
    main_df = df_pipedrive, 
    secondary_df = df_plataforma
)

# Transforma os dados em JSON
records = json.dumps(
    df.to_dict('records'), 
    default=str
)


# Envia JSON para o banco
response = insert_sql_server(
    sqlCommandString=sqlserver_command(records), 
    sqlConnectionString=os.environ['sqlserver_connection_string']
)
#Resposta do sistema
print(response)