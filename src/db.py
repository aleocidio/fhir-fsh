#%%
import sqlite3 as sl
import json
from datetime import datetime
import hashlib

string_encode = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

print(
hashlib.sha1(b'string_encode').hexdigest()
)

print(datetime.utcnow())


#%%
# Conexao com o banco de dados
db_conn = sl.connect('local_data.db')


def db_execute(con: sl.Connection(), sql_query: str, data=[]):
    """Executa uma query com a conexão, texto da query e dados passados.
    Args:
        con: conexão SQLite3 (sqlite3.connect('bancodedados.db))
        sql_query (str): texto da query. Parâmetros são passados como "?"
        data (list, optional): dados para preencher os parâmetros da query. devem ser enviados em tuplas.

    Returns:
        Retorna um cursor que pode ser percorrido em uma interação para exibir os resultados de cada linha.
    """    
    if len(data) > 1:
        with con:
            try:
                return con.executemany(sql_query, data)
            except sl.OperatoinaError as e:
                print(f"ERRO SQL: {e}")
    else:
        with con:
            try:
                return con.executemany(sql_query, data)
            except sl.OperatoinaError as e:
                print(f"ERRO SQL: {e}")


#db_cursor = db_conn.cursor()

# Cria os schemas do banco se não existirem
#df = pd.read_sql('''
#    SELECT s.user_id, u.name, u.age, s.skill 
#    FROM USER u LEFT JOIN SKILL s ON u.id = s.user_id
#''', con)

#df.to_sql('TABELA', con, if_exists='replace', index=False)


json_extracao = {
    '_id':'1234',
    'datetime':'2022-05-29',
    'project':'projeto_teste',
    'files':[],     
}


db_cursor.execute("CREATE TABLE recursos (id varchar(5), data json)")
db_cursor.execute("INSERT INTO recursos VALUES (?,?)", ('id_1', json.dumps(json_extracao)))
db_conn.commit()

data = db_cursor.execute("SELECT json_extract(data, '$.project') from recursos")
for row in data:
    print(row)

db_conn.close()
