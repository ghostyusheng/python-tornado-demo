import pandas as pd
import prestodb
from sqlalchemy import create_engine
from tool.base import Base

from function.function import config


class Presto(Base):

    def __init__(self):
        self.ip = config("presto","ip")
        self.port = config("presto","port")


    def conn_hive(self, schema):
        return prestodb.dbapi.connect(
            host=self.ip,
            port=self.port,
            user='root',
            catalog='hive',
            schema=schema,
        )


    def conn_hbase(self):
        return prestodb.dbapi.connect(
            host=self.ip,
            port=self.port,
            user='root',
            catalog='phoenix',
            schema="default",
        )

    def conn_sync(self):
        return prestodb.dbapi.connect(
            host=self.ip,
            port=self.port,
            user='root',
            catalog='mysql',
            schema="sync",
        )

    def conn_rds_result_sync(self):
        return prestodb.dbapi.connect(
            host=self.ip,
            port=self.port,
            user='root',
            catalog='rds_result',
            schema="analysis",
        )

    def conn_hive_sqlalchemy(self, schema):
        URI = "presto://hadoop@" + self.ip + ":" + self.port + "/hive/" + schema
        return create_engine(URI)


    def conn_hbase_sqlalchemy(self):
        URI = "presto://hadoop@" + self.ip + ":" + self.port + " /phoenix/default"
        return create_engine(URI)

    def conn_sync_sqlalchemy(self):
        URI = "presto://hadoop@" + self.ip + ":" + self.port + " /mysql/sync"
        return create_engine(URI)

    def conn_rds_result_sqlalchemy(self, schema):
        URI = "presto://hadoop@" + self.ip + ":" + self.port + "/rds_result/" + schema
        return create_engine(URI)

    def query(self, conn, sql):
        print(sql)
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()


    def hbase_query(self, sql, typ="sqlalchemy"):
        if typ == "sqlalchemy":
            return pd.read_sql_query(sql, con=self.conn_hbase_sqlalchemy())
        elif typ == "original": 
            return self.query(self.conn_hbase(), sql)
        else:
            raise Exception("you can only use typ in (sqlalchemy, original)")


    def hive_query(self, sql, schema="tap_ods", typ="sqlalchemy"):
        if typ == "sqlalchemy":
            return pd.read_sql_query(sql, con=self.conn_hive_sqlalchemy(schema))
        elif typ == "original": 
            return self.query(self.conn_hive(schema), sql)
        else:
            raise Exception("you can only use typ in (sqlalchemy, original)")

    def sync_query(self, sql, typ="sqlalchemy"):
        if typ == "sqlalchemy":
            return pd.read_sql_query(sql, con=self.conn_sync_sqlalchemy())
        elif typ == "original":
            return self.query(self.conn_sync("sync"), sql)
        else:
            raise Exception("you can only use typ in (sqlalchemy, original)")

    def rds_result_query(self, sql, schema="analysis", typ="sqlalchemy"):
        if typ == "sqlalchemy":
            return pd.read_sql_query(sql, con=self.conn_rds_result_sqlalchemy(schema))
        elif typ == "original":
            return self.query(self.conn_rds_result_sync(schema), sql)
        else:
            raise Exception("you can only use typ in (sqlalchemy, original)")

    def hive_bulk_insert(self, data, table, schema="tap_dw", chunksize=2000):
        conn = self.conn_hive_sqlalchemy(schema).connect()
        data = pd.DataFrame(data)
        if isinstance(data, pd.DataFrame) and not data.empty:
            for col in data.columns:
                data[col] = data[col].apply(lambda x: x.replace("%", "%%"))
                data[col] = data[col].apply(lambda x: x.replace("'", ""))
            data = data.to_dict(orient='records')
        if data:
            start = 0
            num = len(data)
            while start < num:
                end = start + chunksize
                batch_data = data[start:end]
                start = end
                column = ",".join(f"{field}" for field in batch_data[0].keys())
                sql = f"""insert into {schema}.{table}({column}) values """
                values = ",".join(f"{tuple(i.values())}" for i in batch_data)
                sql += values
                # print(sql)
                pd.read_sql_query(sql, con=conn)



