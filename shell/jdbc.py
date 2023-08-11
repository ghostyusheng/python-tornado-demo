# -*- coding: utf8 -*-

from core.const import const
import jaydebeapi
import os
import sys
from os.path import dirname, join, abspath

BASEDIR = abspath(join(dirname(__file__), '..'))
sys.path.append(BASEDIR)


class Jdbc:
    base_sql = [
        "set hive.mapred.mode=nonstrict",
        "set hive.strict.checks.cartesian.product=false",
        "set hive.execution.engine=tez"
    ]

    def query(self, sql, db='tap_ods'):
        url = 'jdbc:hive2://{}/'.format(const.HADOOP) + db
        dirver = 'org.apache.hive.jdbc.HiveDriver'
        DIR = BASEDIR + '/shell/lib/'
        jarFile = [
            DIR + 'hive-jdbc-3.1.1.jar',
            DIR + 'commons-logging-1.2.jar',
            DIR + 'hive-service-3.1.1.jar',
            DIR + 'hive-service-rpc-3.1.1.jar',
            DIR + 'libthrift-0.12.0.jar',
            DIR + 'httpclient-4.5.9.jar',
            DIR + 'httpcore-4.4.11.jar',
            DIR + 'slf4j-api-1.7.26.jar',
            DIR + 'curator-framework-4.2.0.jar',
            DIR + 'curator-recipes-4.2.0.jar',
            DIR + 'curator-client-4.2.0.jar',
            DIR + 'commons-lang-2.6.jar',
            DIR + 'hadoop-common-3.2.0.jar',
            DIR + 'hive-common-3.1.1.jar',
            DIR + 'hive-serde-3.1.1.jar',
            DIR + 'guava-28.0-jre.jar',
        ]
        conn = jaydebeapi.connect(dirver, url, ['hadoop', ''], jarFile)
        curs = conn.cursor()
        for _sql in self.base_sql:
            curs.execute(_sql)
        curs.execute(sql)
        result = curs.fetchall()
        curs.close()
        conn.close()
        return result
