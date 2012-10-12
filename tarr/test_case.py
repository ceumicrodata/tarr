import unittest
import sqlalchemy
from contextlib import contextmanager
from ConfigParser import ConfigParser

import tarr.model


test_cfg = ConfigParser()
test_cfg.read('test.ini')
admin_connection_config = dict(test_cfg.items('connection-testdb-admin'))
test_connection_config = dict(test_cfg.items('connection-tarr-test'))
testdb_admin_commands = dict(test_cfg.items('commands-testdb-admin'))


@contextmanager
def admin_autocommit():
    engine = sqlalchemy.engine_from_config(admin_connection_config)
    conn = engine.connect()
    conn.execute('commit')
    def autocommit(sql):
        conn.execute(sql)
        conn.execute('commit')
    yield autocommit
    conn.close()
    engine.dispose()


class DbTestCase(unittest.TestCase):

    db_engine = None

    def setUp(self):
        super(DbTestCase, self).setUp()
        with admin_autocommit() as sqlexec:
            sqlexec(testdb_admin_commands['drop_test_db'])
            sqlexec(testdb_admin_commands['create_test_db'])
        self.db_engine = sqlalchemy.engine_from_config(test_connection_config)

    def tearDown(self):
        self.db_engine.dispose()
        self.db_engine = None
        with admin_autocommit() as sqlexec:
            sqlexec(testdb_admin_commands['drop_test_db'])
        super(DbTestCase, self).tearDown()

    @contextmanager
    def new_session(self):
        session = tarr.model.Session(bind=self.db_engine)
        yield session
        session.close()


class TarrApplicationTestCase(DbTestCase):

    session = None

    def setUp(self):
        super(TarrApplicationTestCase, self).setUp()
        tarr.model.init(self.db_engine)
        tarr.model.init_meta_with_schema(tarr.model.meta)
        self.session = tarr.model.Session()

    def tearDown(self):
        self.session.close()
        self.session = None
        tarr.model.shutdown()
        super(TarrApplicationTestCase, self).tearDown()
