import sqlalchemy as sa
import sqlalchemy.orm
from sqlalchemy.ext.declarative import declarative_base, Column
from zope.dottedname.resolve import resolve as dottedname_resolve
from datetime import timedelta


Session = sa.orm.sessionmaker()
_engine = None


TARR_SCHEMA = 'tarr'
meta = sa.MetaData(schema=TARR_SCHEMA)


Base = declarative_base(metadata=meta)


class Job(Base):

    __tablename__ = 'job'

    job_name = Column(sa.String, primary_key=True, nullable=False)
    time_created = Column(sa.DateTime, server_default=sa.text('current_timestamp'))
    application = Column(sa.String, nullable=False)
    dag_config = Column(sa.String)
    dag_config_hash = Column(sa.String)
    partitioning_name = Column(sa.String)
    source = Column(sa.String) # 'complex:rovat_13:pc' for data in complex db table rovat_13 selecting fields starting with 'pc': pcirsz, pchely, pcteru, ...
    # params = Column(sa.Text) # (json?)
    description = Column(sa.String)
    # state (ongoing/finished?)

    batches = sa.orm.relationship('Batch', back_populates='job')

    def get_application_instance(self):
        cls = dottedname_resolve(self.application)
        app = cls()
        app.job = self
        return app

    def create_batch(self, source):
        batch = Batch()
        batch.source = source
        self.batches.append(batch)
        return batch


# application: a dotted name resolving to a tarr.application.Application that can process this job
# dag_config: it is not the full dag_config, rather an identifier (dotted path)
# the full dag_config might be stored in another table (it might change over time!)
#
# source/params:
# to define data source? ('rovat_5', 'rovat_6', 'rovat_7')
# to give variations ('rovat_13' has 3 location variations with prefixes: '' 'p' and 'pc' ('rovat_14', 'rovat_15' has 4 by introducting another prefix 'pm'))
# to give data type (like X in: "location for X")


class Batch(Base):

    __tablename__ = 'batch'

    batch_id = Column(sa.Integer, primary_key=True, nullable=False)
    job_name = Column(sa.String, sa.ForeignKey('job.job_name'))
    job = sa.orm.relationship('Job', back_populates='batches')
    source = Column(sa.String) # complex partition_id as string

    time_completed = Column(sa.DateTime)
    dag_config_hash = Column(sa.String)
    runstat_id = Column(sa.Integer, sa.ForeignKey('runstat.runstat_id'))
    runstat = sa.orm.relationship('RunStatistic')

    @property
    def is_processed(self):
        return self.time_completed is not None

    def save_statistics(self, dag):
        self.runstat = RunStatistic()
        for node in dag.nodes:
            self.runstat.nodes.append(NodeStatistic.clone(node))

    def merge_statistics_into(self, dag):
        for nodestat in self.runstat.nodes:
            node = dag.node_by_name(nodestat.node_name)
            NodeStatistic.merge(node, nodestat)


# Job.source and Batch.source together specify the input data
#
# dag_config_hash: over time the dag_config file (and the program) can change
# that change might be harmful, so it is worth knowing if it happened


class RunStatistic(Base):

    __tablename__ = 'runstat'

    runstat_id = Column(sa.Integer, primary_key=True, nullable=False)

    # item_count = Column(sa.Integer)
    # run_time = Column(sa.Interval)

    nodes = sa.orm.relationship('NodeStatistic', back_populates='runstat')


class NodeStatistic(Base):

    __tablename__ = 'dagnodestat'

    nodestat_id = Column(sa.Integer, primary_key=True, nullable=False)

    runstat_id = Column(sa.Integer, sa.ForeignKey('runstat.runstat_id'))
    runstat = sa.orm.relationship('RunStatistic', back_populates='nodes')

    node_name = Column(sa.String)
    item_count = Column(sa.Integer)
    success_count = Column(sa.Integer)
    failure_count = Column(sa.Integer)
    run_time = Column(sa.Interval)

    def init(self, node_name):
        self.node_name = node_name
        self.item_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.run_time = timedelta()

    @staticmethod
    def merge(into, from_stat):
        into.name = from_stat.node_name
        into.item_count += from_stat.item_count
        into.success_count += from_stat.success_count
        into.failure_count += from_stat.failure_count
        into.run_time += from_stat.run_time

    @classmethod
    def clone(cls, node):
        new_node = cls()
        new_node.node_name = node.name
        new_node.item_count = node.item_count
        new_node.success_count = node.success_count
        new_node.failure_count = node.failure_count
        new_node.run_time = node.run_time
        return new_node


def ensure_schema(sqlalchemy_engine, schema):
    c = sqlalchemy_engine.connect()
    c.execution_options(autocommit=True)
    try:
        c.execute('CREATE SCHEMA {}'.format(schema))
    except sa.exc.ProgrammingError as e:
        if 'already exists' not in e.message:
            raise
    finally:
        c.close()


# FIXME: init is untested!
def init(sqlalchemy_engine):
    '''
    Initialize TARR database objects

    Ensure that all used tables are available and potentially link TARR to the given engine.

    Call before using any db operation.
    '''
    # create missing objects
    # create schema - sqlalchemy 0.7.9 do not do it by itself, thus failing in create_all
    ensure_schema(sqlalchemy_engine, TARR_SCHEMA)
    meta.create_all(sqlalchemy_engine, checkfirst=True)

    Session.configure(bind=sqlalchemy_engine)


# FIXME: init_from is untested!
def init_from(args):
    connect_string = 'postgresql://{0.user}:{0.password}@{0.host}:{0.port}/{0.database}'.format(args)

    global _engine
    _engine = sqlalchemy.create_engine(connect_string)
    init(_engine)


def shutdown():
    global _engine
    if _engine:
        _engine.dispose()
        _engine = None
