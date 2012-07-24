import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base, Column


Session = sa.orm.sessionmaker()


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
    # dagstat_id = Column(sa.Integer, sa.ForeignKey('dagstat.dagstat_id'))
    # dagstat = sa.orm.relationship('DagStatistic')

    @property
    def is_processed(self):
        return self.time_completed is not None


# Job.source and Batch.source together specify the input data
#
# dag_config_hash: over time the dag_config file (and the program) can change
# that change might be harmful, so it is worth knowing if it happened



# class DagStatistic(Base):
#
#     __tablename__ = 'dagstat'
#
#     dagstat_id = Column(sa.Integer, primary_key=True, nullable=False)
#
#     item_count = Column(sa.Integer)
#     run_time = Column(sa.Interval)
#
#     nodes = sa.orm.relationship('DagNodeStatistic', back_populates='dag')
#
#
# class DagNodeStatistic(Base):
#
#     __tablename__ = 'dagnodestat'
#
#     dagnodestat_id = Column(sa.Integer, primary_key=True, nullable=False)
#
#     dagstat_id = Column(sa.Integer, sa.ForeignKey('dagstat.dagstat_id'))
#     dag = sa.orm.relationship('DagStatistic', back_populates='nodes')
#
#     node_name = Column(sa.String)
#     item_count = Column(sa.Integer)
#     success_count = Column(sa.Integer)
#     failure_count = Column(sa.Integer)
#     run_time = Column(sa.Interval)


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
