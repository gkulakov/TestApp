# -*- coding: utf-8 -*-

from logbook import Logger, FileHandler
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, event
from contextlib import contextmanager
import time

from .tables import Base

FileHandler("log.txt").push_application()
logger = Logger("myapp.sqltime")


class DataAccessLayer:

    def __init__(self):
        self.engine = None
        self.newSession = None
        self.session = None

    def connect(self):
        self.engine = create_engine('sqlite:///foo.db', pool_recycle=300)
        # self.engine.echo = True
        self.newSession = sessionmaker(bind=self.engine)
        self.session = self.newSession()

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session_context(self):
        """Provide a transactional scope around a series of operations."""
        session = self.newSession()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                          parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    logger.debug("Start Query: \n{}".format(statement))
    logger.debug("Query arguments: {}".format(parameters))


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                         parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    logger.debug("Query Complete!")
    logger.debug("Total Time: {:f}".format(total))
