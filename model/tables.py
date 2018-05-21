# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
Base = declarative_base()


class A(Base):
    __tablename__ = 'A'
    id = Column(Integer, primary_key=True)
    code = Column(String(20), name="Customer No.")
    posting_date = Column(DateTime, name="Posting Date")
    amount = Column(Integer)
