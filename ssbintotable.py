from sqlalchemy import Column, Float, Date, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class NaacTable(Base):
    __tablename__ = 'ssb_naac'
    
    id = Column(Integer, primary_key=True)
    index = Column(Date)
    column_name = Column(ARRAY(String))
    value = Column(Float)

    def __init__(self, index, column, value):
        self.index = index
        self.column_name = column
        self.value = value
