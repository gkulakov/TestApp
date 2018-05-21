# -*- coding: utf-8 -*-

import glob
import xml.etree.ElementTree as ET
from numpy import genfromtxt
from model.database import DataAccessLayer, logger
from model import tables
from datetime import datetime


def load_data_xml(xml_file):
    with open(xml_file) as fobj:
        xml = fobj.read()

    root = ET.fromstring(xml)
    data = []
    for appt in root.getchildren():
        data.append(tuple(elem.text for elem in appt.getchildren()))
    return data


def load_data_csv(file_name):
    data = genfromtxt(file_name,
                      delimiter=';',
                      encoding='utf-8',
                      converters={0: lambda s: str(s),
                                  1: lambda s: str(s),
                                  2: lambda s: int(s)})
    return data.tolist()


dal = DataAccessLayer()
dal.connect()
# dal.create_tables()


def start_import(import_data):
    # Create the session context
    with dal.session_context() as s:

        try:
            if not isinstance(import_data, list):
                import_data = []
                import_data.append(load_data_csv(p))

            for i in import_data:
                code = i[0]
                date = datetime.strptime(i[1], "%d%m%Y")
                amount = int(i[2])
                # Importing amount must be positive.
                # Zero amount is not acceptable
                if amount <= 0:
                    logger.debug("Amount <= 0 !!!")
                    continue

                record = s.query(tables.A)\
                    .filter(tables.A.code == code,
                            tables.A.posting_date == date).first()
                if record:
                    record.posting_date = date
                    # The summarized amount per day
                    # must be less than 1000 for each customer
                    if record.amount + amount >= 1000:
                        record.amount = 1000
                    else:
                        record.amount += amount
                    s.add(record)  # Add all the records
                else:
                    record = tables.A()
                    record.code = code
                    record.posting_date = date
                    record.amount = amount
                    s.add(record)  # Add all the records

            s.commit()  # Attempt to commit all the records
        except Exception as e:
            s.rollback()  # Rollback the changes on error
            logger.debug(e)
        finally:
            s.close()  # Close the connection


if __name__ == "__main__":
    for p in glob.glob("imports/*.csv"):
        data = load_data_csv(p)
        start_import(data)
    for p in glob.glob("imports/*.xml"):
        data = load_data_xml(p)
        start_import(data)
