#!/usr/bin/python3
# coding: utf-8
"""
		Insight Data Engineering

Library used to set up the connection between Spark and my Postgresql database. 
inspired by https://github.com/gauravsm31/Insight_Project/blob/master/postgres.py
"""

from pyspark.sql import DataFrameWriter
import os

class PostgresConnector(object):
	def __init__(self):
		self.database_name = 'postgres'
		self.hostname = os.environ['POSTGRESQL_HOST']
		self.url_connect = "jdbc:postgresql://{hostname}:5412/{db}".format(hostname=self.hostname, db=self.database_name)
		self.properties = {"user":os.environ['POSTGRESQL_USER'], "password":os.environ['POSTGRESQL_PASSWD'], "driver":"org.postgresql.Driver"}

	def get_writer(self, df):
		return DataFrameWriter(df)

	def write(self, df, table, mode):
		my_writer = self.get_writer(df)
		my_writer.jdbc(self.url_connect, table, mode, self.properties)


def write_to_postgres(out_df, table_name):
	table = table_name
	mode = "append"
	connector = PostgresConnector()
	connector.write(out_df, table, mode)
