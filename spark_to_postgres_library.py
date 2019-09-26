#!/usr/bin/python3
# coding: utf-8
"""
		Insight Data Engineering

Library used to set up the connection between Spark and my Postgresql database. 
inspired by https://github.com/gauravsm31/Insight_Project/blob/master/postgres.py
"""

from pyspark.sql import DataFrameWriter


class PostgresConnector(object):
	def __init__(self):
		self.database_name = 'postgres'
		self.hostname ='ip-10-0-0-7'
		self.server_hostname='ec2-54-218-246-42.us-west-2.compute.amazonaws.com'
		self.hostname_dns = 'ec2-35-166-72-17.us-west-2.compute.amazonaws.com'
		self.url_connect = "jdbc:postgresql://{hostname}:5412/{db}".format(hostname=self.hostname, db=self.database_name)
		self.properties = {"user":'shiny', "password":'suzyq', "driver":"org.postgresql.Driver"}

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
