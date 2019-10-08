
"""
			***** INSIGHT DATA ENGINEERING ****
@dealsListener
Library to use spacy with Spark for Named Entity Recognition of Products and Categories
Cohort: SEA '19C
Name: Harold Nikoue
"""


import spacy
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

SPACY_MODEL = None

def get_spacy_model():
	global SPACY_MODEL
	if not SPACY_MODEL:
		# other options for this package are "en_core_web_lg" and "en_core_web_md"
		SPACY_MODEL = spacy.load("en_core_web_md")
	return SPACY_MODEL

def show_product_name(named_entity_dictionary):
	if named_entity_dictionary["PRODUCT"]:
		return named_entity_dictionary["PRODUCT"][0]
	elif named_entity_dictionary["WORK_OF_ART"]:
		return named_entity_dictionary["WORK_OF_ART"][0]
	elif named_entity_dictionary["PERSON"]:
		return named_entity_dictionary["PERSON"][0]
	elif named_entity_dictionary["ORG"]:
		return named_entity_dictionary["ORG"][0]
	else:	
		return "NULL"

def show_brand(named_entity_dictionary):
	if named_entity_dictionary["ORG"]:
		return named_entity_dictionary["ORG"][0]
	elif named_entity_dictionary["PERSON"]:
		return named_entity_dictionary["PERSON"][0]
	else:
		return "NULL"


def get_named_entity(text):
	named_entity = {"ORG":[],"PRODUCT":[],"WORK_OF_ART":[],"PERSON":[]}
	nlp = get_spacy_model()
	doc = nlp(text)
	for ent in doc.ents:
		if ent.label_ in named_entity.keys():
			named_entity[ent.label_].append(ent.text)
	return named_entity

def get_product_name(text):
	return show_product_name(get_named_entity(text))

def get_brand(text):
	return show_brand(get_named_entity(text))

def add_product_column_to_df(df):
	to_product_name_udf = udf(get_product_name, StringType())
	df = df.withColumn("product", to_product_name_udf('text'))
	return df

def add_brand_column_to_df(df):
	to_brand_udf = udf(get_brand, StringType())
	df = df.withColumn("brand", to_brand_udf('text'))
	return df

def add_product_brand_column_to_df(df):
	to_product_name_udf = udf(get_product_name, StringType())
	to_brand_udf = udf(get_brand, StringType())
	df = df.withColumn("brand", to_brand_udf('text')).withColumn("product", to_product_name_udf('text'))
	return df
	
