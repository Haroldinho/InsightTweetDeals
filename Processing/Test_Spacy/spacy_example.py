#!/usr/bin/python3
#-*- coding: utf-8 -*-
"""
Test functionality of spaCy to detect and extract product name from Tweet text
"""

import spacy

test_example2 = "AUKEY's dual-port 2.4A charger is what Apple should have include w/ iPhone: $6, more https://t.co/XwJIEo4mYM by... https://t.co/pM1jtFtw6r" 
test_example1 = "PUMA Men's Contrast Pants for $13 + free shipping - https://t.co/XQ6b5JgJWF" 
test_example0 = u'Samsonite Novex 16" Laptop Backpack for $50 + free shipping - https://t.co/b18e2c3lP4'

# !! The following package must be first downloaded from the command line with the command: python -m spacy download en_core_web_lg
nlp = spacy.load("en_core_web_sm")

doc0 = nlp(test_example0)
print(doc0.text)

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




named_entity0= {"ORG":[],"PRODUCT":[],"WORK_OF_ART":[],"PERSON":[]}
for ent in doc0.ents:
	#print(ent.text, ent.label_)
	if ent.label_ in named_entity0.keys():
		named_entity0[ent.label_].append(ent.text)
print("Found company name to be {}.".format(show_brand(named_entity0))) 
print("Found product name to be {}.".format(show_product_name(named_entity0))) 


named_entity1= {"ORG":[],"PRODUCT":[],"WORK_OF_ART":[],"PERSON":[]}
doc1 = nlp(test_example1)
print(doc1.text)
for ent in doc1.ents:
	#print(ent.text, ent.label_)
	if ent.label_ in named_entity1.keys():
		named_entity1[ent.label_].append(ent.text)
print("Found company name to be {}.".format(show_brand(named_entity1))) 
print("Found product name to be {}.".format(show_product_name(named_entity1))) 


named_entity2= {"ORG":[],"PRODUCT":[],"WORK_OF_ART":[],"PERSON":[]}
doc2 = nlp(test_example2)
print(doc2.text)
for ent in doc2.ents:
	#print(ent.text, ent.label_)
	if ent.label_ in named_entity2.keys():
		named_entity2[ent.label_].append(ent.text)
print("Found company name to be {}.".format(show_brand(named_entity2))) 
print("Found product name to be {}.".format(show_product_name(named_entity2))) 
