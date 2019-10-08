#!/usr/bin/python3
# coding=utf-8
"""
		Front-end to InsightTweetDeals
It's  a little DashTable that interacts with my PostGreSQL tables to let
the users go through products and prices from his favorite user,
and filter by specific brand or product.
	
@dealsListener
code shamelessly copied from Dustin Harris https://github.com/dustinharris
"""


import sqlalchemy
import psycopg2
import sys
import os
import dash
import dash_table
import pandas as pd
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
#Connect dash to flask
app = dash.Dash(__name__,  external_stylesheets=external_stylesheets)
server = app.server


def connect():
    '''Returns a connection and a metadata object'''
    # We connect with the help of the PostgreSQL URL
    user = os.environ['POSTGRESQL_USER']
    password = os.environ['POSTGRESQL_PASSWD']
    host = os.environ['POSTGRESQL_HOST']
    db = os.environ['POSTGRESQL_DB']
    port = 5412
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    # The return value of create_engine() is our connection object
    con = sqlalchemy.create_engine(url, client_encoding='utf8')

    # We then bind the connection to MetaData()
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta

con, meta = connect()
full_table_df = pd.read_sql('SELECT created_at AS "Time", text AS "Text", username AS "User", product AS "Product"'
				', "Price", brand AS "Brand", follower_count AS "Followers",  url FROM my_table', con)
full_table_df['Time'] = pd.to_datetime(full_table_df['Time'])
full_table_df = full_table_df.sort_values(by=['Time'], ascending=False)
full_table_df['Time'] = full_table_df['Time'].dt.strftime("%B %d %Y, %r")

app.layout = html.Div(children=[
	html.H1(children='@DealsListener'),
	html.P(children='Analyzing the history of promotions on Twitter.'),

	html.Div(style={'margin' : '10px 10%'},children=[
		html.Div(children=html.H5(children='''
                	Full Text
        	''')),
        	dash_table.DataTable(
                	id='main-table',
                	style_data={'whiteSpace': 'normal'},
			style_cell={'overflow': 'hidden',  'textOverflow':'ellipsis', 'minWidth':0,  'maxWidth': 55, 'maxHeight':2},
			style_cell_conditional=[
				{
					'if':{'column_id':'text'},
					'overflow': 'hidden',
					'textoverflow':'ellipsis'
				}

			],
			style_table={'maxHeight':'100', 'overflowY':'scroll'},
                	css=[{
                        	'selector': '.dash-cell div.dash-cell-value',
                        	'rule': 'display: inline; '
                	}],
                	columns=[{"name": i, "id": i, "deletable":True, "selectable":True} for i in full_table_df.columns],
			editable=True,
			filter_action="native",
			sort_action="native",
			sort_mode="single",
			selected_columns=[],
			column_selectable="single",
			fixed_rows={'headers':True, 'data':0},
                	data=full_table_df.to_dict("rows"),
        	),
		html.Div(id='main-table-container')
	]),
])

@app.callback(
    Output('main-table', 'style_data_conditional'),
    [Input('main-table', 'selected_columns')]
)
def update_styles(selected_columns):
    return [{
        'if': { 'column_id': i },
        'background_color': '#D2F3FF'
    } for i in selected_columns]


# run the Dash app.
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port='8050')
