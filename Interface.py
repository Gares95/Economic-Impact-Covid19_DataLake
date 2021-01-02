
import plotly.express as px
from plotly.offline import download_plotlyjs, init_notebook_mode,  plot
from plotly.graph_objs import *

import pandas as pd

import configparser
from datetime import datetime
import boto3, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format

import matplotlib.pyplot as plt 

spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

output_data = "./Output_Data/"

# Countries table
countries_df = spark.read.parquet(os.path.join(output_data, "countries/*.parquet")).toPandas()

# Countries table
companies_df = spark.read.parquet(os.path.join(output_data, "stocks/*.parquet")).toPandas()

Ec_status_df = spark.read.parquet(os.path.join(output_data, "Ec_status/*.parquet"))


import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Stringency vs Stock market values'),
    html.Div(children='''
        Filter by country
    '''),
    dcc.Dropdown(
            options = [
                # {'label': i, 'value': j} for i,j in countries_df['country_code'],countries_df['country']
                {'label': i, 'value': j} for i, j in zip(countries_df['country'], countries_df['country_code'])
                ],
            value=''
        ),
    html.Div(children='''
        Select company
    '''),
    
    dcc.Dropdown(
            id = 'company-id',
            options = [
                {'label': i, 'value': j} for i, j in zip(companies_df['company_name'], companies_df['stock_id'])
                ],
            value='GOOGL'
        ),

    dcc.Graph(id='stock-stringency')
])

@app.callback(
    Output('stock-stringency', 'figure'),
    Input('company-id', 'value'))
                          
def update_myPlot(selected_company):
    spain_df_p = Ec_status_df.filter((Ec_status_df.value_type=='Open') & (Ec_status_df.stock_id==selected_company)).sort("Date").toPandas()
    spain_df_p['stringency_index'] = (spain_df_p['stringency_index']/spain_df_p['stringency_index'].max())*spain_df_p['value'].max()
    spain_df_p.plot(x='date', y=['value', 'stringency_index'], kind='line', rot=45)
    
    fig =px.line(spain_df_p, x='date', y=['value', 'stringency_index'])
    fig.update_layout(transition_duration=500)
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)