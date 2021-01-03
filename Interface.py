import os
from pyspark.sql import SparkSession

import plotly.graph_objs as go
from plotly.subplots import make_subplots

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
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Line(x=spain_df_p["date"], y=spain_df_p["value"], name = "Open Values"),
        secondary_y=False,
    )
    
    fig.add_trace(go.Line(x=spain_df_p["date"], y=spain_df_p["stringency_index"], name = "Stringency Index"),
        secondary_y=True,
    )
    # fig.update_layout(
    #     title_text="Stringency vs Stock market values"
    # )
    
    # Set x-axis title
    fig.update_xaxes(title_text="Date")
    
    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Open Values</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Stringency Index</b>", secondary_y=True)
    
    fig.update_layout(transition_duration=500)
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)