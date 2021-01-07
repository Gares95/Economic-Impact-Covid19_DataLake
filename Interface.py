import os
from pyspark.sql import SparkSession

import plotly.graph_objs as go
from plotly.subplots import make_subplots

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

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

Ec_status_inf = spark.read.parquet(os.path.join(output_data, "Ec_status/*.parquet")).select(["country_code", "stock_id"]).sort("country_code").dropDuplicates().toPandas()


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
            value='SAN.MC'
        ),

    html.Div(id='info-company', style={'whiteSpace': 'pre-line', 'display': 'inline-block', 'margin-top': '10px', 'margin-right': '250px'}),
    html.Div(id='info-dates', style={'whiteSpace': 'pre-line', 'display': 'inline-block'}),
    
    dcc.Graph(id='stock-stringency'),
    html.Div(id='info-correlation', style={'whiteSpace': 'pre-line', 'display': 'inline-block'}),
    
])

@app.callback(
    Output('stock-stringency', 'figure'),
    Input('company-id', 'value'))
                          
def update_myPlot(ticker_selected):
    company_selected = Ec_status_df.filter((Ec_status_df.value_type=='Open') & (Ec_status_df.stock_id==ticker_selected)).sort("Date").toPandas()
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Line(x=company_selected["date"], y=company_selected["value"], name = "Open Values"),
        secondary_y=False,
    )
    
    fig.add_trace(go.Line(x=company_selected["date"], y=company_selected["stringency_index"], name = "Stringency Index"),
        secondary_y=True,
    )
    
    # Set x-axis title
    fig.update_xaxes(title_text="Date")
    
    # Set y-axes titles
    fig.update_yaxes(title_text="<b>Open Values</b>", secondary_y=False)
    fig.update_yaxes(title_text="<b>Stringency Index</b>", secondary_y=True)
    
    fig.update_layout(margin = {'t':15},transition_duration=500)
    
    return fig

@app.callback(
    Output('info-company', 'children'),
    [Input('company-id', 'value'),Input('company-id', 'options')])
def update_info(value, options):
    return "Country: {}\nSector: {}".\
        format(countries_df[countries_df['country_code']==Ec_status_inf[Ec_status_inf['stock_id']==value].\
               country_code.iloc[0]].country.iloc[0], \
               companies_df[companies_df['stock_id']==value].sector.iloc[0])

@app.callback(
    Output('info-dates', 'children'),
    Input('company-id', 'value'))
def update_dates_info(value):
    company_selected = Ec_status_df.filter((Ec_status_df.value_type=='Open') & (Ec_status_df.stock_id==value)).sort("Date").toPandas()
    return "Dates from {} to {}\n".format(company_selected.date.min(), company_selected.date.max())

@app.callback(
    Output('info-correlation', 'children'),
    Input('company-id', 'value'))
def update_correlation(ticker_selected):
    company_selected = Ec_status_df.filter((Ec_status_df.value_type=='Open') & (Ec_status_df.stock_id==ticker_selected)).sort("Date").toPandas()
    return "Correlation (pearson): {}\n".format(company_selected.corr(method = "pearson").loc['stringency_index', 'value'])


if __name__ == '__main__':
    app.run_server(debug=True)