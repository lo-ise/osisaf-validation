# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from datetime import datetime

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def multiple_series(data):
    plot_lst = []
    for d in sorted(data.keys()):
        plot_dict = {}
        xdata = [item[0] for item in data[d]]
        ydata = [item[1] for item in data[d]]
        plot_dict['x'] = xdata
        plot_dict['y'] = ydata
        plot_dict['type'] = 'line'
        plot_dict['name'] = str(d)
        plot_lst.append(plot_dict)       
    return plot_lst


def create_dropdown(data):
    options = []
    years = sorted(data.keys(), reverse=True)
    for y in years:
        label_dict = {}
        label_dict['label'] = y
        label_dict['value'] = y
        options.append(label_dict)
    return options    


def create_data(filename):
    datafile = open(filename, 'r')
    data = {}
    for line in datafile.readlines():
        d = line.split(',')
        dt = datetime.strptime(str(d[0]), '%Y%m%d')
        d[0] = dt
        d[1] = d[1].strip()
        if dt.month <= 6:
            season = dt.year
        else:
            season = dt.year + 1
        if season in data:
            data[season].append(d)
        else:
            data[season] = [d]
    datafile.close()
    return data



def serve_layout():

    data = create_data('data.csv')
    timeseries_data = multiple_series(data)
    dropdown_labels = create_dropdown(data)

    return html.Div([
                 html.Div([
                     dcc.Graph(id='all',
                               figure={
                                   'data': timeseries_data,
                                   'layout': {
                                       'title': 'Full time series of validation results',
                                       'xaxis': {
                                           'title':'date', "showline":True, "showgrid":True
                                       },
                                       'yaxis': {
                                           'title':'agreement', "zeroline":False, "showline":True, "showgrid": True
                                       }
                                   }
                               }
                          ),
                     #dcc.RangeSlider(
                         #id='my-range-slider',
                         #min=2011,
                         #max=2019,
                         #step=1,
                         #value=[2011, 2019]
                         #)
                     ], style={"width" : "100%", 'columnCount': 1}),

                 html.Div([
                     html.Label("Select season: "),
                     dcc.Dropdown(
                         id="dropdown_season",
                         options=dropdown_labels,
                         value='2019',
                         style={"width" : "100px"}),
                     dcc.Graph(id='results')
                     ]),

                 ], style={"width" : "80%"})


app.layout = serve_layout


@app.callback(
    Output(component_id='results',component_property='figure'),
    [Input(component_id='dropdown_season', component_property='value')]
    )
def update_graph(season):
    data = create_data('data.csv')

    xdata = [item[0] for item in data[int(season)]]     
    ydata = [item[1] for item in data[int(season)]]

    return {'data': [
                {'x': xdata, 'y': ydata, 'type': 'line'},
                ],
                'layout': {
                    'title': 'Validation results for season {0}'.format(season),
                    'xaxis': {
                        'title':'date', "showline":True, "showgrid":True
                    },
                    'yaxis':{
                        'title':'agreement', "zeroline":False, "showline":True, "showgrid": True
                    }
                }
            }



if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0')

