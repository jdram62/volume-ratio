import bitget
import pandas as pd
import psycopg2
from dash import Dash, html, dcc, Input, Output
import plotly.express as px

with open('bitget-symbols.txt') as f:
    symbols = f.readlines()

with open('db.txt') as f:
    db_info = f.readlines()

app = Dash(__name__)

app.layout = html.Div([
    html.H4('PERP/SPOT past 24hour volume ratio'),
    dcc.Graph(id='time-series-chart'),
    html.P('Select ticker:'),
    dcc.Dropdown(
        id="ticker",
        options=symbols,
        value='ETHUSDT',
        clearable=False,
    ),
])


def get_df(ticker):
    with psycopg2.connect(user=db_info[0].strip(), password=db_info[1].strip(),
                          host=db_info[2].strip(), database=db_info[3].strip()) as conn:
        sql_perps = 'SELECT * FROM bitget_perps ORDER BY ts;'
        sql_spot = 'SELECT * FROM bitget_spot ORDER BY ts;'
        df_perp = pd.read_sql_query(sql_perps, conn)
        df_spot = pd.read_sql_query(sql_spot, conn)

    arr_date = df_perp.ts.dt.to_pydatetime()
    df_perp.ts = pd.Series(arr_date, dtype="object")
    arr_date = df_spot.ts.dt.to_pydatetime()
    df_spot.ts = pd.Series(arr_date, dtype="object")

    # temp = df_perp.loc[df_perp['symbol'] == 'KNCUSDT\n']
    # print(temp.head(20))  # selects all rows containing specific symbol

    for symbol in symbols:
        perp_temp = df_perp.loc[df_perp['symbol'] == symbol]
        spot_temp = df_spot.loc[df_spot['symbol'] == symbol]

        if not perp_temp.empty and not spot_temp.empty and symbol == ticker:  # have info for both tickers spot and perp
            ratio_df = perp_temp.drop('volume', axis=1)
            ratio_df['ratio'] = perp_temp['volume'].values / spot_temp['volume'].values
            return ratio_df


@app.callback(
    Output("time-series-chart", "figure"),
    Input("ticker", "value"))
def display_time_series(ticker):
    df = get_df(ticker)
    fig = px.line(df, x=df.ts, y=df.ratio)
    fig.update_layout(template="plotly_dark")
    return fig


if __name__ == '__main__':
    bitget.run()
    app.run_server(debug=True)

