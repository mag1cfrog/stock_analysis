from concurrent.futures import ProcessPoolExecutor
import itertools
import polars as pl
import duckdb
import numpy as np
import plotly.io as pio
import plotly.express as px
import dash
from dash import dcc, html, Input, Output


# def calculate_moving_averages(data, short_window, long_window):
    
#     return data


# Core function where the trading strategy is implemented
def default_gold_cross_death_cross(data, short_window, long_window, cash=10000, shares_held = 0):
    """
    Simulates trading based on the signals in the DataFrame
    
    Parameters
    ----------
    data : DataFrame
        DataFrame containing the signals

    Returns
    -------
    simulation_df : DataFrame
        DataFrame containing the simulation results

    
    """
    
    simulation_results = []
    normalization_factor = cash / data['close'][0]

    data = data.with_columns(
        data['close'].rolling_mean(window_size=int(short_window)).alias('short_MA'),
        data['close'].rolling_mean(window_size=int(long_window)).alias('long_MA')
    )

    # Create conditions for buying and selling
    buy_condition = (data['short_MA'] > data['long_MA']) & (data['short_MA'].shift(1) <= data['long_MA'].shift(1))
    sell_condition = (data['short_MA'] < data['long_MA']) & (data['short_MA'].shift(1) >= data['long_MA'].shift(1))

    data = data.with_columns(
        pl.when(buy_condition).then(1).when(sell_condition).then(0).alias('percentage_of_cash_invested')
    )

    # for i in range(data.height):
    #     asset_value = cash + shares_held * data['close'][i]
        
    #     if buy_condition[i]:
    #         shares_to_buy = cash // data['close'][i]
    #         cash -= shares_to_buy * data['close'][i]
    #         shares_held += shares_to_buy
    #     elif sell_condition[i]:
    #         cash += shares_held * data['close'][i]
    #         shares_held = 0

    #     simulation_results.append({
    #         'Cash': cash,
    #         'Shares Held': shares_held,
    #         'Asset Value': cash + shares_held * data['close'][i],
    #         'Buy Point': buy_condition[i],
    #         'Sell Point': sell_condition[i],
    #         'Normalized Stock Price': data['close'][i] * normalization_factor
    #     })

    # simulation_df = pl.DataFrame(simulation_results)
    # return simulation_df
    return data







# Function to compute final asset value for a combination
def compute_final_asset_value(combination, data_df):
        short_window, long_window = combination
        return test_strategy_short_long_window((short_window, long_window), data_df)['Asset Value'][-1]  # Return the final asset value


def sensitivity_analysis(data_df, short_range, long_range):
    # Prepare the combinations of short and long windows
    combinations = list(itertools.product(short_range, long_range))

    # Use ThreadPoolExecutor to parallelize the computation
    with ProcessPoolExecutor() as executor:
        final_asset_values = list(executor.map(compute_final_asset_value, combinations,  [data_df]*len(combinations)))

    # Create the results DataFrame
    results = [{'Short_Window': short, 'Long_Window': long, 'Final_Asset_Value': value} 
               for ((short, long), value) in zip(combinations, final_asset_values)]
    results_df = pl.DataFrame(results)
    
    # Reshape the DataFrame
    results_df = results_df.pivot(index='Short_Window', columns='Long_Window', values='Final_Asset_Value')

    return results_df



# Analyze and visualize the results
def analyze_and_visualize(df):
    # Convert Polars DataFrame to Pandas for easier handling with Plotly
    pd_df = df.to_pandas().set_index('Short_Window')

    # Finding the Highest Value and its Indices
    highest_value = pd_df.values.max()
    index_of_highest_value = pd_df.stack().idxmax()  # Returns a tuple of indices (row, column)

    # Calculating the Mean
    mean_value = pd_df.values.mean()

    # Calculating the Median
    median_value = pd_df.median().median()

    # Calculating the Percentage Difference
    percentage_difference = ((highest_value - mean_value) / mean_value) * 100

    # Output the results
    print(f'Highest Value: {highest_value} at {[int(highest_index) for highest_index in index_of_highest_value]} (short, long)')
    print(f'Mean Value: {mean_value}')
    print(f'Median Value: {median_value}')
    print(f'Percentage Difference: {percentage_difference:.2f}%')

    # Visualizing the Data with a Heatmap using Plotly
    fig = px.imshow(pd_df,
                    labels=dict(x="Long Window", y="Short Window", color="Final Asset Value"),
                    x=pd_df.columns,
                    y=pd_df.index,
                    title='Heatmap of Results'
                   )

    fig.update_xaxes(side="top")  # This moves the x-axis labels to the top of the heatmap for better readability
    pio.show(fig) # Show the figure in default web browser


# visualize the results with a Dash app by adding a slider to filter the results
def run_dash_app(df):
    app = dash.Dash(__name__)

    app.layout = html.Div([
        dcc.Slider(
            id='percentage-slider',
            min=1,
            max=100,
            value=100,
            marks={i: f'{i}%' for i in range(1, 101, 10)},
            step=1
        ),
        dcc.Graph(id='heatmap')
    ])

    @app.callback(
        Output('heatmap', 'figure'),
        [Input('percentage-slider', 'value')]
    )
    def update_heatmap(percentage):
        # Flatten the values and sort them to find the threshold
        threshold_values = df.values.flatten()
        threshold_values.sort()
        # Calculate the index for the threshold based on the percentage
        threshold_index = int(len(threshold_values) * (1 - percentage / 100)) - 1
        threshold_index = max(0, threshold_index)  # Ensure the index is not negative
        threshold = threshold_values[threshold_index]

        # Create a masked DataFrame where values below the threshold are set to NaN
        masked_df = df.mask(df < threshold, other=np.nan)

        # Create the heatmap figure with the masked DataFrame
        fig = px.imshow(masked_df,
                        labels=dict(x="Long Window", y="Short Window", color="Final Asset Value"),
                        x=df.columns,
                        y=df.index,
                        title='Heatmap of Results')
        # Set the layout for the figure
        fig.update_layout(
            autosize=True,
            margin=dict(l=50, r=50, t=50, b=50)  # Adjust margins to ensure full use of space
        )
        fig.update_xaxes(side="top")
        return fig

    app.run_server(debug=True)



def main():
    # Time the execution
    import time
    start_time = time.time()
    short_range = range(1, 100)
    long_range = range(1, 100)
    db_path = r'E:\git_repos\stock_analysis\data\raw_stock_price\stock_bar_data.db'
    conn = duckdb.connect(db_path)
    query = """
    SELECT * FROM NVDA_minute_30
    """
    stock_df = conn.execute(query).pl().sort('timestamp')
    conn.close()
        
    results_df = sensitivity_analysis(stock_df, short_range, long_range)
    print(results_df)
    analyze_and_visualize(results_df)
    print(f"Time taken: {time.time() - start_time} seconds")
    run_dash_app(results_df.to_pandas().set_index('Short_Window'))


if __name__ == '__main__':
    main()


