import dash
from dash import dcc, html, Input, Output, State
import plotly.io as pio
import plotly.express as px
import numpy as np
import plotly.graph_objects as go


# Analyze and visualize the results with a heatmap
def heatmap_visualizer(df):
    # Convert Polars DataFrame to Pandas for easier handling with Plotly
    pd_df = df.to_pandas().set_index("short_window")

    # Finding the Highest Value and its Indices
    highest_value = pd_df.values.max()
    index_of_highest_value = (
        pd_df.stack().idxmax()
    )  # Returns a tuple of indices (row, column)

    # Calculating the Mean
    mean_value = pd_df.values.mean()

    # Calculating the Median
    median_value = pd_df.median().median()

    # Calculating the Percentage Difference
    percentage_difference = ((highest_value - mean_value) / mean_value) * 100

    # # Output the results
    # print(
    #     f"Highest Value: {highest_value} at {[int(highest_index) for highest_index in index_of_highest_value]} (short, long)"
    # )
    # print(f"Mean Value: {mean_value}")
    # print(f"Median Value: {median_value}")
    # print(f"Percentage Difference: {percentage_difference:.2f}%")

    # Create a Plotly figure for the heatmap
    fig = px.imshow(
        pd_df,
        labels=dict(x="Long Window", y="Short Window", color="Final Asset Value"),
        x=pd_df.columns,
        y=pd_df.index,
        title="Heatmap of Results",
    )

    fig.update_xaxes(side="top")  # Move x-axis labels to the top for better readability

    # Make the layout responsive and set margins to ensure space for annotations
    fig.update_layout(
        autosize=True,
        margin=dict(t=50, l=50, r=50, b=150),  # Set a large bottom margin
        # Remove fixed width and height to make the layout responsive
    )

    # Add text annotations for the results below the heatmap
    results_text = (
        f"Highest Value: {highest_value} at {[int(highest_index) for highest_index in index_of_highest_value]} (short, long)\n"
        f"Mean Value: {mean_value}\n"
        f"Median Value: {median_value}\n"
        f"Percentage Difference: {percentage_difference:.2f}%"
    )

    fig.add_annotation(
        text=results_text,
        xref="paper", yref="paper",
        x=0.5, y=-0.1,  # Adjust these values as needed
        showarrow=False,
        font=dict(size=12),
        align="center",
        bordercolor="black",
        borderwidth=2,
        borderpad=4,
        bgcolor="white",
        xanchor='center',  # Center the text box horizontally
        yanchor='top'      # Align the text box at the top of the annotation space
    )

    # Show the figure in the default web browser
    pio.show(fig)



def one_condition_trading_simulation_visualizer(df, show_or_return_graph_object='show'):
    """
    Visualizes the results of a trading simulation
    
    Parameters
    ----------
    df : Pandas DataFrame
        DataFrame containing the simulation results
    show_or_return_graph_object : str
        Determines whether the plot is shown or returned as a graph object
        Must be either 'show' or 'return_graph_object'

    Returns
    -------
    fig : Plotly Figure
        Plotly figure containing the visualization


    """
    df = df.set_index('timestamp')

    if show_or_return_graph_object == 'show':
        y_axis_on_annotation = -0.3
    elif show_or_return_graph_object == 'return_graph_object':
        y_axis_on_annotation = 1.05
    else:
        raise ValueError('show_or_return_graph_object must be either "show" or "return_graph_object"')

    # Create figure
    fig = go.Figure()
    
    # Trace for Asset Value
    trace1 = go.Scatter(
        x=df.index,
        y=df['asset_value'],
        mode='lines',
        name='Asset Value'
    )
    
    # Trace for Buy Points
    trace2 = go.Scatter(
        x=df[df['buy_point']].index,
        y=df[df['buy_point']]['asset_value'],
        mode='markers',
        name='Buy Point',
        marker=dict(
            symbol='triangle-up',
            size=10,
            color='green'
        )
    )
    
    # Trace for Sell Points
    trace3 = go.Scatter(
        x=df[df['sell_point']].index,
        y=df[df['sell_point']]['asset_value'],
        mode='markers',
        name='Sell Point',
        marker=dict(
            symbol='triangle-down',
            size=10,
            color='red'
        )
    )
    
    # Trace for Normalized Stock Price
    trace4 = go.Scatter(
        x=df.index,
        y=df['normalized_stock_price'],
        mode='lines',
        name='Normalized Stock Price'
    )
    
    # Add traces to figure
    fig.add_trace(trace1)
    fig.add_trace(trace2)
    fig.add_trace(trace3)
    fig.add_trace(trace4)

    # Calculate statistics for annotations
    buy_sell_pairs = zip(df[df['buy_point']].index, df[df['sell_point']].index)
    successful_periods = 0
    percentage_changes = []

    for buy_index, sell_index in buy_sell_pairs:
        buy_price = df.loc[buy_index, 'normalized_stock_price']
        sell_price = df.loc[sell_index, 'normalized_stock_price']
        percentage_change = ((sell_price - buy_price) / buy_price) * 100
        percentage_changes.append(percentage_change)
        if sell_price > buy_price:
            successful_periods += 1

    success_rate = (successful_periods / len(percentage_changes)) * 100 if percentage_changes else 0
    average_percentage_change = sum(percentage_changes) / len(percentage_changes) if percentage_changes else 0
    standard_deviation_percentage_change = np.std(percentage_changes, ddof=1) if percentage_changes else 0
    trading_period_count = len(percentage_changes)

    # Add text annotation with the trading period count
    text_annotation = (
        f"Trading Periods: {trading_period_count}<br>"
        f"Trading Period Success Rate: {success_rate:.2f}%<br>"
        f"Average Trading Period Increase: {average_percentage_change:.2f}%<br>"
        f"Trading Period Increase Standard Deviation: {standard_deviation_percentage_change:.2f}%"
    )

    fig.add_annotation(
        text=text_annotation,
        xref="paper", yref="paper",
        x=0.5, y=y_axis_on_annotation,
        showarrow=False,
        align="center",
        bordercolor="black",
        borderwidth=2,
        borderpad=4,
        bgcolor="white",
        font=dict(size=12, color="black"),  # Set font color to black
        xanchor="center",
        yanchor="top"
    )

    # Update layout to ensure space for annotations
    fig.update_layout(
        title_text="Trading Simulation",
        xaxis_title="Date",
        yaxis_title="Value",
        template='plotly_dark',
        autosize=True,  # This will make the figure responsive to the layout/container
        margin=dict(t=100, l=50, r=50, b=150)  # Adjust bottom margin to fit annotations
    )
    # 
    if show_or_return_graph_object == 'show':
        # Show plot
        fig.show()
    elif show_or_return_graph_object == 'return_graph_object':
        return fig
    else:
        raise ValueError('show_or_return_graph_object must be either "show" or "return_graph_object"')
    

    



# visualize the results with a Dash app by adding a slider to filter the results
def heatmap_visualizer_with_slider_on_dash(df):
    df = df.to_pandas().set_index("short_window")
    app = dash.Dash(__name__)

    app.layout = html.Div(
        [
            dcc.Slider(
                id="percentage-slider",
                min=1,
                max=100,
                value=100,
                marks={i: f"{i}%" for i in range(1, 101, 10)},
                step=1,
            ),
            dcc.Graph(id="heatmap"),
        ]
    )

    @app.callback(Output("heatmap", "figure"), [Input("percentage-slider", "value")])
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

        # Finding the Highest Value and its Indices (based on the masked DataFrame)
        highest_value = df.values.max()
        index_of_highest_value = (
            df.stack().idxmax()
        )  # Returns a tuple of indices (row, column)

        # Calculating the Mean (ignoring NaNs)
        mean_value = df.values.mean()

        # Calculating the Median (ignoring NaNs)
        median_value = df.median().median()

        # Calculating the Percentage Difference
        percentage_difference = ((highest_value - mean_value) / mean_value) * 100

        # Create the heatmap figure with the masked DataFrame
        fig = px.imshow(
            masked_df,
            labels=dict(x="Long Window", y="Short Window", color="Final Asset Value"),
            x=masked_df.columns,
            y=masked_df.index,
            title="Heatmap of Results",
        )

        # Add text annotations for the results below the heatmap
        results_text = (
            f"Highest Value: {highest_value:.2f} at {index_of_highest_value} (short, long)\n"
            f"Mean Value: {mean_value:.2f}\n"
            f"Median Value: {median_value:.2f}\n"
            f"Percentage Difference: {percentage_difference:.2f}%"
        )

        fig.add_annotation(
            text=results_text,
            xref="paper", yref="paper",
            x=0.5, y=-0.1,  # Adjust these values as needed
            showarrow=False,
            font=dict(size=12),
            align="center",
            bordercolor="black",
            borderwidth=2,
            borderpad=4,
            bgcolor="white",
            xanchor='center',  # Center the text box horizontally
            yanchor='bottom'   # Anchor the text box at the bottom of the annotation space
        )

        # Set the layout for the figure to accommodate the annotation
        fig.update_layout(
            autosize=True,
            margin=dict(t=50, l=50, r=50, b=150),  # Adjust margins to ensure full use of space
        )
        fig.update_xaxes(side="top")

        return fig

    app.run_server(debug=True)

def heatmap_line_graph_visualizer_with_slider_on_dash(reshaped_final_values_df, complete_backtest_results):
    reshaped_final_values_df = reshaped_final_values_df.to_pandas().set_index("short_window")
    app = dash.Dash(__name__)

    app.layout = html.Div(
        [
            dcc.Slider(
                id="percentage-slider",
                min=1,
                max=100,
                value=100,
                marks={i: f"{i}%" for i in range(1, 101, 10)},
                step=1,
            ),
            dcc.Graph(id="heatmap"),
            html.Div([
                html.Div([
                    "Short Window:",
                    dcc.Input(id='input-short-window', type='number', value=31),
                    "Long Window:",
                    dcc.Input(id='input-long-window', type='number', value=43),
                    html.Button('Submit', id='submit-val', n_clicks=0),
                ], style={'margin': '10px'}),
            ]),
            dcc.Graph(id="line-graph"),
        ]
    )

    @app.callback(Output("heatmap", "figure"), [Input("percentage-slider", "value")])
    def update_heatmap(percentage):
        # Flatten the values and sort them to find the threshold
        threshold_values = reshaped_final_values_df.values.flatten()
        threshold_values.sort()
        # Calculate the index for the threshold based on the percentage
        threshold_index = int(len(threshold_values) * (1 - percentage / 100)) - 1
        threshold_index = max(0, threshold_index)  # Ensure the index is not negative
        threshold = threshold_values[threshold_index]

        # Create a masked DataFrame where values below the threshold are set to NaN
        masked_df = reshaped_final_values_df.mask(reshaped_final_values_df < threshold, other=np.nan)

        # Finding the Highest Value and its Indices (based on the masked DataFrame)
        highest_value = reshaped_final_values_df.values.max()
        index_of_highest_value = (
            reshaped_final_values_df.stack().idxmax()
        )  # Returns a tuple of indices (row, column)

        # Calculating the Mean (ignoring NaNs)
        mean_value = reshaped_final_values_df.values.mean()

        # Calculating the Median (ignoring NaNs)
        median_value = reshaped_final_values_df.median().median()

        # Calculating the Percentage Difference
        percentage_difference = ((highest_value - mean_value) / mean_value) * 100

        # Create the heatmap figure with the masked DataFrame
        fig = px.imshow(
            masked_df,
            labels=dict(x="Long Window", y="Short Window", color="Final Asset Value"),
            x=masked_df.columns,
            y=masked_df.index,
            title="Heatmap of Results",
        )

        # Add text annotations for the results below the heatmap
        results_text = (
            f"Highest Value: {highest_value:.2f} at {index_of_highest_value} (short, long)\n"
            f"Mean Value: {mean_value:.2f}\n"
            f"Median Value: {median_value:.2f}\n"
            f"Percentage Difference: {percentage_difference:.2f}%"
        )

        fig.add_annotation(
            text=results_text,
            xref="paper", yref="paper",
            x=0.5, y=-0.1,  # Adjust these values as needed
            showarrow=False,
            font=dict(size=12),
            align="center",
            bordercolor="black",
            borderwidth=2,
            borderpad=4,
            bgcolor="white",
            xanchor='center',  # Center the text box horizontally
            yanchor='bottom'   # Anchor the text box at the bottom of the annotation space
        )

        # Set the layout for the figure to accommodate the annotation
        fig.update_layout(
            autosize=True,
            margin=dict(t=50, l=50, r=50, b=150),  # Adjust margins to ensure full use of space
        )
        fig.update_xaxes(side="top")

        return fig

    @app.callback(
        Output('line-graph', 'figure'),
        [Input('heatmap', 'clickData'),
         Input('submit-val', 'n_clicks')],
        [State('input-short-window', 'value'),
         State('input-long-window', 'value')]
    )
    def update_line_graph(clickData, n_clicks, input_short, input_long):
        ctx = dash.callback_context
        if not ctx.triggered:
            # If called without interaction, just display an empty figure
            return go.Figure()
        # Determine what triggered the callback
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if trigger_id == 'heatmap':
            # Get short and long window from heatmap click
            short_window = clickData['points'][0]['y']
            long_window = clickData['points'][0]['x']
        elif trigger_id == 'submit-val':
            # Use the submitted short and long window values
            short_window = input_short
            long_window = input_long
        else:
            # No valid trigger, just return an empty figure
            return go.Figure()

        # Fetch the specific backtest result for the selected combination
        specific_result = complete_backtest_results.get((int(short_window), int(long_window)))
        if specific_result is None:
            # If there's no result for the given combination, display an empty figure
            return go.Figure()

        # Use the one_condition_trading_simulation_visualizer function to create the line graph
        fig = one_condition_trading_simulation_visualizer(specific_result, show_or_return_graph_object='return_graph_object')

        # Adjust the layout and show the plot
        fig.update_layout(autosize=True, margin=dict(t=50, l=50, r=50, b=50)) # Adjust bottom margin to fit annotations
        return fig
    @app.callback(
        [Output('input-short-window', 'value'),
        Output('input-long-window', 'value')],
        [Input('heatmap', 'clickData')],
        prevent_initial_call=True
    )
    def update_input_boxes(clickData):
        # Get short and long window from heatmap click and update the input boxes
        short_window = clickData['points'][0]['y']
        long_window = clickData['points'][0]['x']
        return short_window, long_window

    app.run_server(debug=True)