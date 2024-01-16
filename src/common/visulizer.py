import dash
from dash import dcc, html, Input, Output
import plotly.io as pio
import plotly.express as px
import numpy as np


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

