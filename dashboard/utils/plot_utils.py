import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_parameter_chart(df, parameter, parameter_name, time_column='time', reference_values=None):
    """
    Create a time series chart for a specific parameter
    
    Args:
        df: DataFrame containing the data
        parameter: Column name for the parameter to plot
        parameter_name: Display name for the parameter
        time_column: Column name for the time data
        reference_values: Dictionary with reference values to show as horizontal lines
    
    Returns:
        A plotly figure
    """
    if parameter not in df.columns or time_column not in df.columns:
        # Create empty chart with message if data is missing
        fig = go.Figure()
        fig.update_layout(
            title=f"No data available for {parameter_name}",
            xaxis_title="Time",
            yaxis_title=parameter_name
        )
        return fig
    
    # Create base chart
    fig = px.line(
        df,
        x=time_column,
        y=parameter,
        title=f"{parameter_name} Over Time",
        labels={
            time_column: "Time",
            parameter: parameter_name
        },
        markers=True
    )
    
    # Add reference values if provided
    if reference_values:
        for label, value in reference_values.items():
            fig.add_shape(
                type="line",
                x0=df[time_column].min(),
                y0=value,
                x1=df[time_column].max(),
                y1=value,
                line=dict(
                    color="red" if "critical" in label.lower() else 
                          "orange" if "warning" in label.lower() else 
                          "green",
                    dash="dash",
                    width=1
                )
            )
            
            # Add annotation for the line
            fig.add_annotation(
                x=df[time_column].max(),
                y=value,
                xref="x",
                yref="y",
                text=label,
                showarrow=False,
                xanchor="right",
                yanchor="bottom" if "critical" in label.lower() else "top",
                xshift=10,
                yshift=10 if "critical" in label.lower() else -10,
                font=dict(
                    size=10,
                    color="red" if "critical" in label.lower() else 
                           "orange" if "warning" in label.lower() else 
                           "green"
                )
            )
    
    # Add moving average if enough data points
    if len(df) >= 5:
        # Calculate 5-point moving average
        df_smooth = df.copy()
        df_smooth[f"{parameter}_ma"] = df[parameter].rolling(window=5, min_periods=1).mean()
        
        fig.add_trace(
            go.Scatter(
                x=df_smooth[time_column],
                y=df_smooth[f"{parameter}_ma"],
                mode="lines",
                name=f"{parameter_name} (Moving Avg)",
                line=dict(color="rgba(0, 0, 255, 0.5)", width=3)
            )
        )
    
    # Improve layout
    fig.update_layout(
        xaxis=dict(
            title="Time",
            gridcolor="lightgray",
            showgrid=True
        ),
        yaxis=dict(
            title=parameter_name,
            gridcolor="lightgray",
            showgrid=True
        ),
        plot_bgcolor="white",
        hovermode="closest",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

def create_multiparameter_chart(df, parameters, parameter_names, time_column='time'):
    """
    Create a time series chart for multiple parameters
    
    Args:
        df: DataFrame containing the data
        parameters: List of column names for the parameters to plot
        parameter_names: List of display names for the parameters
        time_column: Column name for the time data
    
    Returns:
        A plotly figure
    """
    if time_column not in df.columns or not all(param in df.columns for param in parameters):
        # Create empty chart with message if data is missing
        fig = go.Figure()
        fig.update_layout(
            title="No data available for the selected parameters",
            xaxis_title="Time",
            yaxis_title="Value"
        )
        return fig
    
    # Create figure
    fig = go.Figure()
    
    # Add each parameter as a trace
    colors = ['blue', 'red', 'green', 'purple', 'orange', 'brown', 'pink', 'gray']
    
    for i, (param, name) in enumerate(zip(parameters, parameter_names)):
        if param in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df[time_column],
                    y=df[param],
                    name=name,
                    mode="lines+markers",
                    line=dict(color=colors[i % len(colors)])
                )
            )
    
    # Improve layout
    fig.update_layout(
        title="Multiple Parameters Over Time",
        xaxis_title="Time",
        yaxis_title="Value",
        plot_bgcolor="white",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig

def create_heatmap(df, x_column, y_column, value_column, x_title=None, y_title=None, title=None):
    """
    Create a heatmap visualization
    
    Args:
        df: DataFrame containing the data
        x_column: Column name for the x-axis categories
        y_column: Column name for the y-axis categories
        value_column: Column name for the values to display in the heatmap
        x_title: Display title for x-axis
        y_title: Display title for y-axis
        title: Chart title
    
    Returns:
        A plotly figure
    """
    # Create pivot table
    pivot_data = df.pivot_table(
        values=value_column,
        index=y_column,
        columns=x_column,
        aggfunc='mean'
    ).fillna(0)
    
    # Create heatmap
    fig = px.imshow(
        pivot_data,
        labels=dict(
            x=x_title if x_title else x_column,
            y=y_title if y_title else y_column,
            color=value_column
        ),
        x=pivot_data.columns,
        y=pivot_data.index,
        color_continuous_scale="YlOrRd",
        title=title if title else f"{value_column} by {x_column} and {y_column}"
    )
    
    # Add text annotations
    annotations = []
    for i, y in enumerate(pivot_data.index):
        for j, x in enumerate(pivot_data.columns):
            annotations.append(
                dict(
                    x=x,
                    y=y,
                    text=str(round(pivot_data.iloc[i, j], 2)),
                    showarrow=False,
                    font=dict(color="white" if pivot_data.iloc[i, j] > pivot_data.values.mean() else "black")
                )
            )
    
    fig.update_layout(annotations=annotations)
    
    return fig

def create_map(data_df, color_by=None, default_zoom=7, default_center=None):
    """
    Create a map visualization using Plotly
    
    Args:
        data_df: DataFrame with lat, lon, name, and optionally type/pollution columns
        color_by: Column to use for color coding (default: None)
        default_zoom: Default zoom level
        default_center: Default center coordinates [lat, lon]
        
    Returns:
        Plotly figure object
    """
    import plotly.express as px
    
    # Default center if not provided
    if default_center is None:
        default_center = {"lat": 38.5, "lon": -76.4}  # Chesapeake Bay
    
    # Crea una copia del dizionario center senza zoom
    center_dict = {}
    if isinstance(default_center, dict):
        center_dict = {k: v for k, v in default_center.items() if k in ["lat", "lon"]}
    
    # Create the map
    fig = px.scatter_mapbox(
        data_df,
        lat="lat",
        lon="lon",
        color=color_by if color_by in data_df.columns else None,
        size="marker_size" if "marker_size" in data_df.columns else None,
        hover_name="name" if "name" in data_df.columns else None,
        hover_data=['name'] if 'name' in data_df.columns else None,
        zoom=default_zoom,
        height=500,
        size_max=20,
        opacity=0.8
    )
    
    # Update map layout - CORREZIONE QUI
    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox=dict(
            center=center_dict,  # Usa solo lat e lon
            zoom=default_zoom  # Zoom va qui, fuori da center
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    
    return fig