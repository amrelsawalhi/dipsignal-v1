"""
Chart generation utilities using Plotly
"""
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def create_freshness_chart(data):
    """
    Create a bar chart showing data freshness
    data: DataFrame with columns ['table', 'hours_old', 'status']
    """
    color_map = {
        'fresh': '#28a745',
        'stale': '#ffc107',
        'critical': '#dc3545'
    }
    
    fig = px.bar(
        data,
        x='table',
        y='hours_old',
        color='status',
        color_discrete_map=color_map,
        title='Data Freshness by Table',
        labels={'hours_old': 'Hours Since Last Update', 'table': 'Table'},
        height=400
    )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        showlegend=True,
        hovermode='x unified'
    )
    
    return fig


def create_timeline_chart(data):
    """
    Create a timeline chart for asset materialization
    data: DataFrame with columns ['asset', 'timestamp', 'status']
    """
    color_map = {
        'success': '#28a745',
        'failed': '#dc3545',
        'running': '#17a2b8'
    }
    
    fig = px.scatter(
        data,
        x='timestamp',
        y='asset',
        color='status',
        color_discrete_map=color_map,
        title='Asset Materialization Timeline',
        labels={'timestamp': 'Time', 'asset': 'Asset'},
        height=500
    )
    
    fig.update_traces(marker=dict(size=12))
    fig.update_layout(
        showlegend=True,
        hovermode='closest'
    )
    
    return fig


def create_row_count_trend(data):
    """
    Create a line chart showing row count trends over time
    data: DataFrame with columns ['date', 'table', 'count']
    """
    fig = px.line(
        data,
        x='date',
        y='count',
        color='table',
        title='Daily Row Count Trends',
        labels={'count': 'Row Count', 'date': 'Date'},
        height=400
    )
    
    fig.update_layout(
        hovermode='x unified',
        showlegend=True
    )
    
    return fig


def create_asset_distribution_pie(data):
    """
    Create a pie chart showing asset distribution by class
    data: DataFrame with columns ['asset_class', 'count']
    """
    fig = px.pie(
        data,
        values='count',
        names='asset_class',
        title='Asset Distribution by Class',
        height=400,
        hole=0.4  # Donut chart
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig


def create_coverage_chart(data):
    """
    Create a grouped bar chart showing asset coverage
    data: DataFrame with columns ['asset_class', 'total_assets', 'assets_with_data_today']
    """
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Total Assets',
        x=data['asset_class'],
        y=data['total_assets'],
        marker_color='lightblue'
    ))
    
    fig.add_trace(go.Bar(
        name='With Data Today',
        x=data['asset_class'],
        y=data['assets_with_data_today'],
        marker_color='darkblue'
    ))
    
    fig.update_layout(
        title='Asset Coverage Today',
        xaxis_title='Asset Class',
        yaxis_title='Count',
        barmode='group',
        height=400
    )
    
    return fig


def create_gauge_chart(value, max_value, title, threshold_good=0.8, threshold_warning=0.5):
    """
    Create a gauge chart for KPIs
    """
    percentage = (value / max_value) * 100 if max_value > 0 else 0
    
    # Determine color based on thresholds
    if percentage >= threshold_good * 100:
        color = '#28a745'
    elif percentage >= threshold_warning * 100:
        color = '#ffc107'
    else:
        color = '#dc3545'
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=value,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': title},
        delta={'reference': max_value},
        gauge={
            'axis': {'range': [None, max_value]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, max_value * threshold_warning], 'color': "lightgray"},
                {'range': [max_value * threshold_warning, max_value * threshold_good], 'color': "gray"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': max_value * threshold_good
            }
        }
    ))
    
    fig.update_layout(height=300)
    
    return fig
