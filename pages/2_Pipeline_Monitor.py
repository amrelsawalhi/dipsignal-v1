"""
Pipeline Monitor - View Dagster asset status and lineage
"""
import streamlit as st
import sys
import os
import socket
import time
import networkx as nx
import plotly.graph_objects as go

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from admin_utils.components import section_header
from admin_utils.styles import apply_custom_styles

st.set_page_config(page_title="Pipeline Monitor", layout="wide")

# Authentication
from admin_utils.auth import require_authentication, show_logout_button
require_authentication()
show_logout_button()
apply_custom_styles()

st.title("Pipeline Monitor")

# Dagster Status - Compact inline design
dagster_running = False
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex(('localhost', 3000))
    sock.close()
    dagster_running = (result == 0)
except:
    pass

# Custom CSS for professional button styling
st.markdown("""
    <style>
    .stButton > button {
        background-color: #475569;
        color: white;
        border: 1px solid #475569;
        border-radius: 0.375rem;
        padding: 0.375rem 0.75rem;
        font-size: 0.875rem;
        font-weight: 500;
        transition: all 0.2s;
    }
    .stButton > button:hover {
        background-color: #334155;
        border-color: #1e293b;
    }
    </style>
""", unsafe_allow_html=True)

col1, col2, col3 = st.columns([2, 3, 1])

with col1:
    if dagster_running:
        st.markdown("**Dagster Status:** 🟢 Running")
    else:
        st.markdown("**Dagster Status:** 🔴 Stopped")

with col2:
    if dagster_running:
        st.markdown("[Open Dagster UI ↗](http://localhost:3000)")
    else:
        st.caption("Start Dagster to view pipeline status")

with col3:
    if not dagster_running:
        if st.button("Start", key="start_dagster"):
            import subprocess
            import sys
            
            try:
                # Start dagster dev in background
                if sys.platform == 'win32':
                    # Windows: Start in new window
                    subprocess.Popen(
                        ['start', 'cmd', '/k', 'dagster', 'dev'],
                        shell=True,
                        cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
                    )
                else:
                    # Unix/Mac: Start in background
                    subprocess.Popen(
                        ['dagster', 'dev'],
                        cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                
                st.success("Starting Dagster...")
                time.sleep(2)
                st.rerun()
            except Exception as e:
                st.error(f"Failed to start: {e}")

st.markdown("---")

# Pipeline Dependency Flow
st.markdown("### Pipeline Dependency Graph")

def parse_dagster_assets():
    """
    Parse assets.py file to extract asset definitions and dependencies.
    Returns a graph structure by analyzing the Python AST.
    """
    import ast
    import os
    
    # Path to assets.py
    assets_path = os.path.join(os.path.dirname(__file__), '..', 'dagster_pipeline', 'assets.py')
    
    try:
        with open(assets_path, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read())
        
        nodes = {}
        edges = []
        
        # Iterate through all function definitions
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check if function has @asset decorator
                has_asset_decorator = False
                group_name = 'data_collection'  # default
                deps = []
                
                for decorator in node.decorator_list:
                    # Simple decorator: @asset
                    if isinstance(decorator, ast.Name) and decorator.id == 'asset':
                        has_asset_decorator = True
                    
                    # Decorator with arguments: @asset(...)
                    elif isinstance(decorator, ast.Call):
                        if isinstance(decorator.func, ast.Name) and decorator.func.id == 'asset':
                            has_asset_decorator = True
                            
                            # Extract group_name
                            for keyword in decorator.keywords:
                                if keyword.arg == 'group_name':
                                    if isinstance(keyword.value, ast.Constant):
                                        group_name = keyword.value.value
                                
                                # Extract deps
                                elif keyword.arg == 'deps':
                                    if isinstance(keyword.value, ast.List):
                                        for dep in keyword.value.elts:
                                            if isinstance(dep, ast.Name):
                                                deps.append(dep.id)
                                            elif isinstance(dep, ast.Constant):
                                                deps.append(dep.value)
                
                if has_asset_decorator:
                    asset_name = node.name
                    
                    # Determine color based on group and name
                    if 'news' in asset_name.lower():
                        color = '#3b82f6'  # Blue
                    elif group_name == 'ai_analysis':
                        color = '#8b5cf6'  # Purple
                    else:
                        color = '#10b981'  # Green
                    
                    nodes[asset_name] = {
                        'group': group_name,
                        'color': color
                    }
                    
                    # Add edges for dependencies
                    for dep in deps:
                        edges.append((dep, asset_name))
        
        return nodes, edges
        
    except Exception as e:
        st.error(f"Error parsing assets.py: {e}")
        return {}, []

def create_pipeline_graph():
    """
    Create graph from parsed assets.
    Fully dynamic - automatically updates when assets.py changes.
    """
    nodes, edges = parse_dagster_assets()
    
    if not nodes:
        st.warning("No assets found. Using fallback graph.")
        return create_fallback_graph()
    
    G = nx.DiGraph()
    
    # Add nodes
    for node, attrs in nodes.items():
        G.add_node(node, **attrs)
    
    # Add edges
    G.add_edges_from(edges)
    
    return G, nodes

def create_fallback_graph():
    """Fallback if parsing fails"""
    G = nx.DiGraph()
    nodes = {
        'binance_data': {'group': 'data_collection', 'color': '#10b981'},
        'macro_data': {'group': 'data_collection', 'color': '#10b981'},
        'stock_data': {'group': 'data_collection', 'color': '#10b981'},
        'commodity_data': {'group': 'data_collection', 'color': '#10b981'},
        'fgi_data': {'group': 'data_collection', 'color': '#10b981'},
        'crypto_news': {'group': 'news_collection', 'color': '#3b82f6'},
        'stock_news': {'group': 'news_collection', 'color': '#3b82f6'},
        'macro_summary': {'group': 'ai_analysis', 'color': '#8b5cf6'},
        'daily_news_summaries': {'group': 'ai_analysis', 'color': '#8b5cf6'},
        'asset_analysis': {'group': 'ai_analysis', 'color': '#8b5cf6'},
        'weekly_portfolio_recommendation': {'group': 'ai_analysis', 'color': '#8b5cf6'},
    }
    
    for node, attrs in nodes.items():
        G.add_node(node, **attrs)
    
    edges = [
        ('macro_data', 'stock_data'),
        ('stock_data', 'commodity_data'),
        ('commodity_data', 'stock_news'),
        ('stock_news', 'macro_summary'),
        ('binance_data', 'macro_summary'),
        ('crypto_news', 'macro_summary'),
        ('fgi_data', 'macro_summary'),
        ('macro_summary', 'daily_news_summaries'),
        ('daily_news_summaries', 'asset_analysis'),
    ]
    
    G.add_edges_from(edges)
    return G, nodes

def generate_layout(G):
    """
    Generate hierarchical layout automatically based on graph structure.
    """
    # Calculate layers based on topological sort
    layers = {}
    for node in nx.topological_sort(G):
        if G.in_degree(node) == 0:
            layers[node] = 0
        else:
            # Maximum layer of predecessors + 1
            pred_layers = [layers[pred] for pred in G.predecessors(node)]
            layers[node] = max(pred_layers) + 1 if pred_layers else 0
    
    # Group nodes by layer
    layer_groups = {}
    for node, layer in layers.items():
        if layer not in layer_groups:
            layer_groups[layer] = []
        layer_groups[layer].append(node)
    
    # Assign positions
    pos = {}
    for layer, nodes_in_layer in layer_groups.items():
        # Sort nodes alphabetically within each layer for consistency
        sorted_nodes = sorted(nodes_in_layer)
        for i, node in enumerate(sorted_nodes):
            x = layer * 1.2
            # Center vertically
            y = i * 0.8 - (len(sorted_nodes) - 1) * 0.4
            pos[node] = (x, y)
    
    return pos

# Create the graph
G, nodes = create_pipeline_graph()

pos = generate_layout(G)

# Create edge traces with arrows
edge_trace = []
for edge in G.edges():
    x0, y0 = pos[edge[0]]
    x1, y1 = pos[edge[1]]
    
    # Draw line
    edge_trace.append(
        go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            mode='lines',
            line=dict(width=2, color='#94a3b8'),
            hoverinfo='none',
            showlegend=False
        )
    )
    
    # Add arrow annotation
    edge_trace.append(
        go.Scatter(
            x=[(x0 + x1) / 2],
            y=[(y0 + y1) / 2],
            mode='markers',
            marker=dict(
                size=8,
                color='#94a3b8',
                symbol='arrow',
                angle=0 if x1 > x0 else 180,
            ),
            hoverinfo='none',
            showlegend=False
        )
    )

# Create node annotations as rectangles
shapes = []
annotations = []

for node in G.nodes():
    x, y = pos[node]
    node_name = node.replace('_', ' ').title()
    
    # Add rectangle shape
    shapes.append(
        dict(
            type="rect",
            x0=x - 0.4,
            y0=y - 0.25,
            x1=x + 0.4,
            y1=y + 0.25,
            fillcolor=nodes[node]['color'],
            line=dict(color='white', width=2),
            opacity=0.9
        )
    )
    
    # Add text annotation
    annotations.append(
        dict(
            x=x,
            y=y,
            text=f"<b>{node_name}</b>",
            showarrow=False,
            font=dict(size=10, color='white', family='Arial'),
            align='center',
        )
    )

# Create hover trace (invisible markers for hover functionality)
node_x = []
node_y = []
node_hover = []

for node in G.nodes():
    x, y = pos[node]
    node_x.append(x)
    node_y.append(y)
    
    # Create hover text with dependencies
    deps = list(G.predecessors(node))
    dependents = list(G.successors(node))
    hover_text = f"<b>{node}</b><br>"
    hover_text += f"Group: {nodes[node]['group']}<br>"
    if deps:
        hover_text += f"Depends on: {', '.join(deps)}<br>"
    if dependents:
        hover_text += f"Required by: {', '.join(dependents)}"
    node_hover.append(hover_text)

hover_trace = go.Scatter(
    x=node_x,
    y=node_y,
    mode='markers',
    hoverinfo='text',
    hovertext=node_hover,
    marker=dict(
        size=1,
        color='rgba(0,0,0,0)',
    ),
    showlegend=False
)

# Create the figure
fig = go.Figure(data=edge_trace + [hover_trace])

fig.update_layout(
    showlegend=False,
    hovermode='closest',
    margin=dict(b=20, l=20, r=20, t=20),
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, autorange=True),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, autorange=True, scaleanchor='x', scaleratio=0.7),
    plot_bgcolor='#f8fafc',
    height=500,
    shapes=shapes,
    annotations=annotations,
)

st.plotly_chart(fig, use_container_width=True)

# Legend
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("🟢 **Data Collection** - Market data & prices")
with col2:
    st.markdown("🔵 **News Collection** - News articles & feeds")
with col3:
    st.markdown("🟣 **AI Analysis** - Summaries & recommendations")

st.caption("💡 Hover over nodes to see dependencies. **Graph automatically updates when you modify `dagster_pipeline/assets.py`**")
