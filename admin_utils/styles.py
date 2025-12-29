"""
Custom CSS styles for the admin panel
"""
import streamlit as st


def apply_custom_styles():
    """Apply custom CSS to make the admin panel look modern and professional"""
    st.markdown("""
    <style>
    /* Force light mode */
    [data-testid="stAppViewContainer"] {
        color-scheme: light !important;
    }
    
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    /* Global Styles */
    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Main container */
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        background-attachment: fixed;
    }
    
    /* Content area */
    .block-container {
        background: white;
        border-radius: 20px;
        padding: 2rem 3rem;
        box-shadow: 0 20px 60px rgba(0,0,0,0.15);
        margin-top: 2rem;
        margin-bottom: 2rem;
    }
    
    /* Headers */
    h1 {
        color: #1a1a2e;
        font-weight: 700;
        font-size: 2.5rem !important;
        margin-bottom: 0.5rem !important;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    
    h2 {
        color: #2d3748;
        font-weight: 600;
        font-size: 1.8rem !important;
        margin-top: 2rem !important;
        margin-bottom: 1rem !important;
    }
    
    h3 {
        color: #4a5568;
        font-weight: 600;
        font-size: 1.3rem !important;
        margin-bottom: 0.8rem !important;
    }
    
    /* Metric cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #f6f8fb 0%, #ffffff 100%);
        padding: 1.5rem;
        border-radius: 15px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
    }
    
    [data-testid="stMetric"]:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 24px rgba(102, 126, 234, 0.15);
        border-color: #667eea;
    }
    
    [data-testid="stMetric"] label {
        color: #718096 !important;
        font-size: 0.9rem !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #1a202c !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
    }
    
    /* Buttons */
    .stButton > button {
        background-color: #475569;
        color: white;
        border: 1px solid #475569;
        border-radius: 0.5rem;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        font-size: 0.95rem;
        transition: all 0.2s ease;
        box-shadow: 0 2px 4px rgba(71, 85, 105, 0.2);
    }
    
    .stButton > button:hover {
        background-color: #334155;
        border-color: #1e293b;
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(71, 85, 105, 0.3);
    }
    
    .stButton > button:active {
        background-color: #1e293b;
        transform: translateY(0);
    }
    
    /* Secondary buttons */
    .stButton > button[kind="secondary"] {
        background: white;
        color: #475569;
        border: 2px solid #475569;
    }
    
    .stButton > button[kind="secondary"]:hover {
        background: #475569;
        color: white;
    }
    
    /* Dataframes */
    [data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid #e2e8f0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
    }
    
    /* Tables */
    table {
        border-radius: 12px;
        overflow: hidden;
    }
    
    thead tr {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    thead th {
        color: white !important;
        font-weight: 600;
        padding: 1rem;
        text-transform: uppercase;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
    }
    
    tbody tr {
        transition: all 0.2s ease;
    }
    
    tbody tr:hover {
        background-color: #f7fafc;
        transform: scale(1.01);
    }
    
    tbody td {
        padding: 1rem;
        border-bottom: 1px solid #e2e8f0;
    }
    
    /* Info/Warning/Error boxes */
    .stAlert {
        border-radius: 12px;
        border-left: 4px solid;
        padding: 1rem 1.5rem;
        margin: 1rem 0;
        color: #1a202c !important;
    }
    
    [data-baseweb="notification"] {
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        color: #1a202c !important;
    }
    
    /* Success messages */
    .stSuccess {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        border-left-color: #28a745;
        color: #155724 !important;
    }
    
    /* Info messages */
    .stInfo {
        background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%);
        border-left-color: #17a2b8;
        color: #0c5460 !important;
    }
    
    /* Warning messages */
    .stWarning {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
        border-left-color: #ffc107;
        color: #856404 !important;
    }
    
    /* Error messages */
    .stError {
        background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
        border-left-color: #dc3545;
        color: #721c24 !important;
    }
    
    /* Code blocks */
    code {
        background: #f7fafc;
        color: #2d3748;
        padding: 0.2rem 0.5rem;
        border-radius: 6px;
        font-family: 'Fira Code', monospace;
        border: 1px solid #e2e8f0;
    }
    
    pre {
        background: #f7fafc !important;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border: 1px solid #e2e8f0;
    }
    
    pre code {
        background: transparent !important;
        color: #2d3748 !important;
        border: none;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
    }
    
    [data-testid="stSidebar"] * {
        color: #e2e8f0 !important;
    }
    
    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2, 
    [data-testid="stSidebar"] h3 {
        color: white !important;
        -webkit-text-fill-color: white !important;
    }
    
    /* Sidebar navigation */
    [data-testid="stSidebarNav"] {
        background: rgba(255,255,255,0.05);
        border-radius: 12px;
        padding: 1rem;
    }
    
    /* Text inputs */
    input, textarea, select {
        border-radius: 8px !important;
        border: 2px solid #e2e8f0 !important;
        transition: all 0.3s ease !important;
    }
    
    input:focus, textarea:focus, select:focus {
        border-color: #667eea !important;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1) !important;
    }
    
    /* Tabs */
    [data-baseweb="tab-list"] {
        background: #f7fafc;
        border-radius: 12px;
        padding: 0.5rem;
    }
    
    [data-baseweb="tab"] {
        border-radius: 8px;
        font-weight: 600;
    }
    
    [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        color: white !important;
    }
    
    /* Expander */
    [data-testid="stExpander"] {
        border-radius: 12px;
        border: 1px solid #e2e8f0;
        background: white;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Plotly charts */
    .js-plotly-plot {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
    }
    
    /* Horizontal rule */
    hr {
        margin: 2rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #e2e8f0, transparent);
    }
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: #f1f1f1;
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: #764ba2;
    }
    
    /* Loading spinner */
    [data-testid="stSpinner"] > div {
        border-top-color: #667eea !important;
    }
    
    /* Custom card class */
    .custom-card {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        border: 1px solid #e2e8f0;
        margin-bottom: 1rem;
        transition: all 0.3s ease;
    }
    
    .custom-card:hover {
        box-shadow: 0 8px 16px rgba(102, 126, 234, 0.15);
        transform: translateY(-2px);
    }
    
    /* Badge styles */
    .badge {
        display: inline-block;
        padding: 0.35rem 0.8rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .badge-success {
        background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
        color: white;
    }
    
    .badge-warning {
        background: linear-gradient(135deg, #ffc107 0%, #fd7e14 100%);
        color: white;
    }
    
    .badge-error {
        background: linear-gradient(135deg, #dc3545 0%, #c82333 100%);
        color: white;
    }
    
    /* Animation */
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .block-container > div {
        animation: fadeIn 0.5s ease-out;
    }
    </style>
    """, unsafe_allow_html=True)
