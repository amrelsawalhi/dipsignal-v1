"""
Reusable Streamlit UI components
"""
import streamlit as st
import pandas as pd


def metric_card(title, value, delta=None, icon="📊"):
    """Display a styled metric card"""
    col1, col2 = st.columns([1, 4])
    with col1:
        st.markdown(f"<div style='font-size: 40px;'>{icon}</div>", unsafe_allow_html=True)
    with col2:
        st.metric(label=title, value=value, delta=delta)


def status_badge(status):
    """Return a colored status badge"""
    colors = {
        'success': '#28a745',
        'fresh': '#28a745',
        'warning': '#ffc107',
        'stale': '#ffc107',
        'error': '#dc3545',
        'critical': '#dc3545',
        'running': '#17a2b8',
        'pending': '#6c757d'
    }
    
    labels = {
        'success': '✅ Success',
        'fresh': '🟢 Fresh',
        'warning': '⚠️ Warning',
        'stale': '🟡 Stale',
        'error': '❌ Error',
        'critical': '🔴 Critical',
        'running': '🔄 Running',
        'pending': '⏳ Pending'
    }
    
    color = colors.get(status.lower(), '#6c757d')
    label = labels.get(status.lower(), status)
    
    return f'<span style="background-color: {color}; color: white; padding: 4px 12px; border-radius: 12px; font-size: 14px; font-weight: bold;">{label}</span>'


def data_table(df, title=None, height=400):
    """Display a formatted dataframe with optional title"""
    if title:
        st.subheader(title)
    
    if df is None or df.empty:
        st.info("No data available")
        return
    
    st.dataframe(df, use_container_width=True, height=height)


def section_header(title, icon=""):
    """Display a section header with icon"""
    if icon:
        st.markdown(f"## {icon} {title}")
    else:
        st.markdown(f"## {title}")


def info_box(message, type="info"):
    """Display an info/warning/error box"""
    if type == "info":
        st.info(message)
    elif type == "warning":
        st.warning(message)
    elif type == "error":
        st.error(message)
    elif type == "success":
        st.success(message)


def format_time_ago(hours):
    """Format hours into human-readable time ago string"""
    if hours is None:
        return "Never"
    
    if hours < 1:
        minutes = int(hours * 60)
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    elif hours < 24:
        return f"{int(hours)} hour{'s' if int(hours) != 1 else ''} ago"
    else:
        days = int(hours / 24)
        return f"{days} day{'s' if days != 1 else ''} ago"


def progress_bar(current, total, label=""):
    """Display a progress bar"""
    if total == 0:
        percentage = 0
    else:
        percentage = (current / total) * 100
    
    st.progress(current / total if total > 0 else 0)
    st.caption(f"{label} {current}/{total} ({percentage:.1f}%)")
