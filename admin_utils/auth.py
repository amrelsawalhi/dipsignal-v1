"""
Authentication utilities for Admin Panel
"""
import streamlit as st
import os
from dotenv import load_dotenv

load_dotenv()

def require_authentication():
    """Check if user is authenticated, show login page if not"""
    import hashlib
    import time
    
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = None
    
    # Check for stored session in localStorage
    if not st.session_state.authenticated:
        # JavaScript to check localStorage
        check_session = """
        <script>
        const authToken = localStorage.getItem('dipsignal_auth_token');
        const username = localStorage.getItem('dipsignal_username');
        if (authToken && username) {
            // Send to Streamlit via query params
            const url = new URL(window.location);
            url.searchParams.set('auth_token', authToken);
            url.searchParams.set('auth_user', username);
            if (!url.searchParams.get('auth_restored')) {
                url.searchParams.set('auth_restored', '1');
                window.location.href = url.toString();
            }
        }
        </script>
        """
        st.markdown(check_session, unsafe_allow_html=True)
        
        # Check query params for restored session
        query_params = st.query_params
        if 'auth_token' in query_params and 'auth_user' in query_params:
            stored_token = query_params['auth_token']
            stored_user = query_params['auth_user']
            
            # Verify token (simple hash of username + secret)
            admin_username = os.getenv("ADMIN_USERNAME")
            secret = os.getenv("SESSION_SECRET", "dipsignal_secret_key")
            expected_token = hashlib.sha256(f"{admin_username}{secret}".encode()).hexdigest()
            
            if stored_token == expected_token and stored_user == admin_username:
                st.session_state.authenticated = True
                st.session_state.username = stored_user
                # Clear query params
                st.query_params.clear()
                st.rerun()
    
    # If already authenticated, return
    if st.session_state.authenticated:
        return
    
    # Show centered login page
    st.title("🔐 DipSignal Admin Login")
    st.markdown("---")
    
    # Get credentials from environment
    admin_username = os.getenv("ADMIN_USERNAME")
    admin_password = os.getenv("ADMIN_PASSWORD")
    
    if not admin_username or not admin_password:
        st.error("⚠️ Admin credentials not configured in .env file")
        st.info("Please set ADMIN_USERNAME and ADMIN_PASSWORD in your .env file")
        st.stop()
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Login form
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            remember_me = st.checkbox("Remember me", value=True)
            submit = st.form_submit_button("Login", use_container_width=True)
            
            if submit:
                if username == admin_username and password == admin_password:
                    st.session_state.authenticated = True
                    st.session_state.username = username
                    
                    # Store in localStorage if remember me is checked
                    if remember_me:
                        secret = os.getenv("SESSION_SECRET", "dipsignal_secret_key")
                        auth_token = hashlib.sha256(f"{username}{secret}".encode()).hexdigest()
                        
                        store_session = f"""
                        <script>
                        localStorage.setItem('dipsignal_auth_token', '{auth_token}');
                        localStorage.setItem('dipsignal_username', '{username}');
                        </script>
                        """
                        st.markdown(store_session, unsafe_allow_html=True)
                    
                    st.success("✅ Login successful!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials")
    
    st.stop()

def show_logout_button():
    """Show logout button in sidebar with username"""
    with st.sidebar:
        st.markdown("---")
        
        # Show logged in user
        if st.session_state.get('username'):
            st.caption(f"Logged in as: **{st.session_state.username}**")
        
        # Create a container for the logout button with custom styling
        st.markdown("""
            <style>
            /* Target the logout button using its unique key class */
            .st-key-logout_btn button {
                background-color: #475569 !important;
                color: white !important;
                border: 1px solid #475569 !important;
                border-radius: 0.5rem !important;
                padding: 0.5rem 1rem !important;
                font-size: 0.95rem !important;
                font-weight: 600 !important;
                width: 100% !important;
                transition: all 0.3s ease !important;
                text-transform: uppercase !important;
                letter-spacing: 0.5px !important;
            }
            
            .st-key-logout_btn button:hover {
                background-color: #334155 !important;
                border-color: #1e293b !important;
                transform: translateY(-2px) !important;
                box-shadow: 0 4px 8px rgba(71, 85, 105, 0.3) !important;
            }
            
            .st-key-logout_btn button:active {
                background-color: #1e293b !important;
                border-color: #0f172a !important;
                transform: translateY(0) !important;
            }
            
            /* Force all text inside the logout button to be white */
            .st-key-logout_btn button * {
                color: white !important;
            }
            </style>
        """, unsafe_allow_html=True)
        
        # Logout button
        if st.button("Sign Out", key="logout_btn", use_container_width=True):
            # Clear localStorage
            clear_session = """
            <script>
            localStorage.removeItem('dipsignal_auth_token');
            localStorage.removeItem('dipsignal_username');
            </script>
            """
            st.markdown(clear_session, unsafe_allow_html=True)
            
            st.session_state.authenticated = False
            st.session_state.username = None
            st.rerun()



