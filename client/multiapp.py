"""Frameworks for running multiple Streamlit applications as a single app.
"""
import streamlit as st
from streamlit_option_menu import option_menu


class MultiApp:
    """Framework for combining multiple streamlit applications.
    Usage:
        def foo():
            st.title("Hello Foo")
        def bar():
            st.title("Hello Bar")
        app = MultiApp()
        app.add_app("Foo", foo)
        app.add_app("Bar", bar)
        app.run()
    It is also possible keep each application in a separate file.
        import foo
        import bar
        app = MultiApp()
        app.add_app("Foo", foo.app)
        app.add_app("Bar", bar.app)
        app.run()
    """
    def __init__(self, user=None):
        self.user = user
        self.apps = dict()

    def add_app(self, title, func):
        """Adds a new application.
        Parameters
        ----------
        func:
            the python function to render this app.
        title:
            title of the app. Appears in the dropdown in the sidebar.
        """
        self.apps[title] = func

    async def run(self):

        st.set_page_config(layout='wide')

        with st.sidebar:
            selected = option_menu("Main Menu" if not self.user else self.user['name'], list(self.apps.keys()), icons=['house', 'gear'],
                                   menu_icon='cast', default_index=0)
        await self.apps[selected]()

