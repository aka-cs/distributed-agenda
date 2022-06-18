import asyncio
import threading

import streamlit as st
from multiapp import MultiApp
from apps import home, data_stats, login, get_user  # import your app modules here

import logging

app = MultiApp(get_user())

loop = asyncio.get_event_loop_policy().new_event_loop()

# Add all your application here
app.add_app("Login", login.app)
app.add_app("Home", home.app)
app.add_app("Data Stats", data_stats.app)

# The main app
loop.run_until_complete(app.run())