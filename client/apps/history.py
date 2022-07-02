import streamlit as st

from rpc.history import get_history


async def app():
    st.title('History')

    st.write(await get_history())