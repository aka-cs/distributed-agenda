import datetime
import logging

import streamlit as st
from google.protobuf.timestamp_pb2 import Timestamp

from data.create_data import create_table
from rpc.events import create_event, get_user_events
from rpc.history import update_history


async def app():
    st.title('Events')

    st.write("This is a sample home page in the mutliapp.")
    st.write("See `apps/events.py` to know how to use it.")

    st.markdown("### Sample Data")

    user_events = await get_user_events()

    df = create_table(user_events[0])
    st.write(df)

    st.write('Navigate to `Data Stats` page to visualize the data')

    st.write('-' * 100)

    form = st.form(key='new_event')

    with form:

        st.title('New Event')

        event_name = st.text_input('Event name')

        event_description = st.text_area('Event description')

        col1, col2 = st.columns([1, 1])
        with col1:
            event_start_date = st.date_input('Start Date', value=datetime.datetime.now())
            event_end_date = st.date_input('End Date', value=datetime.datetime.now() + datetime.timedelta(days=1))

        with col2:
            event_start_time = st.time_input('Hour', key='start_date_hour', value=datetime.datetime.now().time())
            event_end_time = st.time_input('Hour', key='end_date_hour', value=datetime.datetime.now().time())

        submitted = st.form_submit_button('Create Event')
        if submitted:
            # combine datetime.date and datetime.time
            start = datetime.datetime.combine(event_start_date, event_start_time)
            end = datetime.datetime.combine(event_end_date, event_end_time)
            event_start = Timestamp(seconds=int(start.timestamp()))
            event_end = Timestamp(seconds=int(end.timestamp()))
            await create_event(name=event_name, description=event_description, start=event_start, end=event_end)
            st.success('Event created successfully')
            await update_history()





