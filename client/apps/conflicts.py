import datetime
from typing import List

import streamlit as st
from google.protobuf.timestamp_pb2 import Timestamp

import proto.events_pb2
from rpc.events import create_event
from rpc.history import update_history
from rpc.requests_queue import Request, remove_request, add_request
from store import Storage


async def app():
    st.title('Conflicts')

    conflicts: List[Request] = Storage.get('conflicts')

    for i, conflict in enumerate(conflicts):
        form = st.form(key='new_event')

        with form:

            event = conflict.request.event

            st.title(event.name)

            event_description = st.text_area('Event description', value=event.description)

            start_time = event.start.ToDatetime()
            end_time = event.end.ToDatetime()

            col1, col2 = st.columns([1, 1])
            with col1:
                event_start_date = st.date_input('Start Date', value=start_time.date())
                event_end_date = st.date_input('End Date', value=end_time.date())

            with col2:
                event_start_time = st.time_input('Hour', key='start_date_hour', value=start_time.time())
                event_end_time = st.time_input('Hour', key='end_date_hour', value=end_time.time())

            submitted = st.form_submit_button('Push Event')
            if submitted:
                # combine datetime.date and datetime.time
                start = datetime.datetime.combine(event_start_date, event_start_time)
                end = datetime.datetime.combine(event_end_date, event_end_time)
                event_start = Timestamp(seconds=int(start.timestamp()))
                event_end = Timestamp(seconds=int(end.timestamp()))
                response = await create_event(name=event.name, description=event_description, start=event_start, end=event_end, group_id=event.groupId)
                if response == 0:
                    st.success('Event created successfully')
                    remove_request(conflict)
                    await update_history()
                if response == 1:
                    st.warning('Event stored locally, check your connection')
                    remove_request(conflict)
                if response == 2:
                    st.error('Event breaks someone schedule')

    if not conflicts:
        st.write('There are no conflicts')
