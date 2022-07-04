import datetime

import streamlit as st
from google.protobuf.timestamp_pb2 import Timestamp

from apps.groups import get_user_groups
from data.create_data import create_table
from rpc.events import create_event, get_user_events, accept_event, reject_event
from rpc.history import update_history


async def app():
    st.title('Events')

    user_events, drafts = await get_user_events()

    if drafts:
        st.markdown("### Events invitations")

        st.write("-" * 100)

        col1, col2, col3 = st.columns([10, 1, 1])
        for event in drafts.values():
            with col1:
                st.write(f'##### {event.name}')
            with col2:
                accepted = st.button(f"Accept")
                if accepted:
                    await accept_event(event)
                    st.experimental_rerun()
            with col3:
                rejected = st.button(f"Reject")
                if rejected:
                    await reject_event(event)
                    st.experimental_rerun()

            st.write("-" * 100)

    st.markdown("### Recent Events")

    date = st.date_input('Go to a date', value=datetime.datetime.today())
    if not date:
        date = datetime.datetime.today()

    df = create_table(user_events, date)
    st.dataframe(df, 1920, 1080)

    st.write('-' * 100)

    groups, _ = await get_user_groups()
    group_id = 0
    if groups:
        is_group_event = st.checkbox('Group Event')
        if is_group_event:
            group_id = st.selectbox(label='Select Group', options=groups, format_func=lambda x: groups[x].name)

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
            response = await create_event(name=event_name, description=event_description, start=event_start,
                                          end=event_end, group_id=group_id)
            if response == 0:
                st.success('Event created successfully')
                await update_history()
                st.experimental_rerun()
            if response == 1:
                st.warning('Event stored locally, check your connection')
            if response == 2:
                st.error('Event breaks someone schedule')




