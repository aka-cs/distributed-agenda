from typing import List

import streamlit as st

from rpc.groups import get_all_groups, create_group

INPUTS = 'inputs'


async def app():
    st.title('Groups')

    # st.radio with get_users_groups as options
    groups = await get_user_groups()
    st.radio(label='', options=groups)

    st.write('New Group')

    inputs = st.session_state.get(INPUTS, [])

    for i, text in enumerate(inputs):
        st.text_input(f'Participant No. {i + 1}', value=text, key=f'input_{i}', on_change=lambda: update_input(i))

    st.text_input('Add new participant', value='', key='input_new', on_change=lambda: add_input())

    clicked = st.button('Create Group')
    if clicked:
        await create_group(users=inputs)


def add_input():
    value: str = st.session_state['input_new']
    if value.strip() == '':
        return
    inputs = st.session_state.get(INPUTS, [])
    inputs.append(value)
    st.session_state[INPUTS] = inputs


def update_input(index):
    value = st.session_state[f'input_{index}']

    if value.strip() == '':
        ipts: List = st.session_state.get(INPUTS)
        del ipts[index]
        index += 1
        while st.session_state.get(f'input_{index}'):
            st.session_state[f'input_{index - 1}'] = st.session_state.get(f'input_{index}')
            index += 1
        del st.session_state[f'input_{index - 1}']
        return

    inputs = st.session_state.get(INPUTS, [])
    inputs[index] = value
    st.session_state[INPUTS] = inputs


async def get_user_groups():
    groups = await get_all_groups()
    return groups

