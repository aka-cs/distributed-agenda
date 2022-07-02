import logging
from typing import List

import streamlit as st

from rpc.client import get_user
from rpc.groups import get_all_groups, create_group, get_group_users, remove_user_from_group, add_user_to_group
from rpc.history import update_history

INPUTS = 'inputs'


async def app():
    st.title('Groups')

    current_user = get_user()

    # st.radio with get_users_groups as options
    groups, owner = await get_user_groups()
    if groups:
        option = st.radio(label='', options=groups)
        group = groups[option]
        st.title(f'Group: {group.id}')
        users, admins = await get_group_users(group.id)
        users.sort(key=lambda x: x.username)
        st.write('Participants:')

        col_name, col_action = st.columns([1, 1])
        for i, user in enumerate(users):
            with col_name:
                st.write(user.username)
            if not owner.get(group.id):
                continue
            with col_action:
                if user.username == current_user['sub']:
                    st.write('This is you')
                    continue
                rm_btn = st.button('Remove', key=f'remove_{i}')
                if rm_btn:
                    await st_remove_user(group.id, user.username)
                    st.experimental_rerun()

        if owner.get(group.id) or not any(admins):
            st.text_input(label='Add new participant', key='add_user')
            add_btn = st.button('Add')
            if add_btn:
                await st_add_user(group.id)
                st.experimental_rerun()

    st.title('New Group')

    st.checkbox('Claim yourself as owner', value=True, key='hierarchy')

    inputs = st.session_state.get(INPUTS, [])

    for i, text in enumerate(inputs):
        st.text_input(f'Participant No. {i + 1}', value=text, key=f'input_{i}', on_change=lambda: update_input(i))

    st.text_input('Add new participant', value='', key='input_new', on_change=lambda: add_input())

    clicked = st.button('Create Group')
    if clicked:
        result = await st_create_group(users=inputs)
        logging.info(f'result: {result}')
        if result:
            st.session_state[INPUTS] = []
            await update_history()
            st.experimental_rerun()


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
    groups, owner = await get_all_groups()
    return groups, owner


async def st_create_group(users):
    result = await create_group(users=users, hierarchy=st.session_state.get('hierarchy', False))
    return result


async def st_remove_user(group_id, username):
    return await remove_user_from_group(group_id, username)


async def st_add_user(group_id):
    new_username = st.session_state['add_user']
    return await add_user_to_group(group_id, new_username)
