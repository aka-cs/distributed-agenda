import os

import shutil
import aiofiles
import streamlit as st

import pickle


class Storage:

    @staticmethod
    def store(key, value):
        st.session_state[key] = value

    @staticmethod
    def get(key, default=None):
        return st.session_state.get(key, default)

    @staticmethod
    def delete(key):
        if st.session_state.get(key):
            del st.session_state[key]

    @staticmethod
    def disk_store(key, value):
        user_path = os.path.expanduser('~')
        folder_path = f'{user_path}/.distributed-agenda'

        # create folder_path if it doesn't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        with open(f'{folder_path}/{key}.txt', 'wb') as f:
            f.write(pickle.dumps(value))

    @staticmethod
    def disk_get(key, default=None):
        user_path = os.path.expanduser('~')
        file_path = f'{user_path}/.distributed-agenda/{key}.txt'

        # if file doesn't exist, return None
        if not os.path.exists(file_path):
            return default

        with open(file_path, 'rb') as f:
            return pickle.loads(f.read())

    @staticmethod
    def disk_delete(key):
        user_path = os.path.expanduser('~')
        path = f'{user_path}/.distributed-agenda/{key}.txt'
        if os.path.exists(path):
            os.remove(path)

    # same as disk_store but uses aiofiles
    @staticmethod
    async def async_disk_store(key, value):
        user_path = os.path.expanduser('~')
        folder_path = f'{user_path}/.distributed-agenda'

        # create folder_path if it doesn't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        async with aiofiles.open(f'{folder_path}/{key}.txt', 'wb') as f:
            await f.write(pickle.dumps(value))

    # same as disk_get but uses aiofiles
    @staticmethod
    async def async_disk_get(key, default=None):
        user_path = os.path.expanduser('~')
        file_path = f'{user_path}/.distributed-agenda/{key}.txt'

        # if file doesn't exist, return None
        if not os.path.exists(file_path):
            return default

        async with aiofiles.open(file_path, 'rb') as f:
            return pickle.loads(await f.read())

    # same as disk_delete but uses aiofiles
    @staticmethod
    async def async_disk_delete(key):
        user_path = os.path.expanduser('~')
        path = f'{user_path}/.distributed-agenda/{key}.txt'
        if os.path.exists(path):
            os.remove(path)

    @staticmethod
    def clear():
        st.session_state.clear()

        # remove all files at folder_path with shutil.rmtree
        user_path = os.path.expanduser('~')
        folder_path = f'{user_path}/.distributed-agenda'

        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
