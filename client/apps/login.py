import asyncio as aio
import logging

import bcrypt
import streamlit as st
from grpclib import GRPCError

import proto.auth_grpc
import proto.auth_pb2
import proto.users_grpc
import proto.users_pb2
from rpc import services
from rpc.client import Channel, TOKEN, logout, get_user
from store import Store


async def app():

    loop = aio.get_event_loop()

    token: str = await Store.async_disk_get(TOKEN)

    if token:

        user = get_user()
        print(user)

        st.write(f'Hello {user["name"]}, you are already logged in')

        clicked = st.button('Log Out')
        if clicked:
            await logout()
            st.experimental_rerun()
        return

    signup_state = st.radio(label='', options=['Login', 'SignUp'])

    st.title(signup_state)
    signup_state = signup_state == 'SignUp'

    with st.form('login') as form:
        username = st.text_input('Username')
        if signup_state:
            name = st.text_input('Name')
            email = st.text_input('Email')
        password = st.text_input('Password', type='password')

        if signup_state:
            submitted = st.form_submit_button('Sign Up')
            if submitted:
                result = await signup(username, password, name, email)
        else:
            submitted = st.form_submit_button('Log In')
            if submitted:
                result = await login(username, password)
                if result:
                    st.experimental_rerun()


def sync(f, loop):
    def wrapper(*args, **kwargs):
        return loop.run_until_complete(f(*args, **kwargs))
    return wrapper


async def signup(username: str, password: str, name: str, email: str):
    logging.info(f"Creating user: username: {username}, name: {name}, email: {email}, password: {'*' * len(password)}")
    salt = bcrypt.gensalt()

    user = proto.users_pb2.User(username=username, name=name,
                                passwordHash=bcrypt.hashpw(password.encode(), salt).decode(), email=email)

    request = proto.auth_pb2.SignUpRequest(user=user)

    async with Channel(services.AUTH) as channel:
        stub = proto.auth_grpc.AuthStub(channel)
        try:
            response = await stub.SignUp(request)
            logging.info(f"User created with response result: {response.result}")
            st.success("User created!")
            return True
        except GRPCError as error:
            logging.error(f"An error occurred creating the user: {error.status}: {error.message}")
            st.error(error.message)


async def login(username:str, password: str):
    logging.info(f"Logging in: username: {username}, password: {'*' * len(password)}")

    request = proto.auth_pb2.LoginRequest(username=username, password=password)

    async with Channel(services.AUTH) as channel:
        stub = proto.auth_grpc.AuthStub(channel)
        try:
            response = await stub.Login(request)
            await Store.async_disk_store(TOKEN, response.token)
            logging.info('Login successful')
            return True
        except GRPCError as error:
            logging.error(error.message)
            st.error(error.message)
