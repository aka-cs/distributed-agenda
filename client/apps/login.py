import jwt
import streamlit as st
import pandas as pd
import numpy as np
from data.create_data import create_table

import logging
import grpc
from grpclib.client import Channel
from grpclib import GRPCError, Status
import proto.users_pb2
import proto.users_grpc
import proto.auth_pb2
import proto.auth_grpc
import bcrypt
import asyncio as aio

TOKEN = 'token'


def get_user():
    token = st.session_state.get(TOKEN)
    if not token:
        return None

    with open('pub.pem', 'rb') as pub:
        public_key = pub.read()

    info = jwt.decode(token, public_key, algorithms=['RS256'])

    return info


async def app():

    loop = aio.get_event_loop()

    token: str = st.session_state.get(TOKEN)

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

    # ip = st.number_input(label='', key=1), st.number_input(label='', key=2), st.number_input(label='', key=3), st.number_input(label='', key=4)
    ip = '192.168.175.195'

    with st.form('login') as form:
        username = st.text_input('Username')
        if signup_state:
            name = st.text_input('Name')
            email = st.text_input('Email')
        password = st.text_input('Password', type='password')

        if signup_state:
            submitted = st.form_submit_button('Sign Up')
            if submitted:
                result = await signup(username, password, name, email, ip)
        else:
            submitted = st.form_submit_button('Log In')
            if submitted:
                result = await login(username, password, ip)
                if result:
                    st.experimental_rerun()


def sync(f, loop):
    def wrapper(*args, **kwargs):
        return loop.run_until_complete(f(*args, **kwargs))
    return wrapper


async def signup(username: str, password: str, name: str, email: str,  ip: str):
    logging.info(f"Creating user: username: {username}, name: {name}, email: {email}, password: {'*' * len(password)}")
    salt = bcrypt.gensalt()

    user = proto.users_pb2.User(username=username, name=name,
                                passwordHash=bcrypt.hashpw(password.encode(), salt).decode(), email=email)

    request = proto.users_pb2.CreateUserRequest(user=user)

    async with Channel(ip, 50051) as channel:
        stub = proto.users_grpc.UserServiceStub(channel)
        try:
            response = await stub.CreateUser(request)
            logging.info(f"User created with response result: {response.result}")
            st.success("User created!")
            return True
        except GRPCError as error:
            logging.error(f"An error occurred creating the user: {error.status}: {error.message}")
            st.error(error.message)


async def login(username:str, password: str, ip: str):
    logging.info(f"Logging in: username: {username}, password: {'*' * len(password)}")

    request = proto.auth_pb2.LoginRequest(username=username, password=password)

    async with Channel(ip, 50054) as channel:
        stub = proto.auth_grpc.AuthStub(channel)
        try:
            response = await stub.Login(request)
            st.session_state[TOKEN] = response.token
            st.success('Logged In')
            return True
        except GRPCError as error:
            logging.error(error.message)
            st.error(error.message)


async def logout():
    if st.session_state.get(TOKEN):
        del st.session_state[TOKEN]