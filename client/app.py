import asyncio
import logging
import threading
import time

from apps import home, login, get_user, groups  # import your app modules here
from multiapp import MultiApp
from rpc.history import update_history

app = MultiApp(get_user())

loop = asyncio.get_event_loop_policy().new_event_loop()


def every(__seconds: float, func, *args, **kwargs):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    while True:
        try:
            loop.run_until_complete(func(*args, **kwargs))
        except BaseException as e:
            logging.error(f'function call failed with exception: {e}')
        time.sleep(__seconds)


# run every in a different thread
threading.Thread(target=every, args=(60, update_history)).start()

# Add all your application here
app.add_app("Login", login.app)
app.add_app("Group", groups.app)
app.add_app("Home", home.app)

# The main app
loop.run_until_complete(app.run())
