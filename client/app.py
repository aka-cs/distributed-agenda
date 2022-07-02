import asyncio
import logging
import threading
import time

from apps import home, login, get_user, groups, history  # import your app modules here
from multiapp import MultiApp
from rpc.history import update_history
from store import Storage

app = MultiApp(get_user())

loop = asyncio.get_event_loop_policy().new_event_loop()


def every(__seconds: float, func, *args, **kwargs):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    while True:
        try:
            loop.run_until_complete(func(*args, **kwargs))
        except BaseException as e:
            logging.error(f'function call failed with exception: {e}')
            raise e
        time.sleep(__seconds)


# run every in a different thread
if not Storage.get('repeat'):
    threading.Thread(target=every, args=(60, update_history)).start()
    Storage.store('repeat', True)

# Add all your application here
app.add_app("Login", login.app)
app.add_app("Group", groups.app)
app.add_app("Home", home.app)
app.add_app("History", history.app)

# The main app
loop.run_until_complete(app.run())
