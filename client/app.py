import asyncio
import logging
import threading
import time
from streamlit.scriptrunner.script_run_context import get_script_run_ctx, add_script_run_ctx

from apps import events, login, groups, history, conflicts  # import your app modules here
from multiapp import MultiApp
from rpc.client import update_servers, get_user
from rpc.history import update_history
from rpc.requests_queue import process_requests
from store import Storage

user = get_user()
app = MultiApp(user)

loop = asyncio.get_event_loop_policy().new_event_loop()


def every(__seconds: float, func, *args, **kwargs):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    while True:
        try:
            if asyncio.iscoroutinefunction(func):
                loop.run_until_complete(func(*args, **kwargs))
            else:
                func(*args, **kwargs)
        except BaseException as e:
            logging.error(f'function call failed with exception: {e}')
            raise e
        time.sleep(__seconds)


# run every in a different thread
if not Storage.get('repeat'):
    threads = [threading.Thread(target=every, args=(60, update_servers)),
               threading.Thread(target=every, args=(30, process_requests)),
               threading.Thread(target=every, args=(60, update_history))]
    running_ctx = get_script_run_ctx()
    for thread in threads:
        add_script_run_ctx(thread, running_ctx)
        thread.start()
    Storage.store('repeat', True)

# Add all your application here
app.add_app("Login", login.app)
if user:
    app.add_app("Group", groups.app)
    app.add_app("Events", events.app)
    app.add_app("History", history.app)
    app.add_app("Conflicts", conflicts.app)

# The main app
loop.run_until_complete(app.run())
