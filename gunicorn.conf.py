import os

bind = f"0.0.0.0:{os.environ.get('PORT', '5000')}"
worker_class = "gevent"
workers = 1
timeout = 600
