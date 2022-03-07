from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix
import socket
from flask_cors import CORS

from graffiti import api


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app)
app.config['RESTPLUS_MASK_SWAGGER'] = False  # This is not doing anything
CORS(app)

api.init_app(app)

if __name__ == '__main__':
    hostname = socket.gethostname()
    app.run(hostname, 5001, debug=True)
