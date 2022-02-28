from flask_restx import Api

from .figure_ns import api as figure_api
# from .admin_figure_ns import api as admin_figure_api
from .data_ns import api as data_api

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'X-API-KEY'
    }
}

api = Api(authorizations=authorizations, title="Graffiti", version="0.1",
          description="Manage data and make interactive figures.")

api.add_namespace(figure_api)
# api.add_namespace(admin_figure_api)
api.add_namespace(data_api)
