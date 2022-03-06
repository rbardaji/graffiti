from flask_restx import Api

from .figure_ns import api as figure_api
from .admin_figure_ns import api as admin_figure_api
from .data_ns import api as data_api
from .vocabulary_ns import api as vocabulary_api
from .admin_metadata_ns import api as admin_metadata_api
from .metadata_ns import api as metadata_api
from .admin_data_ns import api as admin_data_api
from .email_ns import api as email_api
from .doi_ns import api as doi_api
from .hash_ns import api as hash_api
from .user_ns import api as user_api


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
api.add_namespace(admin_figure_api)
api.add_namespace(data_api)
api.add_namespace(admin_data_api)
api.add_namespace(vocabulary_api)
api.add_namespace(metadata_api)
api.add_namespace(admin_metadata_api)
api.add_namespace(email_api)
api.add_namespace(doi_api)
api.add_namespace(hash_api)
api.add_namespace(user_api)
