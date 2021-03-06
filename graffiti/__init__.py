from flask_restx import Api

from .figure_ns import api as figure_api
from .admin_figure_ns import api as admin_figure_api
from .data_ns import api as data_api
from .vocabulary_ns import api as vocabulary_api
from .admin_vocabulary_ns import api as admin_vocabulary_api
from .admin_metadata_ns import api as admin_metadata_api
from .metadata_ns import api as metadata_api
from .admin_data_ns import api as admin_data_api
from .email_ns import api as email_api
from .doi_ns import api as doi_api
from .pid_ns import api as pid_api
from .user_ns import api as user_api
from .admin_doi_ns import api as admin_doi_api

from config import swagger_title, swagger_version

authorizations = {
    'apikey': {
        'type': 'apiKey',
        'in': 'header',
        'name': 'Authorization'
    }
}


api = Api(authorizations=authorizations, title=swagger_title,
          version=swagger_version,
          description='API that allows you to search and obtain and ' + \
              'visualize oceanographic data')


api.add_namespace(user_api)
api.add_namespace(email_api)
api.add_namespace(metadata_api)
api.add_namespace(vocabulary_api)
api.add_namespace(data_api)
api.add_namespace(figure_api)
api.add_namespace(admin_data_api)
api.add_namespace(admin_metadata_api)
api.add_namespace(admin_vocabulary_api)
api.add_namespace(admin_figure_api)
api.add_namespace(pid_api)
api.add_namespace(doi_api)
api.add_namespace(admin_doi_api)
