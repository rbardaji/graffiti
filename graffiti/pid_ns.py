from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request, token_required
from .utils.db_manager import get_pid, post_pid


api = Namespace('pid', description='PID management')


name_identifiers = api.model('name_identifiers', {
    'nameIdentifier': fields.String(
        required=True, description='Name identifier of the creator'),
    'nameIdentifierScheme': fields.String(
        required=True,
        description='Scheme of the name identifier of the creator'),
    'schemeUri': fields.String(
        required=True,
        description='Scheme URI of the name identifier of the creator')
})


affiliation = api.model('affiliation', {
    'name': fields.String(required=True, description='Name of the affiliation'),
    'affiliationIdentifier': fields.String(
        required=True, description='Affiliation identifier of the affiliation'),
    'affiliationIdentifierScheme': fields.String(
        required=True,
        description='Scheme of the affiliation identifier of the affiliation'),
    'schemeUri': fields.String(
        required=True,
        description='Scheme URI of the affiliation identifier of the ' + \
            'affiliation')
})


creator = api.model('creator', {
    'name': fields.String(required=True, description='Name of the creator'),
    'givenName': fields.String(required=True,
                               description='Given name of the creator'),
    'familyName': fields.String(required=True,
                                description='Family name of the creator'),
    'nameType': fields.String(required=True,
                              description='Type of the creator'),
    'nameIdentifiers': fields.List(
        fields.Nested(name_identifiers), required=True,
        description='Name identifiers of the creator'),
    'affiliation': fields.List(
        fields.Nested(affiliation), required=True, description='Affiliation'),})


title = api.model('title', {
    'title': fields.String(required=True, description='Title of the work'),
    'titleType': fields.String(required=True, description='Type of the title'),
    'lang': fields.String(required=True, description='Language of the title')
})


description = api.model('description', {
    'description': fields.String(required=True,
                                 description='Description of the work'),
    'descriptionType': fields.String(required=True,
                                     description='Type of the description'),
    'lang': fields.String(required=True,
                          description='Language of the description')
})


pid = api.model('pid', {
    'resource': fields.String(required=True,
                              description='Link to the resource'),
    'resourceTypeGeneral': fields.String(
        required=True,
        description='A description of the resource, free-format text. ' + \
            'The recommended content is a single term of some detail so ' + \
            'that a pair can be formed with the "Resource type" ' + \
            'subproperty'),
    'version': fields.String(required=True,
                             description='Version of the resource'),
    'creators': fields.List(fields.Nested(creator), required=True),
    'titles': fields.List(fields.Nested(title),
                          required=True, description='Titles of the resource'),
    'descriptions': fields.List(fields.Nested(description),
                                required=True,
                                description='Descriptions of the resource')
})


@api.route('/<string:email>')
@api.response(201, 'PID created')
@api.response(204, 'Email not found')
@api.response(401, 'Provide a valid Token')
@api.response(500, 'Server error')
@api.response(503, 'Connection error with the DB')
class GetPostPid(Resource):
    """ PID operations"""
    @api.doc(security='apikey')
    @token_required
    @save_request
    @api.expect(pid)
    def post(self, email):
        """
        Create a PID for a resource
        """
        return post_pid(api.payload, email, DOI=False)

    @api.doc(security='apikey')
    @token_required
    @save_request
    def get(self, email):
        """
        Get the PIDs from a user
        """
        return get_pid(email)
