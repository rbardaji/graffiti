from elasticsearch_dsl import Nested
from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request, token_required
from .utils.db_manager import get_doi, post_doi


api = Namespace('doi', description='DOI management')


related_identifiers = api.model('relatedIdentifiers', {
    'relatedIdentifier': fields.String(
        required=True, description='Related identifier'),
    'relatedIdentifierType': fields.String(
        required=True, description='Related identifier type'),
    'resourceTypeGeneral': fields.String(
        required=True, description='Resource type general'),
    'relationType': fields.String(
        required=True, description='Relation type')
})


name_identifiers = api.model('name_identifiers', {
    'nameIdentifier': fields.String(required=True, description='Name identifier'),
    'nameIdentifierScheme': fields.String(required=True, description='Name identifier scheme'),
    'schemeUri': fields.String(required=True, description='Scheme URI')
})


affiliation = api.model('affiliation', {
    'name': fields.String(required=True, description='Name'),
    'affiliationIdentifier': fields.String(required=True, description='Affiliation identifier'),
    'affiliationIdentifierScheme': fields.String(required=True, description='Affiliation identifier scheme'),
    'schemeUri': fields.String(required=True, description='Scheme URI')
})


creator_identifiers = api.model('creator_identifiers', {
    'name': fields.String(
        required=True, description='Name identifier of the creator'),
    'givenName': fields.String(
        required=True, description='Given name of the creator'),
    'familyName': fields.String(
        required=True, description='Family name of the creator'),
    'nameType': fields.String(
        required=True,
        description='Type of name'),
    'nameIdentifiers': fields.List(
        fields.Nested(name_identifiers), required=True,
        description='Name identifiers of the creator'),
    'affiliation': fields.List(
        fields.Nested(affiliation), required=True,
        description='Affiliation of the creator')
})


title_identifiers = api.model('title_identifiers', {
    'title': fields.String(
        required=True, description='Title of the work'),
    'titleType': fields.String(
        required=False, description='Type of title'),
    'lang': fields.String(
        required=False, description='Language of the title')
})


type_identifiers = api.model('type_identifiers', {
    'resourceTypeGeneral': fields.String(
        required=True, description='resourceTypeGeneral'),
    'resourceType': fields.String(
        required=True, description='resourceType')
})


contributors = api.model('contributors', {
    'name': fields.String(required=True, description='Name of the contributor'),
    'givenName': fields.String(required=True, description='Given name of the contributor'),
    'familyName': fields.String(required=True, description='Family name of the contributor'),
    'nameType': fields.String(required=True, description='Type of name'),
    'contributorType': fields.String(required=True, description='Type of contributor'),
    'nameIdentifiers': fields.List(
        fields.Nested(name_identifiers), required=True,
        description='Name identifiers of the contributor')
})


description = api.model('description', {
    'description': fields.String(required=True, description='Description'),
    'descriptionType': fields.String(required=True, description='Description type'),
    'lang': fields.String(required=True, description='Language')
})


geo_location_point = api.model('geo_location_point', {
    'pointLongitude': fields.String(required=True, description='Point longitude'),
    'pointLatitude': fields.String(required=True, description='Point latitude')
})


geo_location = api.model('geo_location', {
    'geoLocationPoint': fields.Nested(geo_location_point, required=True, description='Geo location point'),
    'geoLocationPlace': fields.String(required=True, description='Geo location place')
})


rights = api.model('rights', {
    'rights': fields.String(required=True, description='Rights'),
    'rightsUri': fields.String(required=True, description='Rights URI'),
    'lang': fields.String(required=True, description='Language')
})


funding_reference = api.model('funding_reference', {
    'funderName': fields.String(required=True, description='Funder name'),
    'funderIdentifier': fields.String(required=True, description='Funder identifier'),
    'funderIdentifierType': fields.String(required=True, description='Funder identifier type'),
    'awardNumber': fields.String(required=True, description='Award number'),
    'awardUri': fields.String(required=True, description='Award URI'),
    'awardTitle': fields.String(required=True, description='Award title')
})


doi_info = api.model('doi_info', {
    'url': fields.String(required=True, description='Link to the resource'),
    'creators': fields.List(fields.Nested(creator_identifiers), required=True),
    'titles': fields.List(fields.Nested(title_identifiers),
                          required=True, description='Titles of the resource'),
    'contributors': fields.List(fields.Nested(contributors), required=True),
    'types': fields.Nested(type_identifiers),
    'relatedIdentifiers': fields.List(
        fields.Nested(related_identifiers), required=True,
        description='Related identifiers of the resource'),
    'descriptions': fields.List(
        fields.Nested(description), required=True,
        description='Descriptions of the resource'),
    'geoLocations': fields.List(
        fields.Nested(geo_location), required=True,
        description='Geo locations of the resource'),
    'formats': fields.List(
        fields.String, required=True,
        description='Formats of the resource'),
    'sizes': fields.List(
        fields.String, required=True,
        description='Sizes of the resource'),
    'rightsList': fields.List(
        fields.Nested(rights), required=True,
        description='Rights of the resource'),
    'fundingReferences': fields.List(
        fields.Nested(funding_reference), required=True,
        description='Funding references of the resource')
})


@api.route('')
@api.response(201, 'DOI created')
@api.response(204, 'Email not found')
@api.response(401, 'Provide a valid Token')
@api.response(500, 'Server error')
@api.response(503, 'Connection error with the DB')
class GetPostDOI(Resource):
    """ DOI operations"""
    @api.doc(security='apikey')
    @token_required
    @save_request
    @api.expect(doi_info)
    def post(self):
        """
        Create a PID for a resource
        """
        return post_doi(api.payload)


    @api.doc(security='apikey')
    @token_required
    @save_request
    def get(self):
        """
        Get the DOIs from a user
        """
        return get_doi()
