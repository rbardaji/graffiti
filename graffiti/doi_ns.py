from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request
from .utils.db_manager import get_doi, post_doi

api = Namespace('doi', description='DOI management')

# identifiers_model = api.schema_model('identifiers_model', {
#     'data': {
#         'id': {'type': 'string'},
#         'type': {'type': 'string'},
#         'attributes': {
#             'doi': {'type': 'string'},
#             'event': {'type': 'string'},
#             'prefix': {'type': 'string'},
#             'suffix': {'type': 'string'},
#             'identifiers': [],
#             'type': 'object'},
#     },
#     'type': 'object'
# })

# doy_payload = api.schema_model('doi_content', {
#     'data': {
#         'id': {'type': 'string'},
#         'type': {'type': 'string'},
#         'attributes': {
#             'doi': {'type': 'string'},
#             'event': {'type': 'string'},
#             'prefix': {'type': 'string'},
#             'suffix': {'type': 'string'},
#             'identifiers': [],
#             'type': 'object'},
#     },
#     'type': 'object'
# })

doy_payload = api.model('doi_content', {
    'data': fields.Raw(required=True, example={
        "attributes": {
        "suffix": "DOI sufix, only use it if we do not have doi and id fields and want to specify the DOI sufix",
        "identifiers": [
            {
            "identifier": "DOI as url -> Example https://doi.org/10.80110/0001",
            "identifierType": "Type of identifier, its value is DOI -> DOI"
            }
        ],
        "creators": [
            {
            "name": "Creator name",
            "givenName": "The personal or first name of the creator",
            "familyName": "The surname or last name of the creator",
            "nameType": "Description for the name type, can be Organizational or Personal -> Example Personal",
            "nameIdentifiers": [
                {
                "nameIdentifier": "Uniquely identifies an individual or legal entity, according to various schemes",
                "nameIdentifierScheme": "Name of the name identifier scheme -> Example ORCID",
                "schemeUri": "URI of the name identifier scheme -> Example https://orcid.org"
                }
            ],
            "affiliation": [
                {
                "name": "Name of the affiliation identifier",
                "affiliationIdentifier": "Uniquely identifies the organizational affiliation of the creator",
                "affiliationIdentifierScheme": "The name of the affiliation identifier scheme",
                "schemeUri": "The URI of the affiliation identifier scheme"
                }
            ]
            }
        ],
        "titles": [
            {
            "title": "DataCite Metadata Schema Documentation for the Publication and Citation of Research Data v4.0",
            "titleType": "The type of Title, it can be: AlternativeTitle, Subtitle, TranslatedTitle, Other -> Example AlternativeTitle",
            "lang": "Idiom -> Example English"
            }
        ],
        "publicationYear": 2021,
        "subjects": [
            {
            "subject": "Subject, keyword, classification code, or key phrase describing the resource",
            "subjectScheme": "The name of the subject scheme or classification code or authority if one is used",
            "schemeUri": "The URI of the subject identifier scheme",
            "valueUri": "The URI of the subject term",
            "lang": "Idiom -> Example English"
            }
        ],
        "contributors": [
            {
            "nameIdentifiers": [
                {
                "nameIdentifier": "Uniquely identifies an individual or legal entity, according to various schemes",
                "nameIdentifierScheme": "The name of the name identifier scheme",
                "schemeUri": "The URI of the name identifier scheme"
                }
            ],
            "nameType": "The type of name, values:  Organizational, Personal -> Example Personal",
            "name": "The full name of the contributor",
            "givenName": "The personal or first name of the contributor",
            "familyName": "The surname or last name of the contributor",
            "affiliation": "The organizational or institutional affiliation of the contributor",
            "contributorType": "The type of contributor of the resource, values: ContactPerson, DataCollector, DataCurator, DataManager, Distributor, Editor, HostingInstitution, Producer, ProjectLeader, ProjectManager, ProjectMember, RegistrationAgency, RegistrationAuthority, RelatedPerson, Researcher, ResearchGroup, RightsHolder, Sponsor, Supervisor, WorkPackageLeader, Other -> Example Editor"
            }
        ],
        "dates": [
            {
            "date": "Different dates relevant to the work, values: YYYY, YYYY-MM-DD, YYYY-MM-DDThh:mm:ssTZD or any other format described in W3CDTF -> Example 2004-03-02",
            "dateType": "The type of date, values: Accepted, Available, Copyrighted, Collected, Created, Issued, Submitted, Updated, Valid, Withdrawn, Other -> Example Created"
            }
        ],
        "language": "The primary language of the resource, recommended values are taken from IETF BCP 47, ISO 639-1 language codes -> Example en",
        "types": {
            "resourceTypeGeneral": "The general type of the resource, values: Audiovisual, Book, BookChapter, Collection, ComputationalNotebook, ConferencePaper, ConferenceProceeding, DataPaper, Dataset, Dissertation, Event, Image, InteractiveResource, Model, PeerReview, Report, Software, Sound, Standard, Text, Workflow, Other",
            "resourceType": "A description of the resource, free-format text. The recommended content is a single term of some detail so that a pair can be formed with the resourceTypeGeneral subproperty"
        },
        "relatedIdentifiers": [
            {
            "relatedIdentifier": "Identifiers of related resources. These must be globally unique identifiers. Free text",
            "relatedIdentifierType": "The type of the RelatedIdentifier, values: ARK, arXiv, bibcode, DOI, EAN13, EISSN, Handle, IGSN, ISBN, ISSN, ISTC, LISSN, LSID, PMID, PURL, UPC, URL, URN, w3id",
            "resourceTypeGeneral": "The general type of the related resource, values: Audiovisual, Book, BookChapter, Collection, ComputationalNotebook, ConferencePaper, ConferenceProceeding, DataPaper, Dataset, Dissertation, Event, Image, InteractiveResource, Model, PeerReview, Report, Software, Sound, Standard, Text, Workflow, Other",
            "relationType": "The realtion with the DOI, values: IsCitedBy, Cites, IsSupplementTo, IsSupplementedBy, IsContinuedBy, Continues, IsNewVersionOf, IsPreviousVersionOf, IsPartOf, HasPart, IsPublishedIn, IsReferencedBy, References, IsDocumentedBy, Documents, IsCompiledBy, Compiles, IsVariantFormOf, IsOriginalFormOf, IsIdenticalTo, HasMetadata, IsMetadataFor, Reviews, IsReviewedBy, IsDerivedFrom, IsSourceOf, Describes, IsDescribedBy, HasVersion, IsVersionOf, Requires, IsRequiredBy, Obsoletes, IsObsoletedBy"
            }
        ],
        "sizes": [
            "sizes of the files -> Example 10Mb"
        ],
        "formats": [
            "formats of the files -> Example JSON"
        ],
        "rightsList": [
            {
            "rights": "Any rights information for this resource. The property may be repeated to record complex rights characteristics. Free text -> Example Apache License, Version 2.0",
            "rightsUri": "The URI of the license",
            "lang": "Idiom -> Example English"
            }
        ],
        "descriptions": [
            {
            "description": "All additional information that does not fit in any of the other categories. May be used for technical information. free text",
            "descriptionType": "The type of the description, values: Abstract, Methods, SeriesInformation, TableOfContents, TechnicalInfo, Other",
            "lang": "Idiom -> Example English"
            }
        ],
        "geoLocations": [
            {
            "geoLocationPoint": {
                "pointLongitude": "Longitude of the geographic point expressed in decimal degrees (positive east), between -180 and 180 -> Example -67",
                "pointLatitude": "Latitude of the geographic point expressed in decimal degrees (positive north), between -90 and 90 -> Example 31.233"
            },
            "geoLocationBox": {
                "westBoundLongitude": "Longitude of the geographic point expressed in decimal degrees (positive east), between -180 and 180 -> Example -67",
                "eastBoundLongitude": "Longitude of the geographic point expressed in decimal degrees (positive east), between -180 and 180  -> Example 31.233",
                "southBoundLatitude": "Latitude of the geographic point expressed in decimal degrees (positive north), between -90 and 90  -> Example 31.233",
                "northBoundLatitude": "Latitude of the geographic point expressed in decimal degrees (positive north), between -90 and 90  -> Example 31.233"
            },
            "geoLocationPlace": "Description of a geographic location. Free text"
            }
        ],
        "fundingReferences": [
            {
            "funderName": "Name of the funding provider",
            "funderIdentifier": "Uniquely identifies a funding entity, according to various types.",
            "funderIdentifierType": "The type of the funderIdentifier, values: Crossref Funder ID, GRID, ISNI, ROR, Other",
            "awardNumber": "The code assigned by the funder to a sponsored award (grant)",
            "awardUri": "The URI leading to a page provided by the funder for more information about the award (grant)",
            "awardTitle": "The human readable title or name of the award (grant)"
            }
        ],
    }})})


# doy_payload = api.model('doi_content', {
#     'data': fields.Raw(required=True, example={
#         "id": "DOI prefix and sufix, if we use this field, delete the prefix sub-field of the attributes field -> Example 10.80110/0001",
#         "type": "statics field, its value is dois -> dois",
#         "attributes": {
#         "doi": "DOI prefix and sufix, if we use this field, delete the prefix sub-field of the attributes field -> Example 10.80110/0001",
#         "event": "State of the DOI, it's values can be publish, register or hide -> Example publish",
#         "prefix": "DOI prefix, if we use this field, delete the id and doi field, it will rendom generate the sufix -> Example 10.80110",
#         "suffix": "DOI sufix, only use it if we do not have doi and id fields and want to specify the DOI sufix",
#         "identifiers": [
#             {
#             "identifier": "DOI as url -> Example https://doi.org/10.80110/0001",
#             "identifierType": "Type of identifier, its value is DOI -> DOI"
#             }
#         ],
#         "creators": [
#             {
#             "name": "Creator name",
#             "givenName": "The personal or first name of the creator",
#             "familyName": "The surname or last name of the creator",
#             "nameType": "Description for the name type, can be Organizational or Personal -> Example Personal",
#             "nameIdentifiers": [
#                 {
#                 "nameIdentifier": "Uniquely identifies an individual or legal entity, according to various schemes",
#                 "nameIdentifierScheme": "Name of the name identifier scheme -> Example ORCID",
#                 "schemeUri": "URI of the name identifier scheme -> Example https://orcid.org"
#                 }
#             ],
#             "affiliation": [
#                 {
#                 "name": "Name of the affiliation identifier",
#                 "affiliationIdentifier": "Uniquely identifies the organizational affiliation of the creator",
#                 "affiliationIdentifierScheme": "The name of the affiliation identifier scheme",
#                 "schemeUri": "The URI of the affiliation identifier scheme"
#                 }
#             ]
#             }
#         ],
#         "titles": [
#             {
#             "title": "DataCite Metadata Schema Documentation for the Publication and Citation of Research Data v4.0",
#             "titleType": "The type of Title, it can be: AlternativeTitle, Subtitle, TranslatedTitle, Other -> Example AlternativeTitle",
#             "lang": "Idiom -> Example English"
#             }
#         ],
#         "publisher": "The name of the entity that holds, archives, publishes prints, distributes, releases, issues, or produces the resource, used to formulate the citation -> Example European Multidisciplinary Seafloor and water-column Observatory (EMSO)",
#         "publicationYear": 2021,
#         "subjects": [
#             {
#             "subject": "Subject, keyword, classification code, or key phrase describing the resource",
#             "subjectScheme": "The name of the subject scheme or classification code or authority if one is used",
#             "schemeUri": "The URI of the subject identifier scheme",
#             "valueUri": "The URI of the subject term",
#             "lang": "Idiom -> Example English"
#             }
#         ],
#         "contributors": [
#             {
#             "nameIdentifiers": [
#                 {
#                 "nameIdentifier": "Uniquely identifies an individual or legal entity, according to various schemes",
#                 "nameIdentifierScheme": "The name of the name identifier scheme",
#                 "schemeUri": "The URI of the name identifier scheme"
#                 }
#             ],
#             "nameType": "The type of name, values:  Organizational, Personal -> Example Personal",
#             "name": "The full name of the contributor",
#             "givenName": "The personal or first name of the contributor",
#             "familyName": "The surname or last name of the contributor",
#             "affiliation": "The organizational or institutional affiliation of the contributor",
#             "contributorType": "The type of contributor of the resource, values: ContactPerson, DataCollector, DataCurator, DataManager, Distributor, Editor, HostingInstitution, Producer, ProjectLeader, ProjectManager, ProjectMember, RegistrationAgency, RegistrationAuthority, RelatedPerson, Researcher, ResearchGroup, RightsHolder, Sponsor, Supervisor, WorkPackageLeader, Other -> Example Editor"
#             }
#         ],
#         "dates": [
#             {
#             "date": "Different dates relevant to the work, values: YYYY, YYYY-MM-DD, YYYY-MM-DDThh:mm:ssTZD or any other format described in W3CDTF -> Example 2004-03-02",
#             "dateType": "The type of date, values: Accepted, Available, Copyrighted, Collected, Created, Issued, Submitted, Updated, Valid, Withdrawn, Other -> Example Created"
#             }
#         ],
#         "language": "The primary language of the resource, recommended values are taken from IETF BCP 47, ISO 639-1 language codes -> Example en",
#         "types": {
#             "resourceTypeGeneral": "The general type of the resource, values: Audiovisual, Book, BookChapter, Collection, ComputationalNotebook, ConferencePaper, ConferenceProceeding, DataPaper, Dataset, Dissertation, Event, Image, InteractiveResource, Model, PeerReview, Report, Software, Sound, Standard, Text, Workflow, Other",
#             "resourceType": "A description of the resource, free-format text. The recommended content is a single term of some detail so that a pair can be formed with the resourceTypeGeneral subproperty"
#         },
#         "relatedIdentifiers": [
#             {
#             "relatedIdentifier": "Identifiers of related resources. These must be globally unique identifiers. Free text",
#             "relatedIdentifierType": "The type of the RelatedIdentifier, values: ARK, arXiv, bibcode, DOI, EAN13, EISSN, Handle, IGSN, ISBN, ISSN, ISTC, LISSN, LSID, PMID, PURL, UPC, URL, URN, w3id",
#             "resourceTypeGeneral": "The general type of the related resource, values: Audiovisual, Book, BookChapter, Collection, ComputationalNotebook, ConferencePaper, ConferenceProceeding, DataPaper, Dataset, Dissertation, Event, Image, InteractiveResource, Model, PeerReview, Report, Software, Sound, Standard, Text, Workflow, Other",
#             "relationType": "The realtion with the DOI, values: IsCitedBy, Cites, IsSupplementTo, IsSupplementedBy, IsContinuedBy, Continues, IsNewVersionOf, IsPreviousVersionOf, IsPartOf, HasPart, IsPublishedIn, IsReferencedBy, References, IsDocumentedBy, Documents, IsCompiledBy, Compiles, IsVariantFormOf, IsOriginalFormOf, IsIdenticalTo, HasMetadata, IsMetadataFor, Reviews, IsReviewedBy, IsDerivedFrom, IsSourceOf, Describes, IsDescribedBy, HasVersion, IsVersionOf, Requires, IsRequiredBy, Obsoletes, IsObsoletedBy"
#             }
#         ],
#         "sizes": [
#             "sizes of the files -> Example 10Mb"
#         ],
#         "formats": [
#             "formats of the files -> Example JSON"
#         ],
#         "version": "The version number of the resource -> Example 0.1.2",
#         "rightsList": [
#             {
#             "rights": "Any rights information for this resource. The property may be repeated to record complex rights characteristics. Free text -> Example Apache License, Version 2.0",
#             "rightsUri": "The URI of the license",
#             "lang": "Idiom -> Example English"
#             }
#         ],
#         "descriptions": [
#             {
#             "description": "All additional information that does not fit in any of the other categories. May be used for technical information. free text",
#             "descriptionType": "The type of the description, values: Abstract, Methods, SeriesInformation, TableOfContents, TechnicalInfo, Other",
#             "lang": "Idiom -> Example English"
#             }
#         ],
#         "geoLocations": [
#             {
#             "geoLocationPoint": {
#                 "pointLongitude": "Longitude of the geographic point expressed in decimal degrees (positive east), between -180 and 180 -> Example -67",
#                 "pointLatitude": "Latitude of the geographic point expressed in decimal degrees (positive north), between -90 and 90 -> Example 31.233"
#             },
#             "geoLocationBox": {
#                 "westBoundLongitude": "Longitude of the geographic point expressed in decimal degrees (positive east), between -180 and 180 -> Example -67",
#                 "eastBoundLongitude": "Longitude of the geographic point expressed in decimal degrees (positive east), between -180 and 180  -> Example 31.233",
#                 "southBoundLatitude": "Latitude of the geographic point expressed in decimal degrees (positive north), between -90 and 90  -> Example 31.233",
#                 "northBoundLatitude": "Latitude of the geographic point expressed in decimal degrees (positive north), between -90 and 90  -> Example 31.233"
#             },
#             "geoLocationPlace": "Description of a geographic location. Free text"
#             }
#         ],
#         "fundingReferences": [
#             {
#             "funderName": "Name of the funding provider",
#             "funderIdentifier": "Uniquely identifies a funding entity, according to various types.",
#             "funderIdentifierType": "The type of the funderIdentifier, values: Crossref Funder ID, GRID, ISNI, ROR, Other",
#             "awardNumber": "The code assigned by the funder to a sponsored award (grant)",
#             "awardUri": "The URI leading to a page provided by the funder for more information about the award (grant)",
#             "awardTitle": "The human readable title or name of the award (grant)"
#             }
#         ],
#         "isActive": "Boolean indicating if the DOI is active -> Example true",
#         "metadataVersion": "Version of the DOI metadata -> Example 0.1.1",
#         "url": "https://schema.datacite.org/meta/kernel-4.0/index.html",
#         "schemaVersion": "http://datacite.org/schema/kernel-4"
#     }})})

@api.route('/<string:user>')
@api.response(201, 'Created')
@api.response(401, 'Provide a valid Token')
@api.response(403, 'Not available DOIs')
@api.response(502, 'API cannot access to the DB')
class GetPostDOI(Resource):
    @save_request
    def get(self, user):
        """
        Get the number of available DOIs per user
        """
        return get_doi(user)

    @api.expect(doy_payload)
    @save_request
    def post(self, user):
        """
        Post the document of the payload and get a DOI number
        """
        return post_doi(user, api.payload)
