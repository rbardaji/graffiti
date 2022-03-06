from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request
from .services.email_service import send_email

api = Namespace('email', description='Send an email to the help service')

email_payload = api.model('email_conent', {
    'from': fields.String(),
    'subject': fields.String(),
    'message': fields.String()})


@api.response(201, 'Email sent')
@api.route('')
class SendEmail(Resource):
    @save_request
    @api.expect(email_payload)
    def post(self):
        """
        Send an email to help@emso-eu.org
        """
        return send_email(api.payload)
