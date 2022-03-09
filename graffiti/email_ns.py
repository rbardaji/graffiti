from flask_restx import Namespace, Resource, fields

from .utils.decorator import save_request, token_required
from .services.email_service import send_email

api = Namespace('email', description='Send an email to the help service')

email_payload = api.model('email_conent', {
    'from': fields.String(),
    'subject': fields.String(),
    'message': fields.String()})


@api.route('')
@api.response(201, 'Email sent')
@api.response(505, 'Email cannot be sent')
class SendEmail(Resource):
    @api.doc(security='apikey')
    @api.expect(email_payload)
    @token_required
    @save_request
    def post(self):
        """
        Send an email to the help service.
        """
        return send_email(api.payload)
