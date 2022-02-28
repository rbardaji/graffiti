from keycloak import KeycloakOpenID  # pip install python-keycloak

from config import keycloak_config


# Client configuration
keycloak_openid = KeycloakOpenID(server_url=keycloak_config['issuer'],
                                 client_id=keycloak_config['client_id'],
                                 realm_name=keycloak_config['realm'],
                                 client_secret_key=keycloak_config[
                                     'client_secret'],
                                 verify=keycloak_config['verify'])


def get_token_info(new_request):
    """
    The token is provided in the header of the request, with the key
    'Authorization'. It returns the user information associated with the token.

    Parameters
    ----------
        new_request: request object
    
    Returns
    -------
        (response_object, status_code): (dict, int)
            The response_object contains the user information.
            The status_code is 200 if the token is valid and 401 if the token is
            invalid.
    """
    # get the auth token
    auth_token = new_request.headers.get('Authorization')

    if auth_token:
        try:
            # Decode token with keyloak
            KEYCLOAK_PUBLIC_KEY = keycloak_openid.public_key()
            options = {"verify_signature": False, "verify_aud": False, "exp": True}
            token_info = keycloak_openid.decode_token(
                auth_token, key=KEYCLOAK_PUBLIC_KEY, options=options)

            admin = 0
            if '/api_admin' in token_info['groups']:
                admin = 1

            response_object = {
                'status': True,
                'message': 'Success',
                'result': {
                    'user_id': token_info['preferred_username'],
                    'email': token_info['email'],
                    'admin': admin
                }
            }

            return response_object, 201
        
        except:
            response_object = {
                'status': False,
                'message': 'Invalid token',
                'result': []
            }
            return response_object, 401
    else:
        response_object = {
            'status': False,
            'message': 'Provide a valid Token',
            'result': []
        }
        return response_object, 401
