import urllib3

from keycloak import KeycloakOpenID, exceptions  # pip install python-keycloak
from flask import abort

from config import (keycloak_config, test_token, data_portal_token, test_user,
                    test_email, data_portal_user, data_portal_email)


# Dissable the InsecureRequestWarning that is raised in the connection with the
# AAI
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        (response, status_code): (dict, int)
            The response_object contains the user information.
            The status_code is 200 if the token is valid and 401 if the token is
            invalid.
    """
    # get the auth token
    auth_token = new_request.headers.get('Authorization')

    if auth_token:
        if auth_token == test_token:
            response = {
                'status': True,
                'message': 'Success',
                'result': {
                    'user_id': test_user,
                    'email': test_email,
                    'admin': 1
                }
            }
        elif auth_token == data_portal_token:
            response = {
                'status': True,
                'message': 'Success',
                'result': {
                    'user_id': data_portal_user,
                    'email': data_portal_email,
                    'admin': 0
                }
            }
        else:
            try:
                # Decode token with keyloak
                KEYCLOAK_PUBLIC_KEY = keycloak_openid.public_key()
                options = {"verify_signature": False, "verify_aud": False, "exp": True}
                token_info = keycloak_openid.decode_token(
                    auth_token, key=KEYCLOAK_PUBLIC_KEY, options=options)

                admin = 0
                if '/api_admin' in token_info['groups']:
                    admin = 1

                response = {
                    'status': True,
                    'message': 'Success',
                    'result': {
                        'user_id': token_info['preferred_username'],
                        'email': token_info['email'],
                        'admin': admin
                    }
                }
            
            except:
                abort(401, 'Invalid Token')
        
        return response, 200
    else:
        abort(401, 'Provide a valid Authorization Token')


def get_token(user: str, password: str):
    """
    Get the Authorization Token to use this API 

    Parameters
    ----------
        user: str
            User name
        password: str
            Password from the user

    Returns
    -------
        (response, status_code): dict, int
            response is a dict:
                'status': True,
                'message': 'The Authorization Token is in result[0]'
                'result': List that contains the token in possition 0.
            The status_code is 201.
        The function also can abort the flask app with a code.
        404 - Invalid email or password or
        503 - Impossible to connect with the AAI
    """
    try:

        # Get the Token from the Keycloak
        auth_token = keycloak_openid.token(user, password)['access_token']

        response = {
            'status': True,
            'message': 'The Authorization Token is in result[0]',
            'result': [auth_token]
        }
        status_code = 201

    except exceptions.KeycloakConnectionError:
        abort(503, 'Impossible to connect with the AAI')
    except exceptions.KeycloakAuthenticationError:
        abort(401, 'Invalid email or password')

    return response, status_code


class Auth:

    @staticmethod
    def get_logged_in_user(new_request):

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

            except Exception as e:
                print('Error:')
                print(e)
                
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
