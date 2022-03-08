import json
import urllib3
from keycloak import KeycloakOpenID

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

server_config = json.loads(open('config.json').read())

# Client configuration
keycloak_openid = KeycloakOpenID(server_url=server_config['issuer'],
                                 client_id=server_config['client_id'],
                                 realm_name=server_config['realm'],
                                 client_secret_key=server_config['client_secret'],
                                 verify=server_config['verify'])


# Get the well known document
def get_well_known():
    return keycloak_openid.well_know()


# Get Token
def user_token(name, password):
    return keycloak_openid.token(name, password)

# Refresh Token
def refresh_token(refresh_token):
    return keycloak_openid.refresh_token(refresh_token)


# All the Token information (introspect token). Best whay of obtaining user information
def introspect_token(access_token):
    return keycloak_openid.introspect(access_token)

# Decode Token
def decode_token(access_token):
    KEYCLOAK_PUBLIC_KEY = keycloak_openid.public_key()
    options = {"verify_signature": False, "verify_aud": False, "exp": True}
    return keycloak_openid.decode_token(access_token, key=KEYCLOAK_PUBLIC_KEY, options=options)


# Get user information
def user_info(access_token):
    return keycloak_openid.userinfo(access_token)


# Logout
def logout(refresh_token):
    keycloak_openid.logout(refresh_token)


def all_info(name, password):
    tokens = user_token(name, password)
    return {'tokens': tokens,
            'session_information': introspect_token(tokens['access_token'])}


# 'introspection_endpoint': 'https://aai-test.emso.eu:8443/auth/realms/EMSO/protocol/openid-connect/token/introspect',
# 'token_endpoint': 'https://aai-test.emso.eu:8443/auth/realms/EMSO/protocol/openid-connect/token'

# Without module
# import requests

# url = 'https://aai.emso.eu/auth/realms/EMSO/protocol/openid-connect/token'
# header = {
#     "Connection": "keep-alive",
#     "Content-Type": "application/x-www-form-urlencoded",
#     "Accept": "*/*",
#     "Accept-Encoding": "gzip, deflate, br"
# }


# def user_info(name, password):
#     payload = {"grant_type": "password",
#                "client_id": "DataPortal",
#                "client_secret": '291a5a7a-2807-4bad-8c13-3bb6fd708187'}
#     headers = header

#     payload['password'] = password
#     payload['username'] = name

#     tokens = json.loads(requests.post(url, data=payload, headers=headers, verify=False).content)

#     payload_1 = {
#         "client_id": "DataPortal",
#         "client_secret": "291a5a7a-2807-4bad-8c13-3bb6fd708187",
#         "username": name,
#         'token': tokens['access_token']}

#     dat = json.loads(
#         requests.post('https://aai.emso.eu/auth/realms/EMSO/protocol/openid-connect/token/introspect',
#                       data=payload_1, headers=headers, verify=False).content)

#     return dat

token = user_token('andreu', '1234')['access_token']
print(user_info(token))
