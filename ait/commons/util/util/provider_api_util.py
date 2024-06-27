import requests


class APIProvider:
    def __init__(self, base_url):
        self.base_url = base_url

    def send_request(self, method, url, access_token, params=None, data=None, data_type_in_hal_link=None):
        """
        Sends an HTTP request to the specified URL with the given method.

        Parameters:
            method (str): The HTTP method (GET, POST, PUT, DELETE).
            url (str): The URL to send the request to.
            access_token (str): Access token for authorization.
            params (dict, optional): The URL parameters to be sent with the request.
            data (dict, optional): The data to be sent in the request body.
            data_type_in_hal_link (str, optional): The data type in the HAL link for extracting URL in POST response.

        Returns:
            dict or str: The response data for PUT/DELETE or the URL for POST requests.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.request(method, url, headers=headers, params=params, json=data)

        # Check for successful response
        if not response.ok:
            response.raise_for_status()

        response_data = response.json()

        if method == 'POST' and data_type_in_hal_link:
            return response_data['_links'][data_type_in_hal_link]['href']
        return response_data

    def put_to_provider_api(self, url, access_token):
        return self.send_request('PUT', url, access_token)

    # TODO: have a generic delete and also a delete with params
    def delete_to_provider_api(self, url, access_token, delete_linked_entities=False):
        params = {'deleteLinkedEntities': str(delete_linked_entities).lower()}
        return self.send_request('DELETE', url, access_token, params=params)

    def post_to_provider_api(self, url, data_type_in_hal_link, data, access_token):
        return self.send_request('POST', url, access_token, data=data, data_type_in_hal_link=data_type_in_hal_link)
