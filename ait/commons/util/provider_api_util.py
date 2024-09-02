import requests


class APIProvider:
    def __init__(self, base_url):
        self.base_url = base_url

    def request(self, method, url, access_token, params=None, data=None, data_type_in_hal_link=None):
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

        Behavior

        Headers Setup: Constructs the request headers, setting Content-Type to application/json and adding the Authorization header with the provided access_token.

        Sending Request: Sends the HTTP request using the requests.request method with the specified method, URL, headers, params, and data.

        Response Handling:
            Checks the status code of the response.

            If the status code is not one of 200, 201, 202, or 204, prints an error message and:
                For DELETE requests, returns None.
                For other methods, raises an exception using response.raise_for_status().

            For POST requests with a data_type_in_hal_link provided,
            returns the URL from the _links section of the response.

            For DELETE requests, returns the status code.

            For other successful requests, returns the JSON-parsed response data.
        """
        # Construct the request headers
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        # Send the HTTP request
        response = requests.request(method, url, headers=headers, params=params, json=data)
        status_code = response.status_code

        # Check for unsuccessful status codes
        if status_code not in (200, 201, 202, 204):
            print(f"Received {status_code} while executing {method} on {url}")

            if method == 'DELETE':
                # Return None for unsuccessful DELETE requests
                return None
            else:
                # Raise an exception for other unsuccessful requests
                raise response.raise_for_status()
        else:
            print(f"Received {status_code} while executing {method} on {url}")
        # Handle POST requests with data_type_in_hal_link
        if method == 'POST' and data_type_in_hal_link:
            response_data = response.json()
            # Return the URL from the HAL link in the response
            return response_data['_links'][data_type_in_hal_link]['href']
        elif method == 'DELETE':
            # Return the status code for DELETE requests
            return status_code
        else:
            # Return the JSON-parsed response data for other successful requests
            return response.json()

    def put(self, url, access_token):
        return self.request('PUT', url, access_token)

    def get(self, url, access_token):
        return self.request('GET', url, access_token)

    def delete_with_relations(self, url, access_token, delete_linked_entities=False):
        params = {'deleteLinkedEntities': str(delete_linked_entities).lower()}
        return self.request('DELETE', url, access_token, params=params)

    def delete(self, url, access_token):
        return self.request('DELETE', url, access_token)

    def post(self, url, data_type_in_hal_link, data, access_token):
        return self.request('POST', url, access_token, data=data, data_type_in_hal_link=data_type_in_hal_link)
