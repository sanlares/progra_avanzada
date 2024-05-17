import requests

# URL base de tu servicio
base_url = 'https://kz3ufctqvi.us-east-2.awsapprunner.com'


# Ejemplo de una solicitud GET
response = requests.get(f'{base_url}/stats/')
print(response.json())

# Ejemplo de una solicitud POST con datos en formato JSON
#data = {'key1': 'value1', 'key2': 'value2'}
#response = requests.post(f'{base_url}/path/to/your/endpoint', json=data)
#print(response.json())
