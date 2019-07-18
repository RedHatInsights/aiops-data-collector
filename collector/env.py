import os

TOPOLOGICAL_INTERNAL_PATH = os.environ.get('TOPOLOGICAL_INTERNAL_PATH')
ALL_TENANTS = os.environ.get('ALL_TENANTS', 'true').lower() in ['true', 'y']
TOPOLOGICAL_INVENTORY_HOST = os.environ.get('TOPOLOGICAL_INVENTORY_HOST')
TOPOLOGICAL_INVENTORY_PATH = os.environ.get('TOPOLOGICAL_INVENTORY_PATH')
HOST_INVENTORY_HOST = os.environ.get('HOST_INVENTORY_HOST')
HOST_INVENTORY_PATH = os.environ.get('HOST_INVENTORY_PATH')
SOURCES_HOST = os.environ.get('SOURCES_HOST')
SOURCES_PATH = os.environ.get('SOURCES_PATH')
INPUT_DATA_FORMAT = os.environ.get('INPUT_DATA_FORMAT', '').upper()
APP_NAME = os.environ.get('APP_NAME')
SSL_VERIFY = os.environ.get('SSL_VERIFY', 'true').lower() in ['true', 'y']
REDIS_ENV = os.environ.get('REDIS_ENV', '{"host": "redis"}')
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', 'smartvm')
PROCESS_WINDOW = int(os.environ.get('PROCESS_WINDOW', 60*60*24))
