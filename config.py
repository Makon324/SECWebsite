import toml
import boto3
from botocore.exceptions import ClientError
import json


class Config:
    def __init__(self):
        self.user_agent = None
        self.num_workers = None
        self.queue_max_size = None
        self.batch_size = None
        self.pool_min_size = None
        self.pool_max_size = None
        self.database_url = None
        self.max_requests_sec = None
        self.to_wait_on_rate_limit = None

        self.load_from_toml("config.toml")
        self.load_db_url()

    def load_from_toml(self, file_path):
        """
        Loads configuration variables from a .toml file.

        Args:
            file_path (str): Path to the .toml configuration file.

        The .toml file should contain the following keys:
        user_agent = "Makon324/test/makon324@yahoo.com"
        num_workers = 30
        queue_max_size = 1000
        batch_size = 500
        pool_min_size = 20
        pool_max_size = 50

        Example usage:
            config = Config()
            config.load_from_toml('config.toml')
            print(config.user_agent)
        """
        try:
            with open(file_path, 'r') as f:
                data = toml.load(f)

            self.user_agent = data.get('user_agent')
            self.num_workers = data.get('num_workers', 30)
            self.queue_max_size = data.get('queue_max_size', 1000)
            self.batch_size = data.get('batch_size', 500)
            self.pool_min_size = data.get('pool_min_size', 20)
            self.pool_max_size = data.get('pool_max_size', 50)
            self.max_requests_sec = data.get('max_requests_sec', 10)
            self.to_wait_on_rate_limit = data.get('to_wait_on_rate_limit', 10*60)

        except FileNotFoundError:
            raise ValueError(f"Config file not found: {file_path}")
        except toml.TomlDecodeError:
            raise ValueError(f"Invalid TOML format in file: {file_path}")
        except Exception as e:
            raise ValueError(f"Error loading config: {str(e)}")

    def load_db_url(self):
        """
        Constructs a database URL from the given configuration dictionary.

        :param db_config: A dictionary containing database connection details.
                          Expected keys: 'username', 'password', 'engine', 'host', 'port', 'dbInstanceIdentifier'
        :return: A string representing the database URL.
        """
        db_config = json.loads(self.get_secret("filingsdatabase_secrets"))

        engine = db_config.get('engine', 'postgresql')  # Default to 'postgresql' for standard compatibility
        username = db_config['username']
        password = db_config['password']
        host = db_config['host']
        port = db_config['port']
        db_name = db_config['dbInstanceIdentifier']

        # Note: If engine is 'postgres', we map it to 'postgresql' for URL standard
        if engine == 'postgres':
            engine = 'postgresql'

        self.database_url = f"{engine}://{username}:{password}@{host}:{port}/{db_name}"

    def get_secret(self, secret_name: str, region_name: str = "eu-north-1"):
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        return get_secret_value_response['SecretString']