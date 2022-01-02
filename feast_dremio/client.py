import logging
import sys

from pyarrow import flight

from feast_dremio.middleware import DremioClientAuthMiddlewareFactory


class Client():
    def __init__(self, hostname, flightport, username, password, tls, certs, disableServerVerification):
        try:
            # Default to use an unencrypted TCP connection.
            scheme = "grpc+tcp"
            connection_args = {}

            if tls:
                # Connect to the server endpoint with an encrypted TLS connection.
                logging.info('Enabling TLS connection')
                scheme = "grpc+tls"
                if certs:
                    logging.info('Trusted certificates provide')
                    # TLS certificates are provided in a list of connection arguments.
                    with open(certs, "rb") as root_certs:
                        connection_args["tls_root_certs"] = root_certs.read()
                elif disableServerVerification:
                    # Connect to the server endpoint with server verification disabled.
                    logging.info('Disable TLS server verification.')
                    connection_args['disable_server_verification'] = disableServerVerification
                else:
                    logging.error('Trusted certificates must be provided to establish a TLS connection')
                    sys.exit()
            client_auth_middleware = DremioClientAuthMiddlewareFactory()
            self._client = flight.FlightClient("{}://{}:{}".format(scheme, hostname, flightport),
                                         middleware=[client_auth_middleware], **connection_args)
            self._username = username
            self._password = password

        except Exception as exception:
            logging.error("Exception: {}".format(repr(exception)))
            raise


    def client(self):
        return self._client


    def connect(self, options=flight.FlightCallOptions(headers=[])):
        self._bearer_token = self.client().authenticate_basic_token(self._username, self._password, options)
        logging.info('Authentication was successful')


    def execute_query(self, query="", options=flight.FlightCallOptions(headers=[])):
        flight_desc = flight.FlightDescriptor.for_command(query)
        logging.info(f"Query: {query}")

        options = flight.FlightCallOptions(headers=[self._bearer_token])
        schema = self.client().get_schema(flight_desc, options)
        logging.info('GetSchema was successful')
        logging.info(f"Schema: {schema}")

        flight_info = self.client().get_flight_info(flight.FlightDescriptor.for_command(query), options)
        logging.info('GetFlightInfo was successful')
        logging.info(f"Ticket: {flight_info.endpoints[0].ticket}")

        # Retrieve the result set as a stream of Arrow record batches.
        reader = self.client().do_get(flight_info.endpoints[0].ticket, options)
        logging.info('Reading query results from Dremio')

        return reader
