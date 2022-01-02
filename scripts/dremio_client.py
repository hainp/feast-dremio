import argparse

def parse_arguments():
    """
    Parses the command-line arguments supplied to the script.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-host', '--hostname', type=str, help='Dremio co-ordinator hostname',
      default='localhost')
    parser.add_argument('-port', '--flightport', type=str, help='Dremio flight server port',
      default='32010')
    parser.add_argument('-user', '--username', type=str, help='Dremio username',
      required=True)
    parser.add_argument('-pass', '--password', type=str, help='Dremio password',
      required=True)
    parser.add_argument('-query', '--sqlquery', type=str, help='SQL query to test',
      required=False)
    parser.add_argument('-tls', '--tls', dest='tls', help='Enable encrypted connection',
      required=False, default=False, action='store_true')
    parser.add_argument('-disableServerVerification', '--disableServerVerification', dest='disableServerVerification',
                        help='Disable TLS server verification',
                        required=False, default=False)
    parser.add_argument('-certs', '--trustedCertificates', type=str,
      help='Path to trusted certificates for encrypted connection', required=False)
    return parser.parse_args()

if __name__ == "__main__":
    from feast_dremio.client import Client

    # Parse the command line arguments.
    args = parse_arguments()
    # Connect to Dremio Arrow Flight server endpoint.
    client = Client(args.hostname, args.flightport, args.username, args.password, args.tls, args.trustedCertificates, args.disableServerVerification)
    client.connect()
    reader = client.execute_query(query=args.sqlquery)
    print(reader.read_pandas(timestamp_as_object=True))
