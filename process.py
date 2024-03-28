import logging

from src.system.execute import execute

endpoint = "opense-clust-wXvYElGUswuV-babfcfe00041edc9.elb.us-west-2.amazonaws.com"

(stdout, stderr, status) = execute(f'curl {endpoint}', ".")
logging.info(stdout)
logging.info(stderr)
logging.info(endpoint)



