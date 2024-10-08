import docker
import logging


def lambda_handler(event, context):
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        client = docker.from_env()
        container=client.containers.get("containerId")
        container.start()
    except Exception as e:
        logger.error(e)
    return