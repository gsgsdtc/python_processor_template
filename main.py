import argparse
import logging
import logging.config
from os import path

env="debug";

def main(args):
    
    env = args.env

    log_file_path = path.join(path.dirname(path.abspath(__file__)), 'conf/log-{}.conf'.format(env))
    logging.config.fileConfig(log_file_path)
    logger = logging.getLogger("main")
    try:
        logger.info("Processor start")
        
        logger.info("Processor end")

    except:
        logger.exception("Processor error")


def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='example code to play with InfluxDB')
    parser.add_argument('--env', type=str, required=False,
                        default='debug',
                        help='hostname of InfluxDB http API')

    return parser.parse_args()



if __name__ == '__main__':
    args = parse_args()
    main(args)