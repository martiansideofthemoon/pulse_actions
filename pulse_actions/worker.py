import json
import logging
import os
import sys
import traceback

from argparse import ArgumentParser
from timeit import default_timer

from pulse_actions.authentication import (
    AuthenticationError,
    get_user_and_password,
)
from pulse_actions.handlers import config, route_functions

from mozci.mozci import disable_validations
from mozci.utils import transfer
from mozillapulse.config import PulseConfiguration
from mozillapulse.consumers import GenericConsumer
# This changes the behaviour of mozci in transfer.py
transfer.MEMORY_SAVING_MODE = True
transfer.SHOW_PROGRESS_BAR = False

LOG = None


class PulseConsumer(GenericConsumer):
    """
    Creates a consumer object for the given exchange.

    Documentation for the exchanges:
    https://wiki.mozilla.org/Auto-tools/Projects/Pulse/Exchanges
    """

    def __init__(self, exchange, **kwargs):
        super(PulseConsumer, self).__init__(
            PulseConfiguration(**kwargs), exchange, **kwargs)


def run_pulse(exchanges, topics, event_handler, topic_base, dry_run):
    """
    Listen to a pulse exchange in a infinite loop.

    Call event_handler on every message.
    """
    if len(topic_base) == 1:
        label = topic_base[0]
    else:
        label = str(topic_base)
    try:
        user, password = get_user_and_password()
    except AuthenticationError as e:
        print(e.message)
        sys.exit(1)

    pulse_args = {
        'applabel': label,
        'topic': topics,
        'durable': True,
        'user': user,
        'password': password
    }

    # Pulse consumer's callback passes only data and message arguments
    # to the function, we need to pass dry-run
    def handler_with_dry_run(data, message):
        start_time = default_timer()
        event_handler(data, message, dry_run)
        elapsed_time = default_timer() - start_time
        LOG.info('Message %s, took %s seconds to execute', data, str(elapsed_time))

    pulse = PulseConsumer(exchanges,
                          callback=handler_with_dry_run,
                          **pulse_args)
    LOG.info('Listening on %s, with topic %s', exchanges, topics)

    while True:
        try:
            pulse.listen()
        except KeyboardInterrupt:
            sys.exit(1)
        except:
            traceback.print_exc()


def setup_logging(logging_level):
    global LOG
    if LOG:
        return LOG
    # Let's use the root logger
    LOG = logging.getLogger()
    LOG.setLevel(logging.DEBUG)

    format = '%(asctime)s %(name)s\t %(levelname)s:\t %(message)s'
    formatter = logging.Formatter(format, datefmt='%H:%M:%S')
    # Handler 1 - Store all INFO messages in a specific file
    # This logging.txt file will *only* show messages for that same day as Heroku
    # dynos restart every day - Use papertrail for more details.
    fh = logging.FileHandler('logging.txt')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    LOG.addHandler(fh)

    # Handler 2 - Store all DEBUG messages in a specific file
    fh = logging.FileHandler('logging_debug.txt')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    LOG.addHandler(fh)

    # Handler 3 - Output to console (this is the output for Papertrail)
    console = logging.StreamHandler()
    console.setLevel(logging_level)
    formatter = logging.Formatter('%(name)s\t %(levelname)s:\t %(message)s')
    console.setFormatter(formatter)
    LOG.addHandler(console)

    LOG.info("Console output logs %s level messages." % logging.getLevelName(logging_level))

    # Reduce logging for other noisy modules
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("amqp").setLevel(logging.WARNING)


def load_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'run_time_config.json')
    with open(config_path, 'r') as config_file:
        options = json.load(config_file)

    return options


def run_exchange_topic(topic_base, dry_run):
    options = load_config()
    topic_base = topic_base.split(",")
    exchanges = []
    topics = []

    for topic in topic_base:
        exchange = options[topic]['exchange']
        exchanges.append(exchange)
        topics.append(options[topic]['topic'])

    # Finding the right event handler for the given exchange and topic
    try:
        if len(topic_base) == 1:
            handler_data = config.HANDLERS_BY_EXCHANGE[exchanges[0]]
            handler_function = handler_data[topic_base[0]]
        else:
            handler_function = route_functions.route
    except KeyError:
        LOG.error("We don't have an event handler for %s with topic %s.",
                  exchanges, topics)
        exit(1)

    run_pulse(
        exchanges=exchanges,
        topics=topics,
        event_handler=handler_function,
        topic_base=topic_base,
        dry_run=dry_run)


def parse_args(argv=None):
    parser = ArgumentParser()
    parser.add_argument("--topic-base",
                        required=True,
                        dest="topic_base",
                        type=str,
                        help="Identifier for exchange and topic to be listened to.")

    parser.add_argument("--dry-run",
                        action="store_true",
                        dest="dry_run",
                        help="flag to test without actual push.")

    parser.add_argument("--debug",
                        action="store_true",
                        dest="debug",
                        help="set debug for logging.")

    options = parser.parse_args(argv)
    return options


def main():
    options = parse_args()

    if options.debug:
        setup_logging(logging.DEBUG)
    else:
        setup_logging(logging.INFO)

    # Disable mozci's validations
    disable_validations()

    run_exchange_topic(options.topic_base, options.dry_run)


if __name__ == '__main__':
    main()
