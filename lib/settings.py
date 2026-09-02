"""
Loads queue-checker settings and configures application logging.
"""

import json
import logging
import os
import pprint
from pathlib import Path

from dotenv import load_dotenv

DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / '.env'
load_dotenv(dotenv_path=DOTENV_PATH, override=True)

ENV_LOG_LEVEL = os.environ.get('QCHKR__LOG_LEVEL', 'INFO')
LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
}
logging.basicConfig(
    level=LEVELS[ENV_LOG_LEVEL],
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S',
)
log = logging.getLogger(__name__)

expectations: dict = json.loads(os.environ.get('QCHKR__EXPECTATIONS_JSON', '{}'))
log.debug(f'expectations, ``{pprint.pformat(expectations)}``')
