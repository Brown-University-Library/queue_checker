"""
This code checks:
- that redis is running.
- that expected queues exist
- that expected workers are running.

Usage:
% cd /path/to/queue_checker/
% source ../env/bin/activate                # for access to rqinfo
% source ../venv_settings/env_settings.sh   # for access to settings
% python ./queue_check.py
"""

import datetime, json, logging, os, pprint, smtplib, subprocess
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# from email.utils import formataddr


logging.basicConfig(  # no file-logging for now
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
log = logging.getLogger( '__name__' )


expectations = json.loads( os.environ['QCHKR__EXPECTATIONS_JSON'] )
log.debug( f'expectations, ``{pprint.pformat(expectations)}``' )    


## main controller --------------------------------------------------


def run_code():
    """
    Controller.
    Called by dunder-main.
    """
    output  = get_rqinfo()                                      ## run `rqinfo`
    assert type(output) == str
    data_dct = parse_rqinfo( output )                           ## parse `rqinfo` output
    assert type(data_dct) == dict
    evaluation_dct = evaluate_qdata( expectations, data_dct )   ## evaluate `rqinfo` output
    send_email( message=repr(data_dct) )                        ## send email if necessary
    return data_dct                                             ## return data-dct for testing


## helper functions called by run_code() ----------------------------


def get_rqinfo() -> str:
    """ Runs `rqinfo`, returns output.
        - `--by-queue` returns the normal queue output, but shows workers associated with each queue.
        - `--raw` doesn't return the summary line or the job-bar, just the basic data. 
        Called by run_code() """
    result = subprocess.run(['rqinfo', '--by-queue', '--raw'], stdout=subprocess.PIPE)
    output = result.stdout.decode()
    assert type(output) == str
    log.debug( f'output, ``{output}``' )
    return output


def parse_rqinfo( rq_output ):
    """ 
    Parses rqinfo output into a dict.
    Called by run_code().
    Doctest usage (w/env sourced): `% python -m doctest ./queue_check.py`

    Example:
    >>> result = parse_rqinfo(
    ...     'queue q_1 0\\n'
    ...     'queue q_2 0\\n'
    ...     'queue failed 333\\n'
    ...     'q_1: server.968 (idle), server.952 (idle)\\n'
    ...     'q_2: server.952 (idle)\\n'
    ...     'failed: –\\n'
    ... )
    >>> result
    {'failed_count': 333, 'queues': ['q_1', 'q_2', 'failed'], 'workers_by_queue': {'q_1': ['server.968', 'server.952'], 'q_2': ['server.952'], 'failed': []}}
    >>> pprint.pprint( result )
    {'failed_count': 333,
     'queues': ['q_1', 'q_2', 'failed'],
     'workers_by_queue': {'failed': [],
                          'q_1': ['server.968', 'server.952'],
                          'q_2': ['server.952']}}
    """
    lines = rq_output.split('\n')
    log.debug( f'lines, ``{lines}``' )
    output = {'failed_count': 0, 'queues': [], 'workers_by_queue': {}}
    for line in lines:
        log.debug( f'processing line, ``{line}``' )
        line = line.strip()
        if line == '':
            log.debug( 'blank line; continuing' )
            continue
        if line.startswith('queue'):    # Line format: queue <queue_name> <count>
            ( _, queue_name, count ) = line.split()
            output['queues'].append(queue_name)
            if queue_name == 'failed':
                output['failed_count'] = int(count)
        else:                           # Line format: <queue_name>: <worker.123 (idle), worker.124 (idle)> ...or...
                                        #                    failed: –
            ( queue_name, worker_data ) = line.split(':')
            worker_data = worker_data.strip()
            worker_names = []
            if worker_data != '–':      # Split by comma and get the worker name from each part
                worker_names = [part.split()[0] for part in worker_data.split(',')]
            output['workers_by_queue'][queue_name] = worker_names
    log.debug( f'output, ``{pprint.pformat(output)}``' )
    return output


def send_email( message ):
    """ Sends mail; generates exception which cron-job should email to crontab owner on sendmail failure.
        Called by run_code() """
    assert type(message) == str
    log.debug( f'message, ``{message}``' )
    EMAIL_HOST = os.environ['QCHKR__EMAIL_HOST']
    EMAIL_PORT = int( os.environ['QCHKR__EMAIL_HOST_PORT'] )  
    EMAIL_FROM = os.environ['QCHKR__EMAIL_FROM']
    # EMAIL_RECIPIENTS = os.environ['QCHKR__EMAIL_RECIPIENTS_JSON'].split( ';' )
    EMAIL_RECIPIENTS = json.loads( os.environ['QCHKR__EMAIL_RECIPIENTS_JSON'] )
    try:
        s = smtplib.SMTP( EMAIL_HOST, EMAIL_PORT )
        body = f'datetime: `{str(datetime.datetime.now())}`\n\nSome intro...\n\n{message}\n\n[END]'
        eml = MIMEText( f'{body}' )
        eml['Subject'] = 'error found in parse-alma-exports logfile'
        eml['From'] = EMAIL_FROM
        eml['To'] = ';'.join( EMAIL_RECIPIENTS )
        s.sendmail( EMAIL_FROM, EMAIL_RECIPIENTS, eml.as_string())
    except Exception as e:
        err = repr( e )
        log.exception( f'Problem sending queue-checker mail, ``{err}``' )
    return


## dunder-main ------------------------------------------------------

if __name__ == '__main__':
    run_code()
