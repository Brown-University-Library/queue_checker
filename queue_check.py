"""
This code checks:
- that expected queues exist
- that expected workers are running.
- that the failed-queue count is as expected.

Usage:
% cd /path/to/queue_checker/
% source ../env/bin/activate                # for access to rqinfo
% source ../venv_settings/env_settings.sh   # for access to settings
% python ./queue_check.py

Tests can be run via substituting for the above line:
% python -m doctest ./queue_check.py
(which will show no output if all tests pass) ...or...
% python -m doctest -v ./queue_check.py
"""

import datetime, json, logging, os, pprint, smtplib, subprocess
from email.mime.text import MIMEText


ENV_LOG_LEVEL = os.environ['QCHKR__LOG_LEVEL']
level_dct = { 'DEBUG': logging.DEBUG, 'INFO': logging.INFO, }
logging.basicConfig(  # no file-logging for now
    level=level_dct[ENV_LOG_LEVEL],
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
log = logging.getLogger( '__name__' )


expectations: dict = json.loads( os.environ['QCHKR__EXPECTATIONS_JSON'] )
log.debug( f'expectations, ``{pprint.pformat(expectations)}``' )    


## main controller --------------------------------------------------


def run_code():
    """
    Controller.
    Called by dunder-main.
    """
    output  = get_rqinfo()                                          ## run `rqinfo`
    assert type(output) == str
    data_dct = parse_rqinfo( output )                               ## parse `rqinfo` output
    assert type(data_dct) == dict
    evaluation_dct = evaluate_qdata( expectations, data_dct )       ## evaluate `rqinfo` output
    if evaluation_dct == {'queue_check': 'ok', 'worker_check': 'ok', 'failure_queue_check': 'ok'}:
        pass
    else:                                                           ## send email if necessary
        msg: str = build_email_message( expectations, evaluation_dct, data_dct )       
        send_email( message=msg )                                   
    return 


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
    # end def parse_rqinfo()


def evaluate_qdata( expectations, data_dct ):
    """ 
    Evaluates rqinfo output against expectation-data.
    Called by run_code() 
        
    Example -- all ok:
    >>> expectations_data = {'expected_queues': ['q1', 'q2'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}], 'permitted_failures': 0}
    >>> rqinfo_data = {'failed_count': 0, 'queues': ['q1', 'q2', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'q2': ['server.234'], 'failed': []}}
    >>> result = evaluate_qdata( expectations_data, rqinfo_data )
    >>> result
    {'queue_check': 'ok', 'worker_check': 'ok', 'failure_queue_check': 'ok'}
    
    Example -- problem:
    >>> expectations_data = {'expected_queues': ['q1', 'q2', 'q3'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}, {'queue': 'q2', 'worker_count': 1}], 'permitted_failures': 0}
    >>> rqinfo_data = {'failed_count': 1, 'queues': ['q1', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'failed': []}}
    >>> result = evaluate_qdata( expectations_data, rqinfo_data )
    >>> result
    {'queue_check': 'FAIL', 'worker_check': 'FAIL', 'failure_queue_check': 'FAIL'}
    """
    checks_result = {'queue_check': 'init', 'worker_check': 'init', 'failure_queue_check': 'init'}
    log.debug( f'starting checks_result, ``{checks_result}``' )
    ## queue check --------------------------------------------------
    queue_check_flag = 'init'
    for queue in expectations['expected_queues']:
        if queue not in data_dct['queues']:
            log.debug( f'queue, ``{queue}``, not found in queue-check' )
            checks_result['queue_check'] = 'FAIL'
            queue_check_flag = 'fail'
            break
    if queue_check_flag == 'init':
        checks_result['queue_check'] = 'ok'
    log.debug( f'after queue-check, checks_result, ``{checks_result}``' )
    ## worker check --------------------------------------------------
    worker_check_flag = 'init'
    for worker_dct in expectations['expected_workers']:
        log.debug( f'checking worker_dct, ``{worker_dct}``')
        queue = worker_dct['queue']
        worker_count = worker_dct['worker_count']
        if queue not in data_dct['workers_by_queue']:
            log.debug( f'queue, ``{queue}``, not found in worker-check' )
            checks_result['worker_check'] = 'FAIL'
            worker_check_flag = 'fail'
            break
        if len( data_dct['workers_by_queue'][queue] ) != worker_count:
            log.debug( f'queue, ``{queue}``, has wrong number of workers' )
            checks_result['worker_check'] = 'FAIL'
            worker_check_flag = 'fail'
            break
    if worker_check_flag == 'init':
        checks_result['worker_check'] = 'ok'
    ## failure-count check ------------------------------------------
    if data_dct['failed_count'] > expectations['permitted_failures']:
        checks_result['failure_queue_check'] = 'FAIL'
    else:
        checks_result['failure_queue_check'] = 'ok'
    log.debug( f'checks_result, ``{checks_result}``' )
    return checks_result
    # end def evaluate_qdata()


def build_email_message( expectations_dct, evaluation_dct, data_dct ):
    """ Assembles email message.
        Called by run_code() """
    assert type(evaluation_dct) == dict
    assert type(data_dct) == dict
    msg = f'''
time-stamp: 
``{datetime.datetime.now()}``
    
check-result: 
``{repr(evaluation_dct)}``

expectations settings:
``{pprint.pformat(expectations_dct)}``

actual rqinfo-data: 
``{pprint.pformat(data_dct)}``
````'''
    log.debug( f'msg, ``{msg}``' )
    return msg


def send_email( message ):
    """ Sends mail; generates exception which cron-job should email to crontab owner on sendmail failure.
        Called by run_code() """
    assert type(message) == str, type(message)
    log.debug( f'message, ``{message}``' )
    EMAIL_HOST = os.environ['QCHKR__EMAIL_HOST']
    EMAIL_PORT = int( os.environ['QCHKR__EMAIL_HOST_PORT'] )  
    EMAIL_FROM = os.environ['QCHKR__EMAIL_FROM']
    EMAIL_RECIPIENTS = json.loads( os.environ['QCHKR__EMAIL_RECIPIENTS_JSON'] )
    try:
        s = smtplib.SMTP( EMAIL_HOST, EMAIL_PORT )
        body = message
        eml = MIMEText( f'{body}' )
        eml['Subject'] = 'QUEUE-CHECKER ALERT on ``{EMAIL_HOST}``'
        eml['From'] = EMAIL_FROM
        eml['To'] = ';'.join( EMAIL_RECIPIENTS )
        s.sendmail( EMAIL_FROM, EMAIL_RECIPIENTS, eml.as_string())
    except Exception as e:
        err = repr( e )
        log.exception( f'Problem sending queue-checker mail, ``{err}``' )
        raise Exception( err )
    return


## dunder-main ------------------------------------------------------

if __name__ == '__main__':
    run_code()
