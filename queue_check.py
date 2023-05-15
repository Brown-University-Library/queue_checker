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

import datetime, json, logging, os, pprint, smtplib, socket, subprocess
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
    previous_rqinfo_data = load_previous_rqinfo_data()
    assert type(previous_rqinfo_data) == dict
    ## run `rqinfo` -------------------------------------------------
    output  = get_rqinfo()
    assert type(output) == str
    ## parse `rqinfo` output ----------------------------------------
    data_dct = parse_rqinfo( output )
    assert type(data_dct) == dict
    save_rqinfo_data( data_dct )
    ## evaluate `rqinfo` output -------------------------------------
    last_failed_count = previous_rqinfo_data['failed_count']
    evaluation_dct = evaluate_qdata( last_failed_count, expectations, data_dct )
    assert type(evaluation_dct) == dict
    if evaluation_dct == {'queue_check': 'ok', 'worker_check': 'ok', 'failure_queue_check': 'ok'}:
        pass
    ## send email if necessary ---------------------------------------
    else:
        previous_failure_count = previous_rqinfo_data['failed_count']
        msg: str = build_email_message( previous_failure_count, expectations, evaluation_dct, data_dct )       
        send_email( message=msg )
    log.info( f'evaluation_dct, ``{pprint.pformat(evaluation_dct)}``' )
    return 


## helper functions called by run_code() ----------------------------


def load_previous_rqinfo_data():
    """
    Loads previous rqinfo data from file."""
    with open( '../previous_rqinfo_data/previous_rqinfo_data.json', 'r' ) as f:
        previous_rqinfo_data = json.loads( f.read() )
    assert type(previous_rqinfo_data) == dict
    log.debug( f' previous_rqinfo_data, ``{pprint.pformat(previous_rqinfo_data)}``' )
    return previous_rqinfo_data


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


def save_rqinfo_data( data_dct ):
    """ Saves rqinfo data to file.
        Called by run_code() """
    assert type(data_dct) == dict
    jsn = json.dumps( data_dct, sort_keys=True, indent=2 )
    ## assume unicorns exist ------------------------------------------
    file_path = '../previous_rqinfo_data/previous_rqinfo_data.json'
    try:
        with open( file_path, 'w' ) as f:
            f.write( jsn )
    ## only acknowledge unhappiness if necessary ----------------------
    except FileNotFoundError:
        os.makedirs( os.path.dirname(file_path), exist_ok=True )
        with open( file_path, 'w' ) as f:
            f.write( jsn )
    except Exception as e:
        log.exception( 'problem saving rqinfo data; traceback follows' )
        raise Exception( f'problem saving rqinfo data; error, ``{repr(e)}``' )
    log.debug( 'rqinfo data saved' )
    return


def evaluate_qdata( previous_failed_count, expectations, data_dct ):
    """ 
    Evaluates rqinfo output against expectation-data.
    Called by run_code()     

    Example -- all ok:
    >>> previous_failed_count = 10
    >>> expectations_data = {'expected_queues': ['q1', 'q2'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}], 'surge_failure_limit': 10}
    >>> rqinfo_data = {'failed_count': 15, 'queues': ['q1', 'q2', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'q2': ['server.234'], 'failed': []}}
    >>> result = evaluate_qdata( previous_failed_count, expectations_data, rqinfo_data )
    >>> result
    {'queue_check': 'ok', 'worker_check': 'ok', 'failure_queue_check': 'ok'}
    
    Example -- problem:
    >>> previous_failed_count = 10
    >>> expectations_data = {'expected_queues': ['q1', 'q2', 'q3'], 'expected_workers': [{'queue': 'q1', 'worker_count': 1}, {'queue': 'q2', 'worker_count': 1}], 'surge_failure_limit': 10}
    >>> rqinfo_data = {'failed_count': 30, 'queues': ['q1', 'failed'], 'workers_by_queue': {'q1': ['server.123'], 'failed': []}}
    >>> result = evaluate_qdata( previous_failed_count, expectations_data, rqinfo_data )
    >>> result
    {'queue_check': 'FAIL', 'worker_check': 'FAIL', 'failure_queue_check': 'FAIL'}
    """
    assert type( previous_failed_count ) == int
    assert type( expectations ) == dict
    assert type( data_dct ) == dict
    checks_result = {'queue_check': 'init', 'worker_check': 'init', 'failure_queue_check': 'init'}
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
    log.debug( f'after worker-check, checks_result, ``{checks_result}``' )
    ## failure-count check ------------------------------------------
    failure_increase = data_dct['failed_count'] - previous_failed_count
    log.debug( f'failure_increase, ``{failure_increase}``' )
    surge_failure_limit = expectations['surge_failure_limit']
    log.debug( f'surge_failure_limit, ``{surge_failure_limit}``' )
    if failure_increase > surge_failure_limit:
        log.debug( f'failure-increase exceeded expectation-settings-limit')
        checks_result['failure_queue_check'] = 'FAIL'
    else:
        checks_result['failure_queue_check'] = 'ok'
    log.debug( f'checks_result, ``{checks_result}``' )
    return checks_result
    # end def evaluate_qdata()


def build_email_message( previous_failure_count, expectations_dct, evaluation_dct, data_dct ):
    """ Assembles email message.
        Called by run_code() """
    assert type(evaluation_dct) == dict
    assert type(data_dct) == dict
    msg = f'''
TIME-STAMP ----------------------------------------------------------
{datetime.datetime.now()}
    
CHECK-RESULT --------------------------------------------------------
{repr(evaluation_dct)}

EXPECTATIONS SETTINGS -----------------------------------------------
{pprint.pformat(expectations_dct)}

ACTUAL RQINFO-DATA -------------------------------------------------- 
{pprint.pformat(data_dct)}

PREVIOUS RQINFO-DATA FAILURE-COUNT ----------------------------------
{previous_failure_count}

[END]

'''
    log.debug( f'msg, ``{msg}``' )
    return msg


def send_email( message ):
    """ Sends mail; generates exception which cron-job should email to crontab owner on sendmail failure.
        Called by run_code() """
    assert type(message) == str, type(message)
    log.debug( f'message, ``{message}``' )
    EMAIL_HOST = os.environ['QCHKR__EMAIL_HOST']
    EMAIL_PORT = int( os.environ['QCHKR__EMAIL_HOST_PORT'] )  
    # EMAIL_FROM = os.environ['QCHKR__EMAIL_FROM']
    EMAIL_FROM = 'donotreply__rq_queue_checker@brown.edu'
    EMAIL_RECIPIENTS = json.loads( os.environ['QCHKR__EMAIL_RECIPIENTS_JSON'] )
    HOST = socket.gethostname()
    try:
        s = smtplib.SMTP( EMAIL_HOST, EMAIL_PORT )
        body = message
        eml = MIMEText( f'{body}' )
        eml['Subject'] = f'queue-checker alert from ``{HOST.upper()}``'
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
