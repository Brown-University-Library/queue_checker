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

import logging, pprint, subprocess

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S' )
log = logging.getLogger( '__name__' )


expectation = {
}

def run_code():
    """
    Controller.
    Called by `if __name__ == '__main__':`
    """
    output: str  = get_rqinfo()
    assert type(output) == str
    data_dct: dict = parse_rqinfo( output )
    return data_dct


def parse_rqinfo( rq_output: str ) -> dict:
    """ 
    Parses rqinfo output into a dict.

    Usage:
    >>> parse_rqinfo(
    ...     'queue q_1 0\\n'
    ...     'queue q_2 0\\n'
    ...     'queue failed 333\\n'
    ...     'q_1: server.968 (idle), server.952 (idle)\\n'
    ...     'q_2: server.952 (idle)\\n'
    ...     'failed: –\\n'
    ... )
    {'failed_count': 333, 'queues': ['q_1', 'q_2', 'failed'], 'workers_by_queue': {'q_1': ['server.968', 'server.952'], 'q_2': ['server.952'], 'failed': []}}
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
                                        #              failed: –
            ( queue_name, worker_data ) = line.split(':')
            worker_data = worker_data.strip()
            worker_names = []
            if worker_data != '–':      # Split by comma and get the server name from each part
                worker_names = [part.split()[0] for part in worker_data.split(',')]
            output['workers_by_queue'][queue_name] = worker_names
    log.debug( f'output, ``{pprint.pformat(output)}``' )
    return output



def get_rqinfo() -> str:
    """
    Runs `rqinfo`, returns output.
    - `--by-queue` returns the normal queue output, but shows workers associated with each queue.
    - `--raw` doesn't return the summary line or the job-bar, just the basic data.
    """
    result = subprocess.run(['rqinfo', '--by-queue', '--raw'], stdout=subprocess.PIPE)
    output = result.stdout.decode()
    assert type(output) == str
    log.debug( f'output, ``{output}``' )
    return output

# def run_code():
#     """
#     Controller.
#     Called by `if __name__ == '__main__':`
#     """
#     ( redis_check, queues, workers)  = get_rqinfo()
#     assert type(redis_check) == str
#     assert type(queues) == list
#     assert type(workers) == list
#     log.debug( f'redis check: ``{redis_check}``' )
#     log.debug( f'queues: ``{queues}``' )
#     log.debug( f'workers: ``{workers}``' )
#     return_tuple = (redis_check, queues, workers)
#     return return_tuple


# def get_rqinfo():
#     """
#     Runs `rqinfo`, parses the output, and builds lists of queues and workers.
#     """
#     ## Run the rqinfo command and get the output
#     result = subprocess.run(['rqinfo'], stdout=subprocess.PIPE)
#     output = result.stdout.decode()

#     ## Split the output into lines
#     lines = output.split('\n')

#     ## Initialize empty lists for queues and workers
#     redis_check = 'init'
#     queues = []
#     workers = []

#     # Iterate over each line in the output
#     for (i, line) in enumerate(lines):
#         ## check for redis-server connection error
#         if i == 0:
#             if '6379' in line and 'connection refused' in line.lower():  # `Error 111 connecting to localhost:6379. Connection refused.`
#                 redis_check = 'redis_down'
#                 break
#             else:
#                 redis_check = 'redis_ok'

#         if line.strip() == '':
#             continue

#         ## If line contains queue info, add it to the queues list
#         if '|' in line:
#             queue_name = line.split('|')[0].strip()
#             queues.append(queue_name)

#         ## If line contains worker info, add it to the workers list
#         if 'idle:' in line:
#             worker_name = line.split(' ')[0].strip()
#             workers.append(worker_name)

#     return redis_check, queues, workers




if __name__ == '__main__':
    run_code()
