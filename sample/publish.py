#!/usr/bin/env python


import os, sys, time
import esgf

submission_config = {
    'metadata': [
        {
            'name': 'name',
            'value': 'Test publication'
        },
        {
            'name': 'organization',
            'value': 'University of Chicago',
        },
        {
            'name': 'firstname',
            'value': 'Lukasz',
        },
        {
            'name': 'lastname',
            'value': 'Lacinski',
        },
        {
            'name': 'description',
            'value': 'Test submission - ESGF/ACME REST API',
        },
        {
            'name': 'datanode',
            'value': 'dev.esgf.anl.gov'
        }
    ],
    'facets': [
        {
            'name': 'project',
            'value': 'ACME'
        },
        {
            'name': 'data_type',
            'value': 'h0'
        },
        {
            'name': 'experiment',
            'value': 'b1850c5_m1a'
        },
        {
            'name': 'versionnum',
            'value': 'v0_1'
        },
        {
            'name': 'realm',
            'value': 'atm'
        },
        {
            'name': 'regridding',
            'value': 'ne30_g16'
        },
        {
            'name': 'range',
            'value': 'all'
        }
    ],
    'scan': {
        'options': '',
        'path': '/esg/gridftp_root/ingestion/test/acme/1'
    },
    'publish': {
        'options': {
            'files': 'all'
        },
        'files': []
    }
}


if __name__ == '__main__':

    # Establish a session with the publication service
    pwd = os.path.dirname(__file__)
    client = esgf.IngestionClient(config_file=os.path.join(pwd, 'user_client_config.yml'))
    if client is None:
        sys.exit('Error: Could not create a session with the publication service')
    print('Created a session with the publication service')


    # Create a new submission
    response, content = client.submit(submission_config)
    if response['status'] != '200':
        sys.exit('Error: HTTP Status %s: Could not start a new submission' % response['status'])
    if content['status'] != 'Success':
        sys.exit('Error: %s\n' % content['message'])
    submission_id = content['submission_id']
    print('New submission has been created successfully. Submission ID: %s' % submission_id)
    sys.exit(0)

    # Scan a directory with dataset files
    response, content = client.scan(submission_id, submission_config['scan'])
    if response['status'] != '200':
        sys.exit('Error: HTTP Status %s: Could not scan dataset files\n' % response['status'])
    if content['status'] == 'Error':
        sys.exit('Error: %s\n' % content['message'])

    while True:
        time.sleep(10)
        response, content = client.get_status(submission_id)
        if response['status'] != '200':
            sys.exit('Error: HTTP Status %s: Could not scan dataset files\n' % response['status'])
        if content['status'] == 'Error':
            sys.exit('Error: %s\n' % content['message'])
        elif content['status'] == 'Success':
            print('Files scanned: %s' % content['files'])
            break


    # Create a THREDDS catalog and publish to Solr
    client.publish(submission_id, submission_config['publish'])
    if response['status'] != '200':
        sys.exit('Error: HTTP Status %s: Could not publish dataset files\n' % response['status'])
    if content['status'] == 'Error':
        sys.exit('Error: %s\n' % content['message'])

    while True:
        time.sleep(10)
        response, content = client.get_status(submission_id)
        if response['status'] != '200':
            sys.exit('Error: HTTP Status %s: Could not publish dataset files\n' % response['status'])
        if content['status'] == 'Error':
            sys.exit('Error: %s\n' % content['message'])
        elif content['status'] == 'Success':
            print('Dataset files have been published')
            break
