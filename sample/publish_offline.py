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
            'value': 'esg.ccs.ornl.gov'
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
        'map': [
            {
                'dataset_id': 'cordex.output.AFR-44.CLMcom.MOHC-HadGEM2-ES.rcp85.r1i1p1.CCLM4-8-17.v1.day.mrso',
                'path': '/esg/data/cordex/output/AFR-44/CLMcom/MOHC-HadGEM2-ES/rcp85/r1i1p1/CCLM4-8-17/v1/day/mrso/mrso_AFR-44_MOHC-HadGEM2-ES_rcp85_r1i1p1_CLMcom-CCLM4-8-17_v1_day_20810101-20851230.nc',
                'size': 108136338,
                'mod_time': 1468387129.000000,
                'checksum': '2a16d56c760854c0361a6ce6e143759c02aa5b6837750425a099cbfb5e4d7382',
                'checksum_type': 'SHA256'
            },
            {
                'dataset_id': 'cordex.output.AFR-44.CLMcom.MOHC-HadGEM2-ES.rcp85.r1i1p1.CCLM4-8-17.v1.day.mrso',
                'path': '/esg/data/cordex/output/AFR-44/CLMcom/MOHC-HadGEM2-ES/rcp85/r1i1p1/CCLM4-8-17/v1/day/mrso/mrso_AFR-44_MOHC-HadGEM2-ES_rcp85_r1i1p1_CLMcom-CCLM4-8-17_v1_day_20460101-20501230.nc',
                'size': 108136338,
                'mod_time': 1468387005.000000,
                'checksum': '7ede9ded61dddcb402ac31371f4efc9bf62318f700e63458269c344ab7a2de25',
                'checksum_type': 'SHA256'
            }
        ]
    },
    'publish': {
        'options': {
            'files': 'all',
            'offline': 1
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
