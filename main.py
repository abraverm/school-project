__author__ = 'alexbmasis@gmail.com'

import logging
import configparser
import os.path
import certifi
from logging import debug, warn, error, info
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import exceptions
import names
from loremipsum import get_sentences
import  bugzilla
import  dateutil
import dateutil.parser
#import dateutil

def convert_iso_time (iso_date):
    return [dateutil.parser.parse(str(iso_date))]

def load_config():
    config = configparser.ConfigParser()
    if os.path.isfile('project.conf'):
        try:
            config.read('project.conf')
        except configparser.Error:
            error("Something went wrong when tried loading the config file")
    return config

def main():
    logging.basicConfig(level=logging.INFO)
    info("Starting project")
    info("Loading configuration")
    config = load_config()
    info("Initialize connection to Database")
    es = Elasticsearch(
        hosts=config.get('elasticsearch', 'hosts'),
        http_auth=(
            config.get('elasticsearch', 'user'),
            config.get('elasticsearch', 'password')
            ),
        port=config.get('elasticsearch', 'port'),
        use_ssl=config.getboolean('elasticsearch', 'use_ssl'),
        verify_certs=config.getboolean('elasticsearch', 'verify_certs'),
        ca_certs=certifi.where()
    )
    info("lets try something")
    """
    for bug_id in range(1,1000):
        #es.index(index="bugzilla", doc_type='bug', id=bug_id, body=bug)
        try:
            res = es.delete(index="bugzilla", doc_type='bug', id=bug_id)
            print("deleted index %s" % bug_id)
        except exceptions.NotFoundError:
            continue
    """
    bz = bugzilla.Bugzilla(url='https://bugzilla.redhat.com', user='', password='')
    query = {}
    query['bug_id'] = '1119115'
    for bug in bz.query(query):
        bug.refresh()
        bug_body = bug.__dict__
        bug_id = bug.__dict__['id']
        #bug_body.pop('last_change_time')
        #bug_body.pop('creation_time')
        bug_body.pop('bugzilla')
        #bug_body.pop('flags')
        bug_body.pop('comments')

        iso_last_change = bug.__dict__['last_change_time']
        bug_last_change = convert_iso_time(iso_last_change)
        iso_creation_time = bug.__dict__['creation_time']
        bug_creation_time = convert_iso_time(iso_creation_time)

        bug_body['last_change_time'] = bug_last_change
        bug_body['creation_time'] = bug_creation_time
        #print(bug_body['comments'])

        for field in bug_body:
            if hasattr(bug, field):
                print("%s: %s"% (field, getattr(bug, field)))
        es.index(index="bugzilla", doc_type='bug', id=bug_id, body=bug_body)

if __name__ == '__main__':
    main()