__author__ = 'alexbmasis@gmail.com'

from color_handler import ColorHandler
import logging
from configobj import ConfigObj, ConfigObjError
from validate import Validator
import os.path
import certifi
from logging import debug, warn, error, info
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import exceptions
import names
from loremipsum import get_sentences
import bugzilla
import dateutil
import dateutil.parser
#import dateutil


def convert_iso_time (iso_date):
    return [dateutil.parser.parse(str(iso_date))]


def init_bz(config):
    try:
        info("Initialize connection to Bugzilla")
        bz = bugzilla.Bugzilla(
            url=config['url'],
            user=config['user'],
            password=config['password']
        )
    except bugzilla.BugzillaError as e:
        error("Failed to initialize Bugzilla session:\n%s" % e.with_traceback())
        raise SystemExit(1)
    else:
        return bz

def init_es(config):
    try:
        info("Initialize connection to Elasticsearch")
        es = Elasticsearch(
                hosts=config['hosts'],
                http_auth=(
                    config['user'],
                    config['password']
                    ),
                port=config['port'],
                use_ssl=config['use_ssl'],
                verify_certs=config['verify_certs'],
                ca_certs=certifi.where()
            )
    except exceptions.ElasticsearchException as e:
        error("unable to connect to elastic search:\n%s" % e)
        raise SystemExit(1)
    else:
        return es

def load_config():
    if os.path.isfile('project.conf'):
        try:
            info("Loading configuration")
            config = ConfigObj('project.conf', configspec='default.conf')
            validator = Validator()
            config.validate(validator)
        except ConfigObjError as e:
            error("Something went wrong when tried loading the config file %s:\n%s" % ('project.conf', e.with_traceback()))
            raise SystemExit(1)
        else:
            return config

def main():
    logging.basicConfig(level=logging.DEBUG, handlers=[ColorHandler()])
    info("Starting project")
    config = load_config()
    es = init_es(config['elasticsearch'])
    bz = init_bz(config['bugzilla'])
    #TODO: bugs = pull_bugs(bz, config['bugzilla']['query'])
    query = {}
    query['bug_id'] = '1119115'
    for bug in bz.query(query):
        bug.refresh()
        bug_body = bug.__dict__
        bug_id = bug.__dict__['id']
        bug_body.pop('bugzilla')
        bug_body.pop('comments')
        bug_body.pop('flags')

        iso_last_change = bug.__dict__['last_change_time']
        bug_last_change = convert_iso_time(iso_last_change)
        iso_creation_time = bug.__dict__['creation_time']
        bug_creation_time = convert_iso_time(iso_creation_time)

        bug_body['last_change_time'] = bug_last_change
        bug_body['creation_time'] = bug_creation_time


        for field in bug_body:
            if hasattr(bug, field):
                print("%s: %s"% (field, getattr(bug, field)))
        es.index(index="bugzilla", doc_type='bug', id=bug_id, body=bug_body)

if __name__ == '__main__':
    main()