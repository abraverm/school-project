__author__ = 'alexbmasis@gmail.com'

from color_handler import ColorHandler
import logging
from configobj import ConfigObj, ConfigObjError
from validate import Validator
import os.path
import certifi
from logging import debug, warn, error, info
from elasticsearch import Elasticsearch
from elasticsearch import exceptions
import bugzilla
import dateutil
import dateutil.parser

# TODO: add some kind report system

def convert_iso_time (iso_date):
    try:
        normal_date = dateutil.parser.parse(str(iso_date))
    except Exception as e:
        error("Something went wrong when tried to convert xmlrpc datetime to normal datetime:\n%s" % e)
    else:
        return '' if normal_date is None else normal_date


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
        raise SystemExit(1) # TODO: on what condition should it die?
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


def bug_2_es_entry(bug):
    try:
        debug('Converting bug to Elasticsearch entry')
        bug.refresh() # TODO: BUG? sometimes python-bugzilla doesn't pull all fields
        bug_body = bug.__dict__
        bug_id = bug.__dict__['id']
        bug_body.pop('bugzilla') # this is not a default bug field
        bug_body.pop('comments') # TODO: handle comments
        bug_body.pop('flags') # TODO: handle flags
        # TODO: improve convert iso (xml rpc) date time - find all values and not one by one
        bug_body['last_change_time'] = convert_iso_time(bug.__dict__['last_change_time'])
        bug_body['creation_time'] = convert_iso_time(bug.__dict__['creation_time'])
        if 'cf_last_closed' in bug_body:
            bug_body['cf_last_closed'] = convert_iso_time(bug.__dict__['cf_last_closed'])
    except Exception as e:
        # TODO: handle different exceptions. such as connection with bugzilla.
        error("Something went wrong when tried to convert bug to Elasticsearch entry:\n%s" % e)
        return {}, ''
    else:
        return bug_body, bug_id


def push_bugs_2_es(es, bz, config):
    for section, section_config in config.iteritems():
        # TODO: not good - it pulls all the bugs and their fields => error 502
        bugs = bz.query(section_config['query'].dict())
        for bug in bugs:
            bug_body, bug_id = bug_2_es_entry(bug)
            try:
                debug("Adding bug number %s to Elasticsearch" % bug_id)
                es.index(index=section_config['index'],
                         doc_type=section_config['doc_type'],
                         id=bug_id, body=bug_body)
            except Exception as e:
                # TODO: handle elasticsearch issues
                error("Something went wrong when tried to add bug %s to Elasticsearch:\n%s\n%s" % (bug_id, bug_body, e))


def main():
    # TODO: configuration file : logging and other main program behavior
    logging.basicConfig(level=logging.INFO, handlers=[ColorHandler()])
    info("Starting project")
    config = load_config()
    # TODO: configuration file : What to initalize
    es = init_es(config['elasticsearch'])
    bz = init_bz(config['bugzilla'])
    # TODO: Configuration file : What to pull and how to store
    push_bugs_2_es(es, bz, config['bugzilla_elasticsearch'])
    # TODO: What is the purpose of this program? only for POC? framework for a job or a plugin?


if __name__ == '__main__':
    main()