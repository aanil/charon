import argparse
import copy
import json
import logging
import multiprocessing as mp
import queue as Queue
import requests
import time
import re
import LIMS2DB.objectsDB.process_categories as pc_cg

from datetime import datetime
from genologics_sql.tables import *
from genologics_sql.utils import *
from genologics_sql.queries import *
from sqlalchemy import text
from charon.utils import QueueHandler


REFERENCE_GENOME_PATTERN = re.compile("\,\s+([0-9A-z\._-]+)\)")


def main(args):
    main_log = setup_logging("acheron_logger", args)
    docs = []
    db_session = get_session()
    if args.proj:
        main_log.info("Updating {0}".format(args.proj))
        project = obtain_project(args.proj, db_session)
        cdt = CharonDocumentTracker(db_session, project, main_log, args)
        cdt.run()
    elif args.new:
        project_list = obtain_recent_projects(db_session)
        main_log.info("Project list : {0}".format(", ".join([x.luid for x in project_list])))
        masterProcess(args, project_list, main_log)
    elif args.all:
        project_list = obtain_all_projects(db_session)
        main_log.info("Project list : {0}".format(", ".join([x.luid for x in project_list])))
        masterProcess(args, project_list, main_log)
    elif args.test:
        print("\n".join(x.__str__() for x in obtain_recent_projects(db_session)))


def setup_logging(name, args):
    mainlog = logging.getLogger(name)
    mainlog.setLevel(level=logging.INFO)
    mfh = logging.handlers.RotatingFileHandler(args.logfile, maxBytes=209715200, backupCount=5)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    mainlog.addHandler(mfh)
    return mainlog


def obtain_all_projects(session):
    query = "select pj.* from project pj \
            where pj.createddate > date '2016-01-01';"
    return session.query(Project).from_statement(text(query)).all()


def obtain_recent_projects(session):
    recent_projectids = get_last_modified_projectids(session)
    if recent_projectids:
        query = "select pj.* from project pj \
            where pj.luid in ({0});".format(",".join(["'{0}'".format(x) for x in recent_projectids]))
        return session.query(Project).from_statement(text(query)).all()
    else:
        return []


def obtain_project(project_id, session):
    query = "select pj.* from project pj \
            where pj.luid LIKE '{pid}'::text OR pj.name LIKE '{pid}';".format(pid=project_id)
    return session.query(Project).from_statement(text(query)).one()


def merge(d1, d2):
    """ Will merge dictionary d2 into dictionary d1.
    On the case of finding the same key, the one in d1 will be used.
    :param d1: Dictionary object
    :param s2: Dictionary object
    """
    d3 = copy.deepcopy(d1)
    for key in d2:
        if key in d3:
            if isinstance(d3[key], dict) and isinstance(d2[key], dict):
                d3[key] = merge(d3[key], d2[key])
            elif d3[key] != d2[key]:
                # special weird cases
                if key == 'status' and d3.get('charon_doctype') == 'sample':
                    d3[key] = d2[key]
            elif d3[key] == d2[key]:
                pass  # same value, nothing to do
        else:
            d3[key] = d2[key]
    return d3

def are_dicts_different(keys, dict1, dict2):
    for key in keys:
        if dict1.get(key) != dict2.get(key):
            return True
    return False

def masterProcess(args, projectList, logger):
    projectsQueue = mp.JoinableQueue()
    logQueue = mp.Queue()
    childs = []
    # spawn a pool of processes, and pass them queue instance
    for i in range(args.processes):
        p = mp.Process(target=processCharon, args=(args, projectsQueue, logQueue))
        p.start()
        childs.append(p)
    # populate queue with data
    for proj in projectList:
        projectsQueue.put(proj.luid)

    # wait on the queue until everything has been processed
    notDone = True
    while notDone:
        try:
            log = logQueue.get(False)
            logger.handle(log)
        except Queue.Empty:
            if not stillRunning(childs):
                notDone = False
                break


def stillRunning(processList):
    ret = False
    for p in processList:
        if p.is_alive():
            ret = True

    return ret


def processCharon(args, queue, logqueue):
    db_session = get_session()
    work = True
    procName = mp.current_process().name
    proclog = logging.getLogger(procName)
    proclog.setLevel(level=logging.INFO)
    mfh = QueueHandler(logqueue)
    mft = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    mfh.setFormatter(mft)
    proclog.addHandler(mfh)
    try:
        time.sleep(int(procname[8:]))
    except:
        time.sleep(1)

    while work:
        # grabs project from queue
        try:
            proj_id = queue.get(block=True, timeout=3)
        except Queue.Empty:
            work = False
            break
        except NotImplementedError:
            # qsize failed, no big deal
            pass
        else:
            # locks the project : cannot be updated more than once.
            proclog.info("Handling {}".format(proj_id))
            project = obtain_project(proj_id, db_session)
            cdt = CharonDocumentTracker(db_session, project, proclog, args)
            cdt.run()

            # signals to queue job is done
            queue.task_done()


class CharonDocumentTracker:

    def __init__(self, session, project, logger, args):
        self.charon_url = args.url
        self.charon_token = args.token
        self.session = session
        self.project = project
        self.logger = logger
        self.samples = {}
        self.docs = []

    def run(self):

        def _compare_doctype(doc):
            order=["project", "sample", "libprep", "seqrun"]
            return order.index(doc['charon_doctype'])

        # order matters because samples depend on seqruns.
        #Document stubs are generated for projects and samples with basic fields and the information that is updated from LIMS.
        #If the document is not present in Charon, fields required for new documents are added to the stubs in the generate_new_*_doc methods.
        #Since libpreps and seqruns are not updated by acheron, they are only added as new documents if not present in Charon.
        self.generate_project_doc_stub()
        self.generate_samples_stubs_and_libprep_seqrun_docs()
        self.docs=sorted(self.docs, key=_compare_doctype)
        self.update_charon()

    def generate_project_doc_stub(self):
        doc = {}
        doc['charon_doctype'] = 'project'
        doc['projectid'] = self.project.luid
        doc['name'] = self.project.name
        
        for udf in self.project.udfs:
            if udf.udfname == 'Bioinformatic QC':
                if udf.udfvalue == 'WG re-seq':
                    doc['best_practice_analysis'] = 'whole_genome_reseq'
                else:
                    doc['best_practice_analysis'] = udf.udfvalue
            if udf.udfname == 'Uppnex ID' and udf.udfvalue:
                doc['uppnex_id'] = udf.udfvalue.strip()
            if udf.udfname == 'Reference genome' and udf.udfvalue:
                matches = REFERENCE_GENOME_PATTERN.search(udf.udfvalue)
                if matches:
                    doc['reference'] = matches.group(1)
                else:
                    doc['reference'] = 'other'
        self.docs.append(doc)
    
    def add_new_project_doc_fields(self):
        fields = {}
        curtime = datetime.now().isoformat()
        fields['created'] = curtime
        fields['modified'] = curtime
        fields['sequencing_facility'] = 'NGI-S'
        fields['pipeline'] = 'NGI'
        fields['status'] = 'OPEN'
        fields['delivery_token'] = 'not_under_delivery'

        return fields

    def generate_samples_stubs_and_libprep_seqrun_docs(self):
        curtime = datetime.now().isoformat()
        for sample in self.project.samples:
            sample_doc = {}
            sample_doc['charon_doctype'] = 'sample'
            sample_doc['projectid'] = self.project.luid
            sample_doc['sampleid'] = sample.name
            self.samples[sample.name] = sample
            
            try:
                remote_sample = self.get_charon_sample(sample.name)
                seqruns_lims = self.seqruns_for_sample(sample.name)
                seqruns_charon = self.remote_seqruns_for_sample(sample.name)
                if remote_sample and remote_sample.get('status') == 'STALE' and seqruns_lims == seqruns_charon:
                    sample_doc['status'] = 'STALE'
            except Exception as e:
                self.logger.error("An error occurred while updating {}: {}. Skipping it.".format(sample.name, e))
                continue

            for udf in sample.udfs:
                if udf.udfname == 'Status (manual)':
                    if udf.udfvalue == 'Aborted':
                        sample_doc['status'] = 'ABORTED'
                if udf.udfname == 'Sample Links' and udf.udfvalue:
                    sample_doc['Pair'] = udf.udfvalue
                if udf.udfname == 'Sample Link Type' and udf.udfvalue:
                    sample_doc['Type'] = udf.udfvalue

            self.docs.append(sample_doc)

            query = "select art.* from artifact art \
                    inner join processiotracker piot on piot.inputartifactid = art.artifactid \
                    inner join artifact_sample_map asm on piot.inputartifactid = asm.artifactid \
                    inner join process pc on piot.processid = pc.processid \
                    where asm.processid = {pcid} and pc.typeid in ({agrlibval_step_id});".format(pcid=sample.processid,agrlibval_step_id=','.join(pc_cg.AGRLIBVAL.keys()))
            libs = self.session.query(Artifact).from_statement(text(query)).all()
            alphaindex = 65
            for lib in libs:
                lib_doc = {}
                lib_doc['charon_doctype'] = 'libprep'
                lib_doc['projectid'] = self.project.luid
                lib_doc['sampleid'] = sample.name
                lib_doc['libprepid'] = chr(alphaindex)
                lib_doc['created'] = curtime
                lib_doc['modified'] = curtime
                lib_doc['qc'] = "PASSED"
                self.docs.append(lib_doc)

                query = "select distinct pro.* from process pro \
                        inner join processiotracker piot on piot.processid = pro.processid \
                        inner join artifact_ancestor_map aam on piot.inputartifactid = aam.artifactid \
                        where pro.typeid in ({seq_step_id}) and aam.ancestorartifactid = {lib_art}".format(seq_step_id=','.join(pc_cg.SEQUENCING.keys()),lib_art=lib.artifactid)
                seqs = self.session.query(Process).from_statement(text(query)).all()
                for seq in seqs:
                    seqdoc = {}
                    seqdoc['charon_doctype'] = 'seqrun'
                    seqdoc['projectid'] = self.project.luid
                    seqdoc['sampleid'] = sample.name
                    seqdoc['libprepid'] = chr(alphaindex)
                    seqdoc['created'] = curtime
                    seqdoc['modified'] = curtime
                    seqdoc['mean_autosomal_coverage'] = 0
                    seqdoc['total_reads'] = 0
                    seqdoc['alignment_status'] = 'NOT_RUNNING'
                    seqdoc['delivery_status'] = 'NOT_DELIVERED'
                    for udf in seq.udfs:
                        if udf.udfname == "Run ID" and udf.udfvalue:
                            seqdoc['seqrunid'] = udf.udfvalue
                            break
                    if 'seqrunid' in seqdoc:
                        self.docs.append(seqdoc)

                alphaindex += 1


    def add_new_samples_doc_fields(self, sample_name):
        fields = {}
        curtime = datetime.now().isoformat()
        fields['created'] = curtime
        fields['modified'] = curtime
        fields['duplication_pc'] = 0
        fields['genotype_concordance'] = 0
        fields['total_autosomal_coverage'] = 0
        fields['status'] = 'FRESH'
        fields['analysis_status'] = 'TO_ANALYZE'

        return fields

    def seqruns_for_sample(self, sampleid):
        seqruns = set()
        for doc in self.docs:
            if doc['charon_doctype'] == 'seqrun' and doc['sampleid'] == sampleid:
                seqruns.add(doc['seqrunid'])

        return seqruns

    def remote_seqruns_for_sample(self, sampleid):
        seqruns = set()
        session = requests.Session()
        headers = {'X-Charon-API-token': self.charon_token, 'content-type': 'application/json'}
        url = "{0}/api/v1/seqruns/{1}/{2}".format(self.charon_url, self.project.luid, sampleid)
        r = session.get(url, headers=headers)
        if r.status_code == requests.codes.ok:
            for sr in r.json()['seqruns']:
                seqruns.add(sr['seqrunid'])
            return seqruns
        elif r.status_code == requests.codes.not_found:
            return None
        else:
            raise Exception('A connection error "{}" occurred while getting the seqrun from Charon'.format(r.status_code))

    def get_charon_sample(self, sampleid):
        session = requests.Session()
        headers = {'X-Charon-API-token': self.charon_token, 'content-type': 'application/json'}
        url = "{0}/api/v1/sample/{1}/{2}".format(self.charon_url, self.project.luid, sampleid)
        r = session.get(url, headers=headers)
        if r.status_code == requests.codes.ok:
            return r.json()
        elif r.status_code == requests.codes.not_found:
            return None
        else:
            raise Exception('A connection error "{}" occurred while getting the sample from Charon'.format(r.status_code))


    def update_charon_modifications(self, cur_doc, new_doc, url, session, headers):
        keys_to_check = new_doc.keys()
        if are_dicts_different(keys_to_check, cur_doc, new_doc):
            merged = merge(cur_doc, new_doc)
            if merged != cur_doc:
                curtime = datetime.now().isoformat()
                #Modification date is added only if the document has changed
                merged['modified'] = curtime
                rq = session.put(url, headers=headers, data=json.dumps(merged))
                doc_type = cur_doc['charon_doctype']
                doc_id_string = cur_doc['projectid']
                if doc_type != 'project':
                    doc_id_string += f"/{cur_doc['sampleid']}"
                if rq.status_code == requests.codes.no_content:
                    self.logger.info(f"{doc_type} {doc_id_string} successfully updated".format(cur_doc['projectid']))         
                else:
                    self.logger.error(f"{doc_type} {doc_id_string} failed to be updated : {rq.text}")


    def update_charon(self):
        session = requests.Session()
        headers = {'X-Charon-API-token': self.charon_token, 'content-type': 'application/json'}
        for doc in self.docs:
            try:
                if doc['charon_doctype'] == 'project':
                    self.logger.info(f"trying to update doc {doc['projectid']}")
                    #Check if the project exists in Charon
                    url = f"{self.charon_url}/api/v1/project/{doc['projectid']}"
                    r = session.get(url, headers=headers)
                    #If the project doc does not exist, create it by adding the fields required for new documents to the stub
                    if r.status_code == 404:
                        url = "{0}/api/v1/project".format(self.charon_url)
                        doc.update(self.add_new_project_doc_fields())
                        rq = session.post(url, headers=headers, data=json.dumps(doc))
                        if rq.status_code == requests.codes.created:
                            self.logger.info(f"project {doc['projectid']} successfully updated".format())
                        else:
                            self.logger.error(f"project {doc['projectid']} failed to be updated : {rq.text}")
                    else:
                        pj = r.json()
                        self.update_charon_modifications(pj, doc, url, session, headers)
                
                elif doc['charon_doctype'] == 'sample':
                    #Check if the sample exists in Charon
                    url = f"{self.charon_url}/api/v1/sample/{doc['projectid']}/{doc['sampleid']}"
                    r = session.get(url, headers=headers)
                    #If the sample doc does not exist, create it by adding the fields required for new documents to the stub
                    if r.status_code == 404:
                        url = "{0}/api/v1/sample/{1}".format(self.charon_url, doc['projectid'])
                        doc.update(self.add_new_samples_doc_fields(doc['sampleid']))
                        rq = session.post(url, headers=headers, data=json.dumps(doc))
                        if rq.status_code == requests.codes.created:
                            self.logger.info(f"sample {doc['projectid']}/{doc['sampleid']} successfully updated")
                        else:
                            self.logger.error(f"sample {doc['projectid']}/{doc['sampleid']} failed to be updated : {rq.text}")
                    else:
                        pj = r.json()
                        self.update_charon_modifications(pj, doc, url, session, headers)
                
                elif doc['charon_doctype'] == 'libprep':
                    url = f"{self.charon_url}/api/v1/libprep/{doc['projectid']}/{doc['sampleid']}/{doc['libprepid']}"
                    r = session.get(url, headers=headers)
                    if r.status_code == 404:
                        url = f"{self.charon_url}/api/v1/libprep/{doc['projectid']}/{doc['sampleid']}"
                        rq = session.post(url, headers=headers, data=json.dumps(doc))
                        if rq.status_code == requests.codes.created:
                            self.logger.info(f"libprep {doc['projectid']}/{doc['sampleid']}/{doc['libprepid']} successfully updated")
                        else:
                            self.logger.error(f"libprep {doc['projectid']}/{doc['sampleid']}/{doc['libprepid']} failed to be updated : {rq.text}")
                
                elif doc['charon_doctype'] == 'seqrun':
                    url = f"{self.charon_url}/api/v1/seqrun/{doc['projectid']}/{doc['sampleid']}/{doc['libprepid']}/{doc['seqrunid']}"
                    r = session.get(url, headers=headers)
                    if r.status_code == 404:
                        url =f"{self.charon_url}/api/v1/seqrun/{doc['projectid']}/{doc['sampleid']}/{doc['libprepid']}"
                        rq = session.post(url, headers=headers, data=json.dumps(doc))
                        if rq.status_code == requests.codes.created:
                            self.logger.info(f"seqrun {doc['projectid']}/{doc['sampleid']}/{doc['libprepid']}/{doc['seqrunid']} successfully updated")
                        else:
                            self.logger.error(f"seqrun {doc['projectid']}/{doc['sampleid']}/{doc['libprepid']}/{doc['seqrunid']} failed to be updated : {rq.text}")
            except Exception as e:
                self.logger.error("Error handling document \n{} \n\n{}".format(doc, e))

if __name__ == "__main__":
    usage = "Usage:       python acheron_sql.py [options]"
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("-k", "--processes", dest="processes", default=12, type=int,
                        help="Number of child processes to start")
    parser.add_argument("-a", "--all", dest="all", default=False, action="store_true",
                        help="Try to upload all IGN projects. This will wipe the current information stored in Charon")
    parser.add_argument("-n", "--new", dest="new", default=False, action="store_true",
                        help="Try to upload new IGN projects. This will NOT erase the current information stored in Charon")
    parser.add_argument("-p", "--project", dest="proj", default=None,
                        help="-p <projectname> will try to upload the given project to charon")
    parser.add_argument("-t", "--token", dest="token", default=os.environ.get('CHARON_API_TOKEN'),
                        help="Charon API Token. Will be read from the env variable CHARON_API_TOKEN if not provided")
    parser.add_argument("-u", "--url", dest="url", default=os.environ.get('CHARON_BASE_URL'),
                        help="Charon base url. Will be read from the env variable CHARON_BASE_URL if not provided")
    parser.add_argument("-v", "--verbose", dest="verbose", default=False, action="store_true",
                        help="prints results for everything that is going on")
    parser.add_argument("-l", "--log", dest="logfile", default=os.path.expanduser("~/acheron.log"),
                        help="location of the log file")
    parser.add_argument("-z", "--test", dest="test", default=False, action="store_true",
                        help="Testing option")
    args = parser.parse_args()

    if not args.token:
        print("No valid token found in arg or in environment. Exiting.")
    if not args.url:
        print("No valid url found in arg or in environment. Exiting.")
        sys.exit(-1)
    main(args)
