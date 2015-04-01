#!/usr/bin/env python

from subprocess import call
try:
    from subprocess import check_output
except ImportError:
    def check_output(*popenargs, **kwargs):
        import subprocess
        # backported from python 2.7 stdlib
        process = subprocess.Popen(
            stdout=subprocess.PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            error = subprocess.CalledProcessError(retcode, cmd)
            error.output = output
            raise error
        return output

import subprocess
import os
import time
import sys
import re
import string
import logging
import json

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.WARNING)


def wait_for_health():
    print "Wait for health_ok...",
    tries = 0
    while call("./ceph health 2> /dev/null | grep -v 'HEALTH_OK\|HEALTH_WARN' > /dev/null", shell=True) == 0:
        if ++tries == 30:
            raise Exception("Time exceeded to go to health")
        call("./ceph -s", shell=True)
        time.sleep(5)
    print "DONE"


def wait_for_clean():
    print "Wait for clean...",
    tries = 0
    while True:
        JSON = check_output("./ceph pg dump_json 2> /dev/null", shell=True)
        jsondict = json.loads(JSON)
        nonclean = [pg for pg in jsondict['pg_stats'] if pg['state'] != "active+clean" ]
        if len(nonclean) == 0:
            break
        if ++tries == 30:
            raise Exception("Time exceeded to go to health")
        time.sleep(5)
    print "DONE"


def get_pool_id(name, nullfd):
    cmd = "./ceph osd pool stats {pool}".format(pool=name).split()
    # pool {pool} id # .... grab the 4 field
    return check_output(cmd, stderr=nullfd).split()[3]


# return a list of unique PGS given an osd subdirectory
def get_osd_pgs(SUBDIR, ID):
    PGS = []
    if ID:
        endhead = re.compile("{id}.*_head$".format(id=ID))
    DIR = os.path.join(SUBDIR, "current")
    PGS += [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and (ID is None or endhead.match(f))]
    PGS = [re.sub("_head", "", p) for p in PGS if "_head" in p]
    return PGS


# return a sorted list of unique PGs given a directory
def get_pgs(DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0]
    PGS = []
    for d in OSDS:
        SUBDIR = os.path.join(DIR, d)
        PGS += get_osd_pgs(SUBDIR, ID)
    return sorted(set(PGS))


def get_one_osd_pgs(DIR, OSD):
    SUBDIR = os.path.join(DIR, OSD)
    PGS = get_osd_pgs(SUBDIR, None)
    return sorted(PGS)


# return a sorted list of PGS a subset of ALLPGS that contain objects with prefix specified
def get_objs(ALLPGS, prefix, DIR, ID):
    OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0]
    PGS = []
    for d in OSDS:
        DIRL2 = os.path.join(DIR, d)
        SUBDIR = os.path.join(DIRL2, "current")
        for p in ALLPGS:
            PGDIR = p + "_head"
            if not os.path.isdir(os.path.join(SUBDIR, PGDIR)):
                continue
            FINALDIR = os.path.join(SUBDIR, PGDIR)
            # See if there are any objects there
            if [f for f in [ val for  _, _, fl in os.walk(FINALDIR) for val in fl ] if string.find(f, prefix) == 0 ]:
                PGS += [p]
    return sorted(set(PGS))


# return a sorted list of OSDS which have data from a given PG
def get_osds(PG, DIR):
    ALLOSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR, f)) and string.find(f, "osd") == 0]
    OSDS = []
    for d in ALLOSDS:
        DIRL2 = os.path.join(DIR, d)
        SUBDIR = os.path.join(DIRL2, "current")
        PGDIR = PG + "_head"
        if not os.path.isdir(os.path.join(SUBDIR, PGDIR)):
            continue
        OSDS += [d]
    logging.debug(OSDS)
    return sorted(OSDS)


def get_lines(filename):
    tmpfd = open(filename, "r")
    line = True
    lines = []
    while line:
        line = tmpfd.readline().rstrip('\n')
        if line:
            lines += [line]
    tmpfd.close()
    os.unlink(filename)
    return lines


def cat_file(level, filename):
    if level < logging.getLogger().getEffectiveLevel():
        return
    print "File: " + filename
    with open(filename, "r") as f:
        while True:
            line = f.readline().rstrip('\n')
            if not line:
                break
            print line
    print "<EOF>"


def vstart(new, opt=""):
    print "vstarting....",
    NEW = new and "-n" or ""
    call("MON=1 OSD=6 CEPH_PORT=7401 ./vstart.sh -l {new} -d mon osd {opt} > /dev/null 2>&1".format(new=NEW, opt=opt), shell=True)
    print "DONE"


def get_nspace(num):
    if num == 0:
        return ""
    return "ns{num}".format(num=num)


def verify(DATADIR, POOL, NAME_PREFIX):
    TMPFILE = r"/tmp/tmp.{pid}".format(pid=os.getpid())
    nullfd = open(os.devnull, "w")
    ERRORS = 0
    for nsfile in [f for f in os.listdir(DATADIR) if f.split('-')[1].find(NAME_PREFIX) == 0]:
        nspace = nsfile.split("-")[0]
        file = nsfile.split("-")[1]
        path = os.path.join(DATADIR, nsfile)
        try:
            os.unlink(TMPFILE)
        except:
            pass
        cmd = "./rados -p {pool} -N '{nspace}' get {file} {out}".format(pool=POOL, file=file, out=TMPFILE, nspace=nspace)
        logging.debug(cmd)
        call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
        cmd = "diff -q {src} {result}".format(src=path, result=TMPFILE)
        logging.debug(cmd)
        ret = call(cmd, shell=True)
        if ret != 0:
            logging.error("{file} data not imported properly".format(file=file))
            ERRORS += 1
        try:
            os.unlink(TMPFILE)
        except:
            pass
    return ERRORS

CEPH_DIR = "ceph_objectstore_tool2_dir"
CEPH_CONF = os.path.join(CEPH_DIR, 'ceph.conf')
PROMPT = False


def kill_daemons():
    logging.warning("kill_daemons")
    call("./init-ceph -c {conf} stop osd mon > /dev/null 2>&1".format(conf=CEPH_CONF), shell=True)
    time.sleep(2)


def down_osd(ONEOSD, nullfd):
    print "Take down osd " + ONEOSD
    cmd = "./ceph osd set noup"
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "./ceph osd down {osd}".format(osd=ONEOSD.split("osd")[1])
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
#    cmd = "./ceph osd out {osd}".format(osd=ONEOSD.split("osd")[1])
#    logging.debug(cmd)
#    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    # XXX: Check for down OSD and out?
    time.sleep(15)


def prompt(message, force=False):
    print message
    call("./ceph -s", shell=True)
    if not ( PROMPT or force ):
        return
    while True:
        print "> ",
        userinput = sys.stdin.readline().rstrip('\n')
        if userinput == "cont" or userinput == "continue":
            return
        call(userinput, shell=True)


def main(argv):
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    nullfd = open(os.devnull, "w")

    call("rm -fr {dir}; mkdir {dir}".format(dir=CEPH_DIR), shell=True)
    os.environ["CEPH_DIR"] = CEPH_DIR
    OSDDIR = os.path.join(CEPH_DIR, "dev")
    REP_POOL = "rep_pool"
    REP_NAME = "REPobject"
    if len(argv) > 0 and argv[0] == 'prompt':
        PROMPT = True
    PG_COUNT = 8
    NUM_REP_OBJECTS = 32
    NUM_NSPACES = 2
    # Larger data sets for first object per namespace
    DATALINECOUNT = 10
    # Number of objects to do xattr/omap testing on
    ATTR_OBJS = 2
    ERRORS = 0
    pid = os.getpid()
    TESTDIR = "/tmp/test.{pid}".format(pid=pid)
    DATADIR = "/tmp/data.{pid}".format(pid=pid)
    CFSD_PREFIX = "./ceph-objectstore-tool --data-path " + OSDDIR + "/{osd} --journal-path " + OSDDIR + "/{osd}.journal "

    os.environ['CEPH_CONF'] = CEPH_CONF
    vstart(new=True, opt="-o osd_min_pg_log_entries=10 -o osd_max_pg_log_entries=20")
    wait_for_health()

    cmd = "./ceph osd pool create {pool} {pg} {pg} replicated".format(pool=REP_POOL, pg=PG_COUNT)
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    time.sleep(5)
    REPID = get_pool_id(REP_POOL, nullfd)

    print "Created Replicated pool #{repid}".format(repid=REPID)

    print "Creating {objs} objects in replicated pool".format(objs=(NUM_REP_OBJECTS*NUM_NSPACES))
    cmd = "mkdir -p {datadir}".format(datadir=DATADIR)
    logging.debug(cmd)
    call(cmd, shell=True)

    db = {}

    objects = range(1, NUM_REP_OBJECTS + 1)
    nspaces = range(NUM_NSPACES)
    for n in nspaces:
        nspace = get_nspace(n)

        db[nspace] = {}

        for i in objects:
            NAME = REP_NAME + "{num}".format(num=i)
            LNAME = nspace + "-" + NAME
            DDNAME = os.path.join(DATADIR, LNAME)

            cmd = "rm -f " + DDNAME
            logging.debug(cmd)
            call(cmd, shell=True)

            if i == 1:
                dataline = range(DATALINECOUNT)
            else:
                dataline = range(1)
            fd = open(DDNAME, "w")
            data = "This is the replicated data for " + LNAME + "\n"
            for _ in dataline:
                fd.write(data)
            fd.close()

            cmd = "./rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=REP_POOL, name=NAME, ddname=DDNAME, nspace=nspace)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=nullfd)
            if ret != 0:
                logging.critical("Replicated pool object creation failed with {ret}".format(ret=ret))
                return 1

            db[nspace][NAME] = {}

            if i < ATTR_OBJS + 1:
                keys = range(i)
            else:
                keys = range(0)
            db[nspace][NAME]["xattr"] = {}
            for k in keys:
                if k == 0:
                    continue
                mykey = "key{i}-{k}".format(i=i, k=k)
                myval = "val{i}-{k}".format(i=i, k=k)
                cmd = "./rados -p {pool} -N '{nspace}' setxattr {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval, nspace=nspace)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.error("setxattr failed with {ret}".format(ret=ret))
                    ERRORS += 1
                db[nspace][NAME]["xattr"][mykey] = myval

            # Create omap header in all objects but REPobject1
            if i < ATTR_OBJS + 1 and i != 1:
                myhdr = "hdr{i}".format(i=i)
                cmd = "./rados -p {pool} -N '{nspace}' setomapheader {name} {hdr}".format(pool=REP_POOL, name=NAME, hdr=myhdr, nspace=nspace)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.critical("setomapheader failed with {ret}".format(ret=ret))
                    ERRORS += 1
                db[nspace][NAME]["omapheader"] = myhdr

            db[nspace][NAME]["omap"] = {}
            for k in keys:
                if k == 0:
                    continue
                mykey = "okey{i}-{k}".format(i=i, k=k)
                myval = "oval{i}-{k}".format(i=i, k=k)
                cmd = "./rados -p {pool} -N '{nspace}' setomapval {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval, nspace=nspace)
                logging.debug(cmd)
                ret = call(cmd, shell=True)
                if ret != 0:
                    logging.critical("setomapval failed with {ret}".format(ret=ret))
                db[nspace][NAME]["omap"][mykey] = myval

    logging.debug(db)

    if ERRORS:
        logging.critical("Unable to set up test")
        return 1

    ALLOSDS = [f for f in os.listdir(OSDDIR) if os.path.isdir(os.path.join(OSDDIR, f)) and string.find(f, "osd") == 0]
    ALLREPPGS = get_pgs(OSDDIR, REPID)
    logging.debug(ALLREPPGS)

    OBJREPPGS = get_objs(ALLREPPGS, REP_NAME, OSDDIR, REPID)
    logging.debug(OBJREPPGS)

    ONEPG = ALLREPPGS[0]
    logging.debug(ONEPG)
    osds = get_osds(ONEPG, OSDDIR)
    ONEOSD = osds[0]
    logging.debug(ONEOSD)
    ALLBUTONEOSD = osds[1:]

    down_osd(ONEOSD, nullfd)
    time.sleep(10)
    prompt("Should have 1 OSD down")
    #wait_for_clean()

    # cmd = "./rados -p {pool} mksnap snap1".format(pool=REP_POOL)
    # logging.debug(cmd)
    # ret = call(cmd, shell=True, stderr=nullfd)

    # for osd in ALLBUTONEOSD:
    #     cmd="./ceph --admin-daemon out/osd.{osd}.asok config set filestore_blackhole 1".format(osd=osd.split("osd")[1])
    #     logging.debug(cmd)
    #     call(cmd, shell=True)

    objects = range(1, NUM_REP_OBJECTS + 1)
    nspaces = range(NUM_NSPACES)
    for n in nspaces:
        nspace = get_nspace(n)

        for i in objects:
            NAME = REP_NAME + "{num}".format(num=i)
            LNAME = nspace + "-" + NAME
            DDNAME = os.path.join(DATADIR, LNAME)

            cmd = "rm -f " + DDNAME
            logging.debug(cmd)
            call(cmd, shell=True)

            if i == 1:
                dataline = range(DATALINECOUNT)
            else:
                dataline = range(1)
            fd = open(DDNAME, "w")
            data = "Second write of replicated data for " + LNAME + "\n"
            for _ in dataline:
                fd.write(data)
            fd.close()

            cmd = "./rados -p {pool} -N '{nspace}' put {name} {ddname}".format(pool=REP_POOL, name=NAME, ddname=DDNAME, nspace=nspace)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stderr=nullfd)
            if ret != 0:
                logging.critical("Replicated pool object creation failed with {ret}".format(ret=ret))
                return 1

    # Restart with recovery delay
    print "Restart with recovery delay to get new log without recovery"
    kill_daemons()
    vstart(new=False, opt="-o 'osd recovery delay start = 10000' -o osd_min_pg_log_entries=5 -o osd_max_pg_log_entries=10")
    time.sleep(15)
    prompt("Bring up with recovery delay")

    print "Bring back up osd " + ONEOSD
    cmd = "./ceph osd unset noup"
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    cmd = "./ceph osd in {osd}".format(osd=ONEOSD.split("osd")[1])
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)

    # Let changes above get going
    time.sleep(15)
    prompt("rejoining")
    # With recovery delay, let down OSD get log, but not finish recovery
    wait_for_health()
    prompt("after health")
    kill_daemons()

    vstart(new=False, opt="-o 'osd recovery delay start = 10000' -o osd_min_pg_log_entries=5 -o osd_max_pg_log_entries=10")
    wait_for_health()

    down_osd(ONEOSD, nullfd)
    cmd = "pkill -f 'ceph-osd -i {osdid}'".format(osdid=ONEOSD.split("osd")[1])
    logging.debug(cmd)
    call(cmd, shell=True)
    cmd = "./ceph osd unset noup"
    logging.debug(cmd)
    call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    prompt("Waiting for killed osd to happen")
    wait_for_health()

    # Put back osd_recovery_delay_start

    print "Cause a split to occur with recovery disabled"
    # Cause REP_POOL to split and test import with object/log filtering
    cmd = "./ceph osd pool set {pool} pg_num 32".format(pool=REP_POOL)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    time.sleep(15)
    cmd = "./ceph osd pool set {pool} pgp_num 32".format(pool=REP_POOL)
    logging.debug(cmd)
    ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
    # wait_for_clean()
    time.sleep(30)

    kill_daemons()

    print "Export from osd " + ONEOSD
    os.mkdir(TESTDIR)
    os.mkdir(os.path.join(TESTDIR, ONEOSD))
    for pg in get_one_osd_pgs(OSDDIR, ONEOSD):
        mydir = os.path.join(TESTDIR, ONEOSD)
        fname = os.path.join(mydir, pg)
        cmd = (CFSD_PREFIX + "--op export --pgid {pg} --file {file}").format(osd=ONEOSD, pg=pg, file=fname)
        logging.debug(cmd)
        ret = call(cmd, shell=True, stdout=nullfd, stderr=nullfd)
        if ret != 0:
            logging.error("Exporting failed for pg {pg} on {osd} with {ret}".format(pg=pg, osd=ONEOSD, ret=ret))
            ERRORS += 1

    if ERRORS:
        logging.critical("Export failures")
        return 1

    print "Moving some pgs from old export information"
    for pg in get_one_osd_pgs(OSDDIR, ONEOSD):
        CUROSDS = get_osds(pg, OSDDIR)
        for testosd in ALLOSDS:
            if testosd == ONEOSD:
                continue
            if testosd in CUROSDS:
                continue

            dir = os.path.join(TESTDIR, ONEOSD)
            file = os.path.join(dir, pg)
            cmd = (CFSD_PREFIX + "--op import --file {file}").format(osd=testosd, file=file)
            logging.debug(cmd)
            ret = call(cmd, shell=True, stdout=nullfd)
            if ret != 0:
                logging.error("Import failed from {file} with {ret}".format(file=file, ret=ret))
                ERRORS += 1
            # cmd = (CFSD_PREFIX + "--op log --pgid {pgid}").format(osd=testosd, pgid=pg)
            # logging.debug(cmd)
            # ret = call(cmd, shell=True)

    # See if we can start cluster now with imported pgs
    if ERRORS == 0:
        vstart(new=False)
        wait_for_health()

        prompt("Finished", True)

    call("/bin/rm -rf {dir}".format(dir=TESTDIR), shell=True)
    call("/bin/rm -rf {dir}".format(dir=DATADIR), shell=True)

    if ERRORS == 0:
        print "TEST PASSED"
        return 0
    else:
        print "TEST FAILED WITH {errcount} ERRORS".format(errcount=ERRORS)
        return 1

if __name__ == "__main__":
    status = 1
    try:
        status = main(sys.argv[1:])
    finally:
        kill_daemons()
        call("/bin/rm -fr {dir}".format(dir=CEPH_DIR), shell=True)
    sys.exit(status)
