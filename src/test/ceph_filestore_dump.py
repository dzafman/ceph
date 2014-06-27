#! /usr/bin/python

from subprocess import call
from subprocess import check_output
#from subprocess import DEVNULL
import os
import time
import sys
import re
import string

#  wait_for_health() {
#    echo -n "Wait for health_ok..."
#    while( ./ceph health 2> /dev/null | grep -v HEALTH_OK > /dev/null)
#    do
#      sleep 5
#    done
#    echo DONE
#  }

def wait_for_health():
  print "Wait for healt_ok...",
  while call("./ceph health 2> /dev/null | grep -v HEALTH_OK > /dev/null", shell=True) == 0:
    time.sleep(5)
  print "DONE"

def get_pool_id(name):
  cmd = "./ceph osd pool stats {pool}".format(pool = name).split()
  # pool {pool} id # .... grab the 4 field
  return check_output(cmd, stderr=nullfd).split()[3]

# return a sorted list of unique PGs given a directory
def get_pgs(DIR, ID):
  OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR,f)) and string.find(f,"osd") == 0 ]
  PGS = []
  endhead = re.compile("{id}.*_head$".format(id=ID))
  for d in OSDS:
    DIRL2 = os.path.join(DIR, d)
    SUBDIR = os.path.join(DIRL2, "current")
    PGS += [f for f in os.listdir(SUBDIR) if os.path.isdir(os.path.join(SUBDIR, f)) and endhead.match(f)]
  PGS = [re.sub("_head", "", p) for p in PGS]
  return sorted(set(PGS))

# return a sorted list of PGS a subset of ALLPGS that contain objects with prefix specified
def get_objs(ALLPGS, prefix, DIR, ID):
  OSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR,f)) and string.find(f,"osd") == 0 ]
  PGS = []
  for d in OSDS:
    DIRL2 = os.path.join(DIR, d)
    SUBDIR = os.path.join(DIRL2, "current")
    OBJS = []
    for p in ALLPGS:
      PGDIR = p + "_head"
      if not os.path.isdir(os.path.join(SUBDIR, PGDIR)): continue
      FINALDIR = os.path.join(SUBDIR, PGDIR)
      # See if there are any objects there
      if [f for f in os.listdir(FINALDIR) if os.path.isfile(os.path.join(FINALDIR, f)) and string.find(f, prefix) == 0 ]:
        PGS += [p]
  return sorted(set(PGS))

# return a sorted list of OSDS which have data from a given PG
def get_osds(PG, DIR):
  ALLOSDS = [f for f in os.listdir(DIR) if os.path.isdir(os.path.join(DIR,f)) and string.find(f,"osd") == 0 ]
  for d in ALLOSDS:
    DIRL2 = os.path.join(DIR, d)
    SUBDIR = os.path.join(DIRL2, "current")
    OSDS = []
    PGDIR = PG + "_head"
    if not os.path.isdir(os.path.join(SUBDIR, PGDIR)): continue
    OSDS += [d]
  return sorted(OSDS)

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
nullfd = open(os.devnull, "w")

OSDDIR="dev"
REP_POOL="rep_pool"
REP_NAME="REPobject"
EC_POOL="ec_pool"
EC_NAME="ECobject"
NUM_OBJECTS=4
ERRORS=0
pid = os.getpid()
TESTDIR="/tmp/test.{pid}".format(pid=pid)
DATADIR="/tmp/data.{pid}".format(pid=pid)
JSONOBJ="/tmp/json.{pid}".format(pid=pid)

#  REP_POOL="rep_pool"
#  REP_NAME="REPobject"
#  EC_POOL="ec_pool"
#  EC_NAME="ECobject"
#  NUM_OBJECTS=4
#  ERRORS=0
#  TESTDIR="/tmp/test.$$"
#  DATADIR="/tmp/data.$$"
#  JSONOBJ="/tmp/json.$$"
#  

print "vstarting....",
call("OSD=4 ./vstart.sh -l -n -d > /dev/null 2>&1", shell=True)
print "DONE"

wait_for_health()

#  echo -n "vstarting...."
#  OSD=4 ./vstart.sh -l -n -d > /dev/null 2>&1
#  echo DONE
#  
#  wait_for_health

cmd = "./ceph osd pool create {pool} 12 12 replicated  2> /dev/null".format(pool = REP_POOL)
call(cmd, shell=True)
#cmd = "./ceph osd pool stats {pool}  2> /dev/null | grep ^pool | awk '{{ print $4 }}'".format(pool = REP_POOL)

#cmd = "./ceph osd pool stats {pool}".format(pool = REP_POOL).split()
# pool {pool} id # .... grab the 4 field
#REPID = check_output(cmd, stderr=nullfd).split()[3]
REPID = get_pool_id(REP_POOL)

print "Created Replicated pool #{repid}".format(repid=REPID)

#  
#  ./ceph osd pool create $REP_POOL 12 12 replicated  2> /dev/null
#  REPID=`./ceph osd pool stats $REP_POOL  2> /dev/null | grep ^pool | awk '{ print $4 }'`
#  
#  echo "Created replicated pool #" $REPID

cmd = "./ceph osd erasure-code-profile set testprofile ruleset-failure-domain=osd"
call(cmd, shell=True)
cmd = "./ceph osd erasure-code-profile get testprofile"
call(cmd, shell=True)
cmd = "./ceph osd pool create {pool} 12 12 erasure testprofile".format(pool = EC_POOL)
call(cmd, shell=True)
ECID = get_pool_id(EC_POOL)

print "Created Erasure coded pool #{ecid}".format(ecid=ECID)

#  
#  ./ceph osd erasure-code-profile set testprofile ruleset-failure-domain=osd || return 1
#  ./ceph osd erasure-code-profile get testprofile
#  ./ceph osd pool create $EC_POOL 12 12 erasure testprofile || return 1
#  ECID=`./ceph osd pool stats $EC_POOL  2> /dev/null | grep ^pool | awk '{ print $4 }'`
#  
#  echo "Created Erasure coded pool #" $ECID

print "Creating {objs} objects in replicated pool".format(objs=NUM_OBJECTS)
cmd = "mkdir -p {datadir}".format(datadir=DATADIR)
call(cmd, shell=True)

db = {}

objects = range(1,NUM_OBJECTS + 1)
for i in objects:
  NAME = REP_NAME + "%d" %  i
  DDNAME = DATADIR + NAME

  cmd = "rm -f " + DDNAME
  call(cmd, shell=True)

  dataline = range(10000)
  f = open(DDNAME, "w")
  data = "This is the replicated data for " + NAME + "\n"
  for j in dataline:
    f.write(data)
  f.close()

  cmd = "./rados -p {pool} put {name} {ddname}".format(pool=REP_POOL, name=NAME, ddname=DDNAME)
  call(cmd, shell=True, stderr=nullfd)

  db[NAME] = {}

  keys = range(i)
  db[NAME]["xattr"] = {}
  for k in keys:
    if k == 0: continue
    mykey = "key{i}-{k}".format(i=i, k=k)
    myval = "val{i}-{k}".format(i=i, k=k)
    cmd = "./rados -p {pool} setxattr {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval)
    print cmd
    call(cmd, shell=True)
    db[NAME]["xattr"][mykey] = myval

  # Create omap header in all objects but REPobject1
  if i != 1:
    myhdr = "hdr{i}".format(i=i)
    cmd = "./rados -p {pool} setomapheader {name} {hdr}".format(pool=REP_POOL, name=NAME, hdr=myhdr)
    print cmd
    call(cmd, shell=True)
    db[NAME]["omapheader"] = myhdr

  db[NAME]["omap"] = {}
  for k in keys:
    if k == 0: continue
    mykey = "okey{i}-{k}".format(i=i, k=k)
    myval = "oval{i}-{k}".format(i=i, k=k)
    cmd = "./rados -p {pool} setomapval {name} {key} {val}".format(pool=REP_POOL, name=NAME, key=mykey, val=myval)
    print cmd
    call(cmd, shell=True)
    db[NAME]["omap"][mykey] = myval


#  
#  echo "Creating $NUM_OBJECTS objects in replicated pool"
#  mkdir -p $DATADIR
#  for i in `seq 1 $NUM_OBJECTS`
#  do
#    NAME="${REP_NAME}$i"
#    rm -f $DATADIR/$NAME
#    for j in `seq 1 10000`
#    do
#      echo "This is the replicated data for $NAME" >> $DATADIR/$NAME
#    done
#    ./rados -p $REP_POOL put $NAME $DATADIR/$NAME  2> /dev/null
#    ACNT=`expr $i - 1`
#    for k in `seq 0 $ACNT`
#    do
#      if [ $k = "0" ];
#      then
#        continue
#      fi
#      ./rados -p $REP_POOL setxattr $NAME key${i}-${k} val${i}-${k}
#    done
#    # Create omap header in all objects but REPobject1
#    if [ $i != "1" ];
#    then
#      ./rados -p $REP_POOL setomapheader $NAME hdr${i}
#    fi
#    for k in `seq 0 $ACNT`
#    do
#      if [ $k = "0" ];
#      then
#        continue
#      fi
#      ./rados -p $REP_POOL setomapval $NAME okey${i}-${k} oval${i}-${k}
#    done
#  done

print "Creating {objs} objects in erasure coded pool".format(objs=NUM_OBJECTS)

for i in objects:
  NAME = EC_NAME + "%d" %  i
  DDNAME = DATADIR + NAME

  cmd = "rm -f " + DDNAME
  call(cmd, shell=True)

  f = open(DDNAME, "w")
  data = "This is the erasure coded data for " + NAME + "\n"
  for j in dataline:
    f.write(data)
  f.close()

  cmd = "./rados -p {pool} put {name} {ddname}".format(pool=EC_POOL, name=NAME, ddname=DDNAME)
  call(cmd, shell=True, stderr=nullfd)
  
  db[NAME] = {}

  db[NAME]["xattr"] = {}
  keys = range(i)
  for k in keys:
    if k == 0: continue
    mykey = "key{i}-{k}".format(i=i, k=k)
    myval = "val{i}-{k}".format(i=i, k=k)
    cmd = "./rados -p {pool} setxattr {name} {key} {val}".format(pool=EC_POOL, name=NAME, key=mykey, val=myval)
    print cmd
    call(cmd, shell=True)
    db[NAME]["xattr"][mykey] = myval

  # Omap isn't supported in EC pools
  db[NAME]["omap"] = {}

print db
#  
#  echo "Creating $NUM_OBJECTS objects in erausre coded pool"
#  for i in `seq 1 $NUM_OBJECTS`
#  do
#    NAME="${EC_NAME}$i"
#    rm -f $DATADIR/$NAME
#    for j in `seq 1 10000`
#    do
#      echo "This is the erasure coded data for $NAME" >> $DATADIR/$NAME
#    done
#    ./rados -p $EC_POOL put $NAME $DATADIR/$NAME  2> /dev/null
#    ACNT=`expr $i - 1`
#    for k in `seq 0 $ACNT`
#    do
#      if [ $k = "0" ];
#      then
#        continue
#      fi
#      ./rados -p $EC_POOL setxattr $NAME key${i}-${k} val${i}-${k}
#    done
#    # Omap isn't supported in EC pools
#  done
#  

call("./stop.sh", stderr=nullfd)

ALLREPPGS=get_pgs(OSDDIR, REPID)
print ALLREPPGS
ALLECPGS = get_pgs(OSDDIR, ECID)
print ALLECPGS

OBJREPPGS = get_objs(ALLREPPGS, REP_NAME, OSDDIR, REPID)
print OBJREPPGS
OBJECPGS = get_objs(ALLECPGS, EC_NAME, OSDDIR, ECID)
print OBJECPGS

#  ./stop.sh > /dev/null 2>&1
#  
#  ALLREPPGS=`ls -d dev/*/current/${REPID}.*_head | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#  #echo ALL REP PGS: $ALLREPPGS
#  ALLECPGS=`ls -d dev/*/current/${ECID}.*_head | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#  #echo ALL EC PGS: $ALLECPGS
#  
#  OBJREPPGS=`ls dev/*/current/${REPID}.*_head/${REP_NAME}* | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#  #echo OBJECT REP PGS: $OBJREPPGS
#  OBJECPGS=`ls dev/*/current/${ECID}.*_head/${EC_NAME}* | awk -F / '{ print $4}' | sed 's/_head//' | sort -u`
#  #echo OBJECT EC PGS: $OBJECPGS

ONEPG=ALLREPPGS[0]
osds=get_osds(ONEPG, OSDDIR)
ONEOSD=osds[0]

#  
#  ONEPG=`echo $OBJREPPGS | awk '{ print $1 }'`
#  #echo $ONEPG
#  ONEOSD=`ls -d dev/*/current/${ONEPG}_head | awk -F / '{ print $2 }' | head -1`
#  #echo $ONEOSD


def test_failure(cmd, errmsg):
  ttyfd = open("/dev/tty", "rw")
  TMPFILE=r"/tmp/tmp.{pid}".format(pid=pid)
  tmpfd = open(TMPFILE, "w")

  ret = call(cmd, shell=True, stdin=ttyfd, stdout=ttyfd, stderr=tmpfd)
  ttyfd.close()
  tmpfd.close()
  if ret == 0:
    print "Should have failed, but got exit 0"
    return 1

  tmpfd = open(TMPFILE, "r")
  line = tmpfd.readline().rstrip('\n')
  tmpfd.close()
  if line == errmsg:
    print "Correctly failed with message \"" + line + "\""
    return 0
  else:
    print "Bad message to stderr \"" + line + "\""
    return 1

# On export can't use stdout to a terminal
cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type export --pgid {pg}".format(osd=ONEOSD, pg=ONEPG)
ERRORS += test_failure(cmd, "stdout is a tty and no --file option specified")

#  # On export can't use stdout to a terminal
#  ./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type export --pgid $ONEPG > /dev/tty 2> /tmp/tmp.$$
#  if [ $? = "0" ];
#  then
#    echo Should have failed, but got exit 0
#    ERRORS=`expr $ERRORS + 1`
#  fi
#  if head -1 /tmp/tmp.$$ | grep -- "stdout is a tty and no --file option specified" > /dev/null
#  then
#    echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
#  else
#    echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
#    ERRORS=`expr $ERRORS + 1`
#  fi
#  

OTHERFILE="/tmp/foo.{pid}".format(pid=pid)
foofd = open(OTHERFILE, "w")
foofd.close()

#  # On import can't specify a PG
cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type import --pgid {pg} --file {FOO}".format(osd=ONEOSD,\
    pg=ONEPG, FOO=OTHERFILE)
ERRORS += test_failure(cmd, "--pgid option invalid with import")


#  # On import can't specify a PG
#  touch /tmp/foo.$$
#  ./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type import --pgid $ONEPG --file /tmp/foo.$$ 2> /tmp/tmp.$$
#  if [ $? = "0" ];
#  then
#    echo Should have failed, but got exit 0
#    ERRORS=`expr $ERRORS + 1`
#  fi
#  if head -1 /tmp/tmp.$$ | grep -- "--pgid option invalid with import" > /dev/null
#  then
#    echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
#  else
#    echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
#    ERRORS=`expr $ERRORS + 1`
#  fi

os.unlink(OTHERFILE)
cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type import --file {FOO}".format(osd=ONEOSD, FOO=OTHERFILE)
ERRORS += test_failure(cmd, "open: No such file or directory")

#  rm -f /tmp/foo.$$
#  
#  # On import input file not found
#  ./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type import --file /tmp/foo.$$ 2> /tmp/tmp.$$
#  if [ $? = "0" ];
#  then
#    echo Should have failed, but got exit 0
#    ERRORS=`expr $ERRORS + 1`
#  fi
#  if head -1 /tmp/tmp.$$ | grep -- "open: No such file or directory" > /dev/null
#  then
#    echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
#  else
#    echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
#    ERRORS=`expr $ERRORS + 1`
#  fi
#  

# On import can't use stdin from a terminal
cmd = "./ceph_filestore_dump --filestore-path dev/{osd} --journal-path dev/{osd}.journal --type import --pgid {pg}".format(osd=ONEOSD, pg=ONEPG)
ERRORS += test_failure(cmd, "stdin is a tty and no --file option specified")

#  # On import can't use stdin from a terminal
#  ./ceph_filestore_dump --filestore-path dev/$ONEOSD --journal-path dev/$ONEOSD.journal --type import --pgid $ONEPG < /dev/tty 2> /tmp/tmp.$$
#  if [ $? = "0" ];
#  then
#    echo Should have failed, but got exit 0
#    ERRORS=`expr $ERRORS + 1`
#  fi
#  if head -1 /tmp/tmp.$$ | grep -- "stdin is a tty and no --file option specified" > /dev/null
#  then
#    echo Correctly failed with message \"`head -1 /tmp/tmp.$$`\"
#  else
#    echo Bad message to stderr \"`head -1 /tmp/tmp.$$`\"
#    ERRORS=`expr $ERRORS + 1`
#  fi
#  
#  rm -f /tmp/tmp.$$
#  

#  
#  # Test --type list and generate json for all objects
#  echo "Testing --type list by generating json for all objects"
#  for pg in $OBJREPPGS $OBJECPGS
#  do
#    OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
#    for osd in $OSDS
#    do
#      ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type list --pgid $pg >> /tmp/tmp.$$
#      if [ "$?" != "0" ];
#      then
#        echo "Bad exit status $? from --type list request"
#        ERRORS=`expr $ERRORS + 1`
#      fi
#    done
#  done
#  
#  sort -u /tmp/tmp.$$ > $JSONOBJ
#  rm -f /tmp/tmp.$$
#  
#  # Test get-bytes
#  echo "Testing get-bytes and set-bytes"
#  for file in ${DATADIR}/${REP_NAME}*
#  do
#    rm -f /tmp/tmp.$$
#    BASENAME=`basename $file`
#    JSON=`grep \"$BASENAME\" $JSONOBJ`
#    for pg in $OBJREPPGS
#    do
#      OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
#      for osd in $OSDS
#      do
#        if [ -e dev/$osd/current/${pg}_head/${BASENAME}_* ];
#        then
#          rm -f /tmp/getbytes.$$
#          ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" get-bytes /tmp/getbytes.$$
#          if [ $? != "0" ];
#          then
#            echo "Bad exit status $?"
#            ERRORS=`expr $ERRORS + 1`
#            continue
#          fi
#          diff -q $file /tmp/getbytes.$$ 
#          if [ $? != "0" ];
#          then
#            echo "Data from get-bytes differ"
#            echo "Got:"
#            cat /tmp/getbyte.$$
#            echo "Expected:"
#            cat $file
#            ERRORS=`expr $ERRORS + 1`
#          fi
#          echo "put-bytes going into $file" > /tmp/setbytes.$$
#          ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" set-bytes - < /tmp/setbytes.$$
#          if [ $? != "0" ];
#          then
#            echo "Bad exit status $? from set-bytes"
#            ERRORS=`expr $ERRORS + 1`
#          fi
#          ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" get-bytes - > /tmp/testbytes.$$
#          if [ $? != "0" ];
#          then
#            echo "Bad exit status $? from get-bytes"
#            ERRORS=`expr $ERRORS + 1`
#          fi
#          diff -q /tmp/setbytes.$$ /tmp/testbytes.$$
#          if [ $? != "0" ];
#          then
#            echo "Data after set-bytes differ"
#            echo "Got:"
#            cat /tmp/testbyte.$$
#            echo "Expected:"
#            cat /tmp/setbytes.$$
#            ERRORS=`expr $ERRORS + 1`
#          fi
#          ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" set-bytes < $file
#          if [ $? != "0" ];
#          then
#            echo "Bad exit status $? from set-bytes to restore object"
#            ERRORS=`expr $ERRORS + 1`
#          fi
#        fi
#      done
#    done
#  done
#  
#  rm -f /tmp/getbytes.$$ /tmp/testbytes.$$ /tmp/setbytes.$$
#  
#  # Testing attrs
#  echo "Testing list-attrs get-attr"
#  for file in ${DATADIR}/${REP_NAME}*
#  do
#    rm -f /tmp/tmp.$$
#    BASENAME=`basename $file`
#    JSON=`grep \"$BASENAME\" $JSONOBJ`
#    for pg in $OBJREPPGS
#    do
#      OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
#      for osd in $OSDS
#      do
#        if [ -e dev/$osd/current/${pg}_head/${BASENAME}_* ];
#        then
#          ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" list-attrs > /tmp/attrs.$$
#          if [ $? != "0" ];
#          then
#            echo "Bad exit status $?"
#            ERRORS=`expr $ERRORS + 1`
#            continue
#          fi
#          for key in `cat /tmp/attrs.$$`
#          do
#            ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal  --pgid $pg "$JSON" get-attr $key > /tmp/val.$$
#            if [ $? != "0" ];
#            then
#              echo "Bad exit status $?"
#              ERRORS=`expr $ERRORS + 1`
#              continue
#            fi
#            if [ "$key" = "_" -o "$key" = "snapset" ];
#            then
#              continue
#            fi
#            OBJNUM=`echo $BASENAME | sed "s/$REP_NAME//"`
#            echo -n $key | sed 's/_key//' > /tmp/checkval.$$
#            cat /tmp/val.$$ | sed 's/val//' > /tmp/testval.$$
#            diff -q /tmp/testval.$$ /tmp/checkval.$$
#            if [ "$?" != "0" ];
#            then
#              echo Got `cat /tmp/val.$$` instead of val`cat /tmp/check.$$`
#              ERRORS=`expr $ERRORS + 1`
#            fi
#          done
#        fi
#      done
#    done
#  done
#  
#  rm -rf /tmp/testval.$$ /tmp/checkval.$$ /tmp/val.$$ /tmp/attrs.$$
#  
#  echo Checking pg info
#  for pg in $ALLREPPGS $ALLECPGS
#  do
#    OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
#    for osd in $OSDS
#    do
#      ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type info --pgid $pg | grep "\"pgid\": \"$pg\"" > /dev/null
#      if [ "$?" != "0" ];
#      then
#        echo "FAILURE: getting info for $pg"
#        ERRORS=`expr $ERRORS + 1`
#      fi
#    done
#  done
#  
#  echo Checking pg logs
#  for pg in $ALLREPPGS $ALLECPGS
#  do
#    OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
#    for osd in $OSDS
#    do
#      ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type log --pgid $pg > /tmp/tmp.$$
#      if [ "$?" != "0" ];
#      then
#        echo "FAILURE: getting log for $pg from $osd"
#        ERRORS=`expr $ERRORS + 1`
#        continue
#      fi
#      # Is this pg in the list of pgs with objects
#      echo -e "\n$OBJREPPGS\n$OBJECPGS" | grep "^$pg$" > /dev/null
#      HASOBJ=$?
#      # Does the log have a modify
#      grep modify /tmp/tmp.$$ > /dev/null
#      MODLOG=$?
#      if [ "$HASOBJ" != "$MODLOG" ];
#      then
#        echo "FAILURE: bad log for $pg from $osd"
#        if [ "$HASOBJ" = "0" ];
#        then
#          MSG=""
#        else
#          MSG="NOT"
#        fi
#        echo "Log should $MSG have a modify entry"
#        ERRORS=`expr $ERRORS + 1`
#      fi
#    done
#  done
#  rm /tmp/tmp.$$
#  
#  echo Checking pg export
#  EXP_ERRORS=0
#  mkdir $TESTDIR
#  for pg in $ALLREPPGS $ALLECPGS
#  do
#    OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
#    for osd in $OSDS
#    do
#       mkdir -p $TESTDIR/$osd
#      ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type export --pgid $pg --file $TESTDIR/$osd/$pg.export.$$ > /tmp/tmp.$$
#      if [ "$?" != "0" ];
#      then
#        echo "FAILURE: Exporting $pg on $osd"
#        echo "ceph_filestore_dump output:"
#        cat /tmp/tmp.$$
#        EXP_ERRORS=`expr $EXP_ERRORS + 1`
#      fi
#      rm /tmp/tmp.$$
#    done
#  done
#  
#  ERRORS=`expr $ERRORS + $EXP_ERRORS`
#  
#  echo Checking pg removal
#  RM_ERRORS=0
#  for pg in $ALLREPPGS $ALLECPGS
#  do
#    OSDS=`ls -d dev/*/current/${pg}_head | awk -F / '{ print $2 }'`
#    for osd in $OSDS
#    do
#      ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type remove --pgid $pg >  /tmp/tmp.$$
#      if [ "$?" != "0" ];
#      then
#        echo "FAILURE: Removing $pg on $osd"
#        echo "ceph_filestore_dump output:"
#        cat /tmp/tmp.$$
#        RM_ERRORS=`expr $RM_ERRORS + 1`
#      fi
#      rm /tmp/tmp.$$
#    done
#  done
#  
#  ERRORS=`expr $ERRORS + $RM_ERRORS`
#  
#  IMP_ERRORS=0
#  if [ $EXP_ERRORS = "0" -a $RM_ERRORS = "0" ];
#  then
#    for dir in $TESTDIR/*
#    do
#       osd=`basename $dir`
#       for file in $dir/*
#       do
#        ./ceph_filestore_dump --filestore-path dev/$osd --journal-path dev/$osd.journal --type import --file $file > /tmp/tmp.$$
#        if [ "$?" != "0" ];
#        then
#          echo "FAILURE: Importing from $file"
#          echo "ceph_filestore_dump output:"
#          cat /tmp/tmp.$$
#          IMP_ERRORS=`expr $IMP_ERRORS + 1`
#        fi
#        rm /tmp/tmp.$$
#      done
#    done
#  else
#    echo "SKIPPING IMPORT TESTS DUE TO PREVIOUS FAILURES"
#  fi
#  
#  ERRORS=`expr $ERRORS + $IMP_ERRORS`
#  rm -rf $TESTDIR
#  
#  if [ $EXP_ERRORS = "0" -a $RM_ERRORS = "0" -a $IMP_ERRORS = "0" ];
#  then
#    echo "Checking replicated import data"
#    for path in ${DATADIR}/${REP_NAME}*
#    do
#      file=`basename $path`
#      for obj_loc in `find dev -name "${file}_*"`
#      do
#        diff $path $obj_loc > /dev/null
#        if [ $? != "0" ];
#        then
#          echo FAILURE: $file data not imported properly into $obj_loc
#          ERRORS=`expr $ERRORS + 1`
#        fi
#      done
#    done
#  
#    OSD=4 ./vstart.sh -l -d > /dev/null 2>&1
#    wait_for_health
#  
#    echo "Checking erasure coded import data"
#    for file in ${DATADIR}/${EC_NAME}*
#    do
#      rm -f /tmp/tmp.$$
#      ./rados -p $EC_POOL get `basename $file` /tmp/tmp.$$ > /dev/null 2>&1
#      diff $file /tmp/tmp.$$ > /dev/null
#      if [ $? != "0" ];
#      then
#        echo FAILURE: $file data not imported properly
#        ERRORS=`expr $ERRORS + 1`
#      fi
#      rm -f /tmp/tmp.$$
#    done
#  
#    ./stop.sh > /dev/null 2>&1
#  else
#    echo "SKIPPING CHECKING IMPORT DATA DUE TO PREVIOUS FAILURES"
#  fi
#  
#  rm -rf $DATADIR $JSONOBJ

if ERRORS == 0:
  print "TEST PASSED"
  sys.exit(0)
else:
  print "TEST FAILED WITH {errcount} ERRORS".format(errcount=ERRORS)
  sys.exit(1)

#  
#  if [ $ERRORS = "0" ];
#  then
#    echo "TEST PASSED"
#    exit 0
#  else
#    echo "TEST FAILED WITH $ERRORS ERRORS"
#    exit 1
#  fi
#  
