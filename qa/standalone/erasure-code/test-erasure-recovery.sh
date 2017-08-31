#!/bin/bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
#
# Author: Kefu Chai <kchai@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7112" # git grep '\<7112\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        run_mon $dir a || return 1
	run_mgr $dir x || return 1
	create_rbd_pool || return 1

        # check that erasure code plugins are preloaded
        CEPH_ARGS='' ceph --admin-daemon $(get_asok_path mon.a) log flush || return 1
        grep 'load: jerasure.*lrc' $dir/mon.a.log || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function setup_osds() {
    for id in $(seq 0 5) ; do
        CEPH_ARGS='--osd_min_pg_log_entries=1 --osd_max_pg_log_entries=2' run_osd $dir $id || return 1
    done
    wait_for_clean || return 1

    # check that erasure code plugins are preloaded
    CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.0) log flush || return 1
    grep 'load: jerasure.*lrc' $dir/osd.0.log || return 1
}

function create_erasure_coded_pool() {
    local poolname=$1

    ceph osd erasure-code-profile set myprofile \
        plugin=jerasure \
        k=2 m=1 \
        crush-failure-domain=osd || return 1
    ceph osd pool create $poolname 1 1 erasure myprofile \
        || return 1
    wait_for_clean || return 1
}

function delete_pool() {
    local poolname=$1

    ceph osd pool delete $poolname $poolname --yes-i-really-really-mean-it
    ceph osd erasure-code-profile rm myprofile
}

function TEST_erasure_code_recovery() {
    local dir=$1

    setup_osds || return 1

    local poolname=pool-jerasure
    create_erasure_coded_pool $poolname || return 1

    bin/ceph osd out 0 1 2

    dd if=/dev/urandom of=$dir/data256k bs=256K count=1
    for i in $(seq 1 100)
    do
	rados -p $poolname put obj$i $dir/data256k
    done
    rm -f $dir/data256k

    ceph osd out 3 4 5
    kill_daemons $dir TERM osd.5 || return 1
    ceph osd down 5
    ceph osd in 0 1 2

    wait_for_clean

    ceph pg dump pgs

    dd if=/dev/urandom of=$dir/data512k bs=512K count=1
    for i in $(seq 1 100)
    do
	rados -p test put obj$i $dir/data512k
    done
    rm -f $dir/data512k

    kill_daemons $dir TERM osd.1 || return 1
    kill_daemons $dir TERM osd.2 || return 1
    ceph osd down 1 2
    ceph osd out 1 2

    CEPH_ARGS='--osd_min_pg_log_entries=1 --osd_max_pg_log_entries=2' run_osd $dir 5 || return 1
    ceph osd up 5
    ceph osd in 3 4 5

    wait_for_clean

    delete_pool $poolname
}

main test-erasure-recovery "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/erasure-code/test-erasure-recovery.sh"
# End:
