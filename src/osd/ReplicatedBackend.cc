// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/errno.h"
#include "ReplicatedBackend.h"
#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDPGPushReply.h"

#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
static ostream& _prefix(std::ostream *_dout, ReplicatedBackend *pgb) {
  return *_dout << pgb->get_parent()->gen_dbg_prefix();
}

ReplicatedBackend::ReplicatedBackend(
  PGBackend::Listener *pg, coll_t coll, OSDService *osd) :
  PGBackend(pg), temp_created(false),
  temp_coll(coll_t::make_temp_coll(pg->get_info().pgid)),
  coll(coll), osd(osd), cct(osd->cct) {}

void ReplicatedBackend::run_recovery_op(
  PGBackend::RecoveryHandle *_h,
  int priority)
{
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  send_pushes(priority, h->pushes);
  send_pulls(priority, h->pulls);
  delete h;
}

void ReplicatedBackend::recover_object(
  const hobject_t &hoid,
  ObjectContextRef head,
  ObjectContextRef obc,
  RecoveryHandle *_h
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;
  RPGHandle *h = static_cast<RPGHandle *>(_h);
  if (get_parent()->get_local_missing().is_missing(hoid)) {
    assert(!obc);
    // pull
    prepare_pull(
      hoid,
      head,
      h);
    return;
  } else {
    assert(obc);
    int started = start_pushes(
      hoid,
      obc,
      h);
    assert(started > 0);
  }
}

void ReplicatedBackend::check_recovery_sources(const OSDMapRef osdmap)
{
  for(map<int, set<hobject_t> >::iterator i = pull_from_peer.begin();
      i != pull_from_peer.end();
      ) {
    if (osdmap->is_down(i->first)) {
      dout(10) << "check_recovery_sources resetting pulls from osd." << i->first
	       << ", osdmap has it marked down" << dendl;
      for (set<hobject_t>::iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	assert(pulling.count(*j) == 1);
	get_parent()->cancel_pull(*j);
	pulling.erase(*j);
      }
      pull_from_peer.erase(i++);
    } else {
      ++i;
    }
  }
}

bool ReplicatedBackend::can_handle_while_inactive(OpRequestRef op)
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PULL:
    return true;
  case MSG_OSD_SUBOP: {
    MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
    if (m->ops.size() >= 1) {
      OSDOp *first = &m->ops[0];
      switch (first->op.op) {
      case CEPH_OSD_OP_PULL:
	return true;
      default:
	return false;
      }
    } else {
      return false;
    }
  }
  default:
    return false;
  }
}

bool ReplicatedBackend::handle_message(
  OpRequestRef op
  )
{
  dout(10) << __func__ << ": " << op << dendl;
  switch (op->get_req()->get_type()) {
  case MSG_OSD_PG_PUSH:
    // TODOXXX: needs to be active possibly
    do_push(op);
    return true;

  case MSG_OSD_PG_PULL:
    do_pull(op);
    return true;

  case MSG_OSD_PG_PUSH_REPLY:
    do_push_reply(op);
    return true;

  case MSG_OSD_SUBOP: {
    MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
    if (m->ops.size() >= 1) {
      OSDOp *first = &m->ops[0];
      switch (first->op.op) {
      case CEPH_OSD_OP_PULL:
	sub_op_pull(op);
	return true;
      case CEPH_OSD_OP_PUSH:
        // TODOXXX: needs to be active possibly
	sub_op_push(op);
	return true;
      default:
	break;
      }
    }
    break;
  }

  case MSG_OSD_SUBOPREPLY: {
    MOSDSubOpReply *r = static_cast<MOSDSubOpReply*>(op->get_req());
    if (r->ops.size() >= 1) {
      OSDOp &first = r->ops[0];
      switch (first.op.op) {
      case CEPH_OSD_OP_PUSH:
	// continue peer recovery
	sub_op_push_reply(op);
	return true;
      }
    }
    break;
  }

  default:
    break;
  }
  return false;
}

void ReplicatedBackend::clear_state()
{
  // clear pushing/pulling maps
  pushing.clear();
  pulling.clear();
  pull_from_peer.clear();
}

void ReplicatedBackend::on_change(ObjectStore::Transaction *t)
{
  dout(10) << __func__ << dendl;
  // clear temp
  for (set<hobject_t>::iterator i = temp_contents.begin();
       i != temp_contents.end();
       ++i) {
    dout(10) << __func__ << ": Removing oid "
	     << *i << " from the temp collection" << dendl;
    t->remove(get_temp_coll(t), *i);
  }
  temp_contents.clear();
  clear_state();
}

coll_t ReplicatedBackend::get_temp_coll(ObjectStore::Transaction *t)
{
  if (temp_created)
    return temp_coll;
  if (!osd->store->collection_exists(temp_coll))
      t->create_collection(temp_coll);
  temp_created = true;
  return temp_coll;
}

void ReplicatedBackend::on_flushed()
{
  if (have_temp_coll() &&
      !osd->store->collection_empty(get_temp_coll())) {
    vector<hobject_t> objects;
    osd->store->collection_list(get_temp_coll(), objects);
    derr << __func__ << ": found objects in the temp collection: "
	 << objects << ", crashing now"
	 << dendl;
    assert(0 == "found garbage in the temp collection");
  }
}


int ReplicatedBackend::objects_list_partial(
  const hobject_t &begin,
  int min,
  int max,
  snapid_t seq,
  vector<hobject_t> *ls,
  hobject_t *next)
{
  vector<ghobject_t> objects;
  ghobject_t _next;
  int r = osd->store->collection_list_partial(
    coll,
    begin,
    min,
    max,
    seq,
    &objects,
    &_next);
  ls->reserve(objects.size());
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    assert(i->is_degenerate());
    ls->push_back(i->hobj);
  }
  assert(_next.is_degenerate());
  *next = _next.hobj;
  return r;
}

int ReplicatedBackend::objects_list_range(
  const hobject_t &start,
  const hobject_t &end,
  snapid_t seq,
  vector<hobject_t> *ls)
{
  vector<ghobject_t> objects;
  int r = osd->store->collection_list_range(
    coll,
    start,
    end,
    seq,
    &objects);
  ls->reserve(objects.size());
  for (vector<ghobject_t>::iterator i = objects.begin();
       i != objects.end();
       ++i) {
    assert(i->is_degenerate());
    ls->push_back(i->hobj);
  }
  return r;
}

int ReplicatedBackend::objects_get_attr(
  const hobject_t &hoid,
  const string &attr,
  bufferlist *out)
{
  bufferptr bp;
  int r = osd->store->getattr(
    coll,
    hoid,
    attr.c_str(),
    bp);
  if (r >= 0 && out) {
    out->clear();
    out->push_back(bp);
  }
  return r;
}

/* 
 * pg lock may or may not be held
 */
void ReplicatedBackend::be_scan_list(
  ScrubMap &map, vector<hobject_t> &ls, bool deep,
  ThreadPool::TPHandle &handle)
{
  dout(10) << "_scan_list scanning " << ls.size() << " objects"
           << (deep ? " deeply" : "") << dendl;
  int i = 0;
  for (vector<hobject_t>::iterator p = ls.begin(); 
       p != ls.end(); 
       ++p, i++) {
    handle.reset_tp_timeout();
    hobject_t poid = *p;

    struct stat st;
    int r = osd->store->stat(coll, poid, &st, true);
    if (r == 0) {
      ScrubMap::object &o = map.objects[poid];
      o.size = st.st_size;
      assert(!o.negative);
      osd->store->getattrs(coll, poid, o.attrs);

      // calculate the CRC32 on deep scrubs
      if (deep) {
        bufferhash h, oh;
        bufferlist bl, hdrbl;
        int r;
        __u64 pos = 0;
        while ( (r = osd->store->read(coll, poid, pos,
                                       cct->_conf->osd_deep_scrub_stride, bl,
		                      true)) > 0) {
	  handle.reset_tp_timeout();
          h << bl;
          pos += bl.length();
          bl.clear();
        }
	if (r == -EIO) {
	  dout(25) << "_scan_list  " << poid << " got "
		   << r << " on read, read_error" << dendl;
	  o.read_error = true;
	}
        o.digest = h.digest();
        o.digest_present = true;

        bl.clear();
        r = osd->store->omap_get_header(coll, poid, &hdrbl, true);
        if (r == 0) {
          dout(25) << "CRC header " << string(hdrbl.c_str(), hdrbl.length())
             << dendl;
          ::encode(hdrbl, bl);
          oh << bl;
          bl.clear();
        } else if (r == -EIO) {
	  dout(25) << "_scan_list  " << poid << " got "
		   << r << " on omap header read, read_error" << dendl;
	  o.read_error = true;
	}

        ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
          coll, poid);
        assert(iter);
	uint64_t keys_scanned = 0;
        for (iter->seek_to_first(); iter->valid() ; iter->next()) {
	  if (cct->_conf->osd_scan_list_ping_tp_interval &&
	      (keys_scanned % cct->_conf->osd_scan_list_ping_tp_interval == 0)) {
	    handle.reset_tp_timeout();
	  }
	  ++keys_scanned;

          dout(25) << "CRC key " << iter->key() << " value "
            << string(iter->value().c_str(), iter->value().length()) << dendl;

          ::encode(iter->key(), bl);
          ::encode(iter->value(), bl);
          oh << bl;
          bl.clear();
        }
	if (iter->status() == -EIO) {
	  dout(25) << "_scan_list  " << poid << " got "
		   << r << " on omap scan, read_error" << dendl;
	  o.read_error = true;
	  break;
	}

        //Store final calculated CRC32 of omap header & key/values
        o.omap_digest = oh.digest();
        o.omap_digest_present = true;
      }

      dout(25) << "_scan_list  " << poid << dendl;
    } else if (r == -ENOENT) {
      dout(25) << "_scan_list  " << poid << " got " << r << ", skipping" << dendl;
    } else if (r == -EIO) {
      dout(25) << "_scan_list  " << poid << " got " << r << ", read_error" << dendl;
      ScrubMap::object &o = map.objects[poid];
      o.read_error = true;
    } else {
      derr << "_scan_list got: " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}
