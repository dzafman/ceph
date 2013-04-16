// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <stdlib.h>
#include <fstream>

#include "common/Formatter.h"

#include "global/global_init.h"
#include "os/ObjectStore.h"
#include "os/FileStore.h"
#include "common/perf_counters.h"
#include "common/errno.h"
#include "osd/PG.h"
#include "osd/OSD.h"

namespace po = boost::program_options;
using namespace std;

enum {
    TYPE_NONE = 0,
    TYPE_PG_BEGIN = 0x11111111,
    TYPE_PG_END = 0x22222222,
    TYPE_OBJECT_BEGIN = 0x33333333,
    TYPE_OBJECT_END = 0x44444444,
    TYPE_DATA = 0x55555555,
    TYPE_SNAPS = 0x66666666,
    TYPE_ATTRS = 0x77777777,
    TYPE_OMAP_HDR = 0x88888888,
    TYPE_OMAP = 0x99999999,
    TYPE_PG_METADATA = 0xaaaaaaaa,
};

typedef uint32_t sectiontype_t;
typedef uint32_t mymagic_t;
typedef int64_t mysize_t;
//const ssize_t max_read = 1024 * 1024;
const ssize_t max_read = 1 * 1024;	//FIX AFTER TESTING
const mymagic_t themagic = 0xdeadbeef;
const int fd_none = INT_MIN;

//The first sizeof(super_header) bytes are a fixed
//portion of the export output.  This includes the overall
//version number, and size of header and footer.
//THIS STRUCTURE CAN NOT CHANGE.  If it needs to a
//the version can be bumped and then anything
//can be added to the export format.
struct super_header {
  static const uint32_t super_magic = 0xcef4ace5;
  static const uint32_t super_ver = 1;
  uint32_t magic;
  uint32_t version;
  uint32_t header_size;
  uint32_t footer_size;

  int read_super();

  void encode(bufferlist& bl) const {
    ::encode(magic, bl);
    ::encode(version, bl);
    ::encode(header_size, bl);
    ::encode(footer_size, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(magic, bl);
    ::decode(version, bl);
    ::decode(header_size, bl);
    ::decode(footer_size, bl);
  }
};

struct header {
  sectiontype_t type;
  mysize_t size;
  header(sectiontype_t type, mysize_t size) :
    type(type), size(size) { }
  header(): type(0), size(0) { }

  int get_header();

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(type, bl);
    ::encode(size, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(type, bl);
    ::decode(size, bl);
    DECODE_FINISH(bl);
  }
};

struct footer {
  mymagic_t magic;
  footer() : magic(themagic) { }

  int get_footer();

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(magic, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(magic, bl);
    DECODE_FINISH(bl);
  }
};

struct pg_begin {
  pg_t pgid;

  pg_begin(pg_t pg): pgid(pg) { }
  pg_begin() { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(pgid, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(pgid, bl);
    DECODE_FINISH(bl);
  }
};

struct object_begin {
  hobject_t hoid;
  object_begin(hobject_t &hoid): hoid(hoid) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(hoid, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(hoid, bl);
    DECODE_FINISH(bl);
  }
};

struct data {
  uint64_t offset;
  uint64_t len;
  bufferlist databl;
  data(uint64_t offset, uint64_t len, bufferlist bl):
     offset(offset), len(len), databl(bl) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(offset, bl);
    ::encode(len, bl);
    ::encode(databl, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(offset, bl);
    ::decode(len, bl);
    ::decode(databl, bl);
    DECODE_FINISH(bl);
  }
};

struct snaps_section {
  bufferlist data;
  snaps_section(bufferlist data) : data(data) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};

struct attr_section {
  bufferlist data;
  attr_section(bufferlist data) : data(data) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(data, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(data, bl);
    DECODE_FINISH(bl);
  }
};

struct omap_hdr_section {
  bufferlist hdr;
  omap_hdr_section(bufferlist hdr) : hdr(hdr) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(hdr, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(hdr, bl);
    DECODE_FINISH(bl);
  }
};

struct omap_section {
  map<string, bufferlist> omap;
  omap_section(map<string, bufferlist> omap) :
    omap(omap) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(omap, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(omap, bl);
    DECODE_FINISH(bl);
  }
};

struct metadata_section {
  epoch_t map_epoch;
  pg_info_t info;
  pg_log_t log;
  bufferlist collattr;
  metadata_section(epoch_t map_epoch, pg_info_t info, pg_log_t log,
      bufferlist collattr):
    map_epoch(map_epoch), info(info), log(log), collattr(collattr) { }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(map_epoch, bl);
    ::encode(info, bl);
    ::encode(log, bl);
    ::encode(collattr, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator& bl) {
    DECODE_START(1, bl);
    ::decode(map_epoch, bl);
    ::decode(info, bl);
    ::decode(log, bl);
    ::decode(collattr, bl);
    DECODE_FINISH(bl);
  }
};

hobject_t infos_oid;
hobject_t biginfo_oid, log_oid;

int file_fd = fd_none;
bool debug;
super_header sh;

template <typename T>
int write_section(sectiontype_t type, const T& obj, int fd) {
  // with error returns as appropriate!
  //catch exceptions?
  bufferlist blhdr, bl, blftr;
  obj.encode(bl);
  header hdr(type, bl.length());
  hdr.encode(blhdr);
  footer ft;
  ft.encode(blftr);

  blhdr.write_fd(fd);
  bl.write_fd(fd);
  blftr.write_fd(fd);
  return 0;
}

int write_simple(sectiontype_t type, int fd)
{
  // with error returns as appropriate!
  //catch exceptions?
  bufferlist hbl;

  header hdr(type, 0);
  hdr.encode(hbl);
  hbl.write_fd(fd);
  return 0;
}

static void
corrupt()
{
  cout << "Corrupt input for import" << std::endl;
}

static void invalid_path(string &path)
{
  cout << "Invalid path to osd store specified: " << path << "\n";
  exit(1);
}

int get_log(ObjectStore *fs, coll_t coll, pg_t pgid, const pg_info_t &info,
   PG::IndexedLog &log, pg_missing_t &missing)
{ 
  PG::OndiskLog ondisklog;
  try {
    ostringstream oss;
    PG::read_log(fs, coll, log_oid, info, ondisklog, log, missing, oss);
    if (debug && oss.str().size())
      cerr << oss.str() << std::endl;
  }
  catch (const buffer::error &e) {
    cout << "read_log threw exception error", e.what();
    return 1;
  }
  return 0;
}

//Based on RemoveWQ::_process()
void remove_coll(ObjectStore *store, const coll_t &coll)
{
  OSDriver driver(
    store,
    coll_t(),
    OSD::make_snapmapper_oid());
  SnapMapper mapper(&driver, 0, 0, 0);

  vector<hobject_t> objects;
  hobject_t max;
  int r = 0;
  int64_t num = 0;
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  cout << "remove_coll " << coll << std::endl;
  while (!max.is_max()) {
    r = store->collection_list_partial(coll, max, 200, 300, 0, &objects, &max);
    if (r < 0)
      return;
    for (vector<hobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i, ++num) {

      OSDriver::OSTransaction _t(driver.get_transaction(t));
      cout << "remove " << *i << std::endl;
      int r = mapper.remove_oid(*i, &_t);
      if (r != 0 && r != -ENOENT) {
        assert(0);
      }

      t->remove(coll, *i);
      if (num >= 30) {
        store->apply_transaction(*t);
        delete t;
        t = new ObjectStore::Transaction;
        num = 0;
      }
    }
  }
  t->remove_collection(coll);
  store->apply_transaction(*t);
  delete t;
}

//Based on part of OSD::load_pgs()
int finish_remove_pgs(ObjectStore *store, uint64_t *next_removal_seq)
{
  vector<coll_t> ls;
  int r = store->list_collections(ls);
  if (r < 0) {
    cout << "finish_remove_pgs: failed to list pgs: " << cpp_strerror(-r) << std::endl;
    return r;
  }

  for (vector<coll_t>::iterator it = ls.begin();
       it != ls.end();
       ++it) {
    pg_t pgid;
    snapid_t snap;

    if (it->is_temp(pgid)) {
      cout << "finish_remove_pgs " << *it << " clearing temp" << std::endl;
      OSD::clear_temp(store, *it);
      continue;
    }

    if (it->is_pg(pgid, snap)) {
      continue;
    }

    uint64_t seq;
    if (it->is_removal(&seq, &pgid)) {
      if (seq >= *next_removal_seq)
	*next_removal_seq = seq + 1;
      cout << "finish_remove_pgs removing " << *it << ", seq is "
	       << seq << " pgid is " << pgid << std::endl;
      remove_coll(store, *it);
      continue;
    }

    //cout << "finish_remove_pgs ignoring unrecognized " << *it << std::endl;
  }
  return 0;
}

int initiate_new_remove_pg(ObjectStore *store, pg_t r_pgid, uint64_t *next_removal_seq)
{
  ObjectStore::Transaction *rmt = new ObjectStore::Transaction;

  if (store->collection_exists(coll_t(r_pgid))) {
      coll_t to_remove = coll_t::make_removal_coll((*next_removal_seq)++, r_pgid);
      cout << "collection rename " << coll_t(r_pgid) << " to " << to_remove << std::endl;
      rmt->collection_rename(coll_t(r_pgid), to_remove);
  } else {
    return ENOENT;
  }

  cout << "remove " << coll_t::META_COLL << " " << log_oid.oid << std::endl;
  rmt->remove(coll_t::META_COLL, log_oid);
  cout << "remove " << coll_t::META_COLL << " " << biginfo_oid.oid << std::endl;
  rmt->remove(coll_t::META_COLL, biginfo_oid);

  store->apply_transaction(*rmt);

  return 0;
}

int header::get_header()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sh.header_size);
  if (bytes != sh.header_size) {
    corrupt();
    return 1;
  }

  decode(ebliter);

  return 0;
}

int footer::get_footer()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sh.footer_size);
  if (bytes != sh.footer_size) {
    corrupt();
    return 1;
  }

  decode(ebliter);

  if (magic != themagic) {
    corrupt();
    if (debug)
      cout << "Bad footer magic" << std::endl;
    return 1;
  }

  return 0;
}

int write_info(ObjectStore::Transaction &t, epoch_t epoch, pg_info_t &info)
{
  //Empty for this
  interval_set<snapid_t> snap_collections; // obsolete
  map<epoch_t,pg_interval_t> past_intervals;
  coll_t coll(info.pgid);

  return PG::_write_info(t, epoch,
    info, coll,
    past_intervals,
    snap_collections,
    infos_oid,
    0,      //Get version (struct_v)
    true);
}

void write_log(ObjectStore::Transaction &t, pg_log_t &log)
{
  map<eversion_t, hobject_t> divergent_priors;
  PG::_write_log(t, log, log_oid, divergent_priors);
}

void write_pg(ObjectStore::Transaction &t, epoch_t epoch, pg_info_t &info, pg_log_t &log)
{
  write_info(t, epoch, info);
  write_log(t, log);
}

int export_file(ObjectStore *store, coll_t cid, hobject_t &obj)
{
  struct stat st;
  mysize_t total;
  ostringstream objname;
  footer ft;

  int ret = store->stat(cid, obj, &st);
  if (ret < 0)
    return ret;

  objname << obj;
  if (debug && file_fd != STDOUT_FILENO)
    cout << "objname=" << objname.str() << std::endl;

  total = st.st_size;
  if (debug && file_fd != STDOUT_FILENO)
    cout << "size=" << total << std::endl;

  object_begin objb(obj);
  ret = write_section<object_begin>(TYPE_OBJECT_BEGIN, objb, file_fd);

  uint64_t offset = 0;
  bufferlist rawdatabl, databl;
  while(total > 0) {
    rawdatabl.clear();
    databl.clear();
    mysize_t len = max_read;
    if (len > total)
      len = total;

    ret = store->read(cid, obj, offset, len, rawdatabl);
    if (ret < 0)
      return ret;
    if (ret == 0)
      return -EINVAL;
    total -= ret;

    data dblock(offset, len, rawdatabl);
    ret = write_section<data>(TYPE_DATA, dblock, file_fd);

    if (debug && file_fd != STDOUT_FILENO)
      cout << "data section offset=" << offset << " len=" << len << std::endl;
    offset += ret;
  }

  //Handle snapshots for this object
  databl.clear();
  store->getattr(cid, obj, SS_ATTR, databl);
  if (databl.length() > 0) {
    snaps_section ss(databl);
    ret = write_section<snaps_section>(TYPE_SNAPS, ss, file_fd);
    if (ret)
      return ret;

    if (debug && file_fd != STDOUT_FILENO)
      cout << "snapshot data length " << databl.length() << std::endl;
  }

  //Handle attrs for this object
  databl.clear();
  store->getattr(cid, obj, OI_ATTR, databl);
  if (databl.length() > 0) {
    attr_section as(databl);
    ret = write_section<attr_section>(TYPE_ATTRS, as, file_fd);
    if (ret)
      return ret;

    if (debug && file_fd != STDOUT_FILENO)
      cout << "attrs data length " << databl.length() << std::endl;
  }

  //Handle omap information
  databl.clear();
  bufferlist hdrbuf;
  map<string, bufferlist> out;
  ret = store->omap_get(cid, obj, &hdrbuf, &out);
  if (ret < 0)
    return ret;

  omap_hdr_section ohs(hdrbuf);
  ret = write_section<omap_hdr_section>(TYPE_OMAP_HDR, ohs, file_fd);
  if (ret)
    return ret;

  if (out.size() > 0) {
    omap_section oms(out);
    ret = write_section<omap_section>(TYPE_OMAP, oms, file_fd);
    if (ret)
      return ret;

    if (debug && file_fd != STDOUT_FILENO)
      cout << "omap map size " << out.size() << std::endl;
  }

  ret = write_simple(TYPE_OBJECT_END, file_fd);
  if (ret)
    return ret;

  return 0;
}

int export_files(ObjectStore *store, coll_t coll)
{
  vector<hobject_t> objects;
  hobject_t max;
  int r = 0;

  while (!max.is_max()) {
    r = store->collection_list_partial(coll, max, 200, 300, 0, &objects, &max);
    if (r < 0)
      return r;
    for (vector<hobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      r = export_file(store, coll, *i);
      if (r < 0)
        return r;
    }
  }
  return 0;
}

void get_section(bufferlist &ebl, bufferlist::iterator &ebliter)
{
  int bytes;
  ssize_t size;

  bytes = ebl.read_fd(file_fd, sizeof(size));
  if (bytes != sizeof(size))
    corrupt();

  ::decode(size, ebliter);

  do {
    ssize_t read_len = size;
    if (read_len > max_read)
      read_len = max_read;
    
    bytes = ebl.read_fd(file_fd, read_len);
    if (bytes == 0)
      corrupt();
    size -= bytes;
  } while(size > 0);
  assert(size == 0);
}

int import_files(ObjectStore *store, coll_t coll)
{
  do {
    bufferlist ebl;
    bufferlist::iterator ebliter = ebl.begin();
    mysize_t hobjdatlen;
    ssize_t bytes;
    hobject_t hobj;
    ObjectStore::Transaction tran;
    ObjectStore::Transaction *t = &tran;
  
    bytes = ebl.read_fd(file_fd, sizeof(hobjdatlen));
    //See if are at EOF
    if (bytes == 0)
      break;
    if (bytes != sizeof(hobjdatlen))
      corrupt();
  
    ::decode(hobjdatlen, ebliter);
  
    ssize_t read_len = hobjdatlen;
    if (read_len > max_read)
      read_len = max_read;
  
    bytes = ebl.read_fd(file_fd, read_len);
    if (bytes != read_len)
      corrupt();
  
    ::decode(hobj, ebliter);
  
    t->touch(coll, hobj);
    store->apply_transaction(*t);

    if (debug) {
      ostringstream objname;
      objname << hobj;
      cout << std::endl;
      cout << "filename=" << objname.str() << std::endl;
    }
  
    //CREATE NEW FILE AND WRITE REST OF ebl
    bufferptr bp = ebliter.get_current_ptr();
    if (debug)
      cout << "data=" << string(bp.c_str(), bp.length());
    mysize_t size = bp.length();
    uint64_t off = 0;
    bufferlist databl;
    databl.push_front(bp);
    t->write(coll, hobj, off, size,  databl);
    off += size;
  
    hobjdatlen -= bytes;
  
    while(hobjdatlen > 0) {
      bufferlist buf;
      mysize_t read_len = hobjdatlen;
      if (read_len > max_read)
        read_len = max_read;
  
      bytes = buf.read_fd(file_fd, read_len);
      if (bytes == 0)
        corrupt();
      hobjdatlen -= bytes;
      assert(bytes == buf.length());
  
      //Write buf to file
      if (debug)
        cout << string(buf.c_str(), bytes);
      t->write(coll, hobj, off, bytes,  buf);
      size += bytes;
      off += bytes;
    }
    if (debug) {
      cout << std::endl;
      cout << "size=" << size << std::endl;
    }

    //Get snapshots
    {
      bufferlist bl, vbl;
      bufferlist::iterator bliter = bl.begin();

      get_section(bl, bliter);

      bufferptr bp = bliter.get_current_ptr();
      vbl.push_front(bp);

      t->setattr(coll, hobj, SS_ATTR, vbl);
    }

    //Get attributes
    {
      bufferlist bl, vbl;
      bufferlist::iterator bliter = bl.begin();

      get_section(bl, bliter);
  
      bufferptr bp = bliter.get_current_ptr();
      vbl.push_front(bp);

      t->setattr(coll, hobj, OI_ATTR, vbl);
    }

    {
      bufferlist ebl;
      bufferlist::iterator ebliter = ebl.begin();
      bufferlist hdrbuf;
      map<string, bufferlist> out;
  
      get_section(ebl, ebliter);

      ::decode(hdrbuf, ebliter);
      if (debug)
        cout << "header=" << string(hdrbuf.c_str(), hdrbuf.length())
          << std::endl;
      ::decode(out, ebliter);
      for (map<string, bufferlist>::iterator i = out.begin();
         i != out.end();
         ++i) {
        if (debug)
          cout << "key=" << i->first 
             << " val=" << string(i->second.c_str(), i->second.length())
             << std::endl;
      }
    }
    store->apply_transaction(*t);
  } while(true);

  return 0;
}

//Write super_header with its fixed 16 byte length
void write_super()
{
  bufferlist superbl;
  super_header sh;
  footer ft;

  header hdr(TYPE_NONE, 0);
  hdr.encode(superbl);

  sh.magic = super_header::super_magic;
  sh.version = super_header::super_ver;
  sh.header_size = superbl.length();
  superbl.clear();
  ft.encode(superbl);
  sh.footer_size = superbl.length();
  superbl.clear();

  sh.encode(superbl);
  assert(sizeof(super_header) == superbl.length());
  superbl.write_fd(file_fd);
}

int do_export(ObjectStore *fs, coll_t coll, pg_t pgid, pg_info_t &info,
    epoch_t map_epoch)
{
  PG::IndexedLog log;
  pg_missing_t missing;
  bufferlist collattrbl;

  int ret = get_log(fs, coll, pgid, info, log, missing);
  if (ret > 0)
      return ret;

  ret = fs->collection_getattr(coll, "info", collattrbl);
  if (ret < 0)
    return ret;

  write_super();

  pg_begin pgb(pgid);
  ret = write_section<pg_begin>(TYPE_PG_BEGIN, pgb, file_fd);
  if (ret)
    return ret;

  export_files(fs, coll);

  metadata_section ms(map_epoch, info, log, collattrbl);
  ret = write_section<metadata_section>(TYPE_PG_METADATA, ms, file_fd);
  if (ret)
    return ret;

  ret = write_simple(TYPE_PG_END, file_fd);
  if (ret)
    return ret;

  return 0;
}

int super_header::read_super()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sizeof(super_header));
  if (bytes != sizeof(super_header)) {
    corrupt();
    return 1;
  }

  decode(ebliter);

  return 0;
}

int read_section(int fd, sectiontype_t *type, bufferlist &bl)
{
  header hdr;
  ssize_t bytes;

  int ret = hdr.get_header();
  if (ret)
    return ret;

  *type = hdr.type;

  bl.clear();
  bytes = bl.read_fd(fd, hdr.size);
  if (bytes != hdr.size) {
    corrupt();
    return 1;
  }

  if (hdr.size > 0) {
    footer ft;
    ret = ft.get_footer();
    if (ret)
      return ret;
  }

  return 0;
}

int get_object(ObjectStore *store, coll_t coll)
{
  bufferlist ebl;
  bool done = false;
  while(!done) {
    sectiontype_t type;
    int ret = read_section(file_fd, &type, ebl);
    if (ret)
      return ret;

    cout << "\tdo_object: Section type " << hex << type << std::endl;
    switch(type) {
    case TYPE_DATA:
    case TYPE_SNAPS:
    case TYPE_ATTRS:
    case TYPE_OMAP_HDR:
    case TYPE_OMAP:
      cout << "\t\tsection size " << ebl.length() << std::endl;
      break;
    case TYPE_OBJECT_END:
      done = true;
      break;
    case TYPE_PG_METADATA:
    case TYPE_PG_END:
    case TYPE_OBJECT_BEGIN:
      //Didn't see OBJECT_END
      corrupt();
      return EINVAL;
    }
  }
  return 0;
}

int do_import(ObjectStore *store)
{
  bufferlist ebl;
  pg_info_t info;
  PG::IndexedLog log;
  //epoch_t epoch;

  uint64_t next_removal_seq = 0;	//My local seq
  finish_remove_pgs(store, &next_removal_seq);

  int ret = sh.read_super();
  if (ret)
    return ret;

  if (sh.magic != super_header::super_magic) {
    cout << "Invalid magic number" << std::endl;
    return EINVAL;
  }

  if (sh.version > super_header::super_ver) {
    cout << "Can't handle export format version=" << sh.version << std::endl;
    return EINVAL;
  }

  //First section must be TYPE_PG_BEGIN
  sectiontype_t type;
  ret = read_section(file_fd, &type, ebl);
  if (type != TYPE_PG_BEGIN) {
    corrupt();
    return EINVAL;
  }

  bufferlist::iterator ebliter = ebl.begin();
  pg_begin pgb;
  pgb.decode(ebliter);
  pg_t pgid = pgb.pgid;
  
  log_oid = OSD::make_pg_log_oid(pgid);
  biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

  //Check for PG already present.
  coll_t coll(pgid);
  if (store->collection_exists(coll)) {
    cout << "pgid " << pgid << " already exists" << std::endl;
    return 1;
  }

  //Switch to collection which will be removed automatically if
  //this program is interupted.
  coll_t rmcoll = coll_t::make_removal_coll(next_removal_seq, pgid);
  ObjectStore::Transaction *t = new ObjectStore::Transaction;
  t->create_collection(rmcoll);
  store->apply_transaction(*t);
  delete t;

  cout << "Importing pgid " << pgid << std::endl;

  bool done = false;
  while(!done) {
    ret = read_section(file_fd, &type, ebl);
    if (ret)
      return ret;

    cout << "do_import: Section type " << hex << type << std::endl;
    switch(type) {
    case TYPE_OBJECT_BEGIN:
      get_object(store, rmcoll);
      break;
    case TYPE_PG_METADATA:
      //get_pg_metadata();
      break;
    case TYPE_PG_END:
      done = true;
      break;
    }
  }

  return 0;

  //XXX: Should somehow write everything to a temporary location
  //import_files();

#if 0
  ::decode(epoch, ebliter);

  info.decode(ebliter);

  log.decode(ebliter);
 

  ObjectStore::Transaction *t = new ObjectStore::Transaction;

  write_pg(*t, epoch, info, log);
  fs->apply_transaction(*t);
  delete t;

  t = new ObjectStore::Transaction;

  {
    bufferlist ebl, infobl;
    bufferlist::iterator ebliter = ebl.begin();
    
    get_section(ebl, ebliter);

    ebliter.copy(ebliter.get_remaining(), infobl);

    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    t->collection_setattr(coll, "info", infobl);

    fs->apply_transaction(*t);
    delete t;
  }

  import_files(fs, coll);

  //XXX: Rename pg into place?  I don't think this can be a single rename

#if DIAGNOSTIC
  cout << "epoch " << epoch << std::endl;
  formatter->open_object_section("info");
  info.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
  
  formatter->open_object_section("log");
  log.dump(formatter);
  formatter->close_section();
  formatter->flush(cout);
  cout << std::endl;
#endif
  return 0;
#endif
}

int main(int argc, char **argv)
{
  string fspath, jpath, pgidstr, type, file;
  Formatter *formatter = new JSONFormatter(true);

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("filestore-path", po::value<string>(&fspath),
     "path to filestore directory, mandatory")
    ("journal-path", po::value<string>(&jpath),
     "path to journal, mandatory")
    ("pgid", po::value<string>(&pgidstr),
     "PG id, mandatory")
    ("type", po::value<string>(&type),
     "Type which is 'info' or 'log', mandatory")
    ("file", po::value<string>(&file),
     "path of file to export or import")
    ("debug", "Enable diagnostic output to stderr")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store( parsed, vm);
  try {
    po::notify(vm);
  }
  catch(...) {
    cout << desc << std::endl;
    exit(1);
  }
     
  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  if (!vm.count("filestore-path")) {
    cout << "Must provide filestore-path" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  if (!vm.count("journal-path")) {
    cout << "Must provide journal-path" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  if (!vm.count("type")) {
    cout << "Must provide type ('info' or 'log')" << std::endl
	 << desc << std::endl;
    return 1;
  } 
  if (type != "import" && !vm.count("pgid")) {
    cout << "Must provide pgid" << std::endl
	 << desc << std::endl;
    return 1;
  } 

  file_fd = fd_none;
  if (type == "export") {
    if (!vm.count("file")) {
      file_fd = STDOUT_FILENO;
    } else {
      file_fd = open(file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
    }
  } else if (type == "import") {
    if (!vm.count("file")) {
      file_fd = STDIN_FILENO;
    } else {
      file_fd = open(file.c_str(), O_RDONLY);
    }
  }

  if (vm.count("file") && file_fd == fd_none) {
    cout << "--file option only applies to import or export" << std::endl;
    return 1;
  }

  if (file_fd != fd_none && file_fd < 0) {
    perror("open");
    return 1;
  }
  
  if ((fspath.length() == 0 || jpath.length() == 0) ||
      (type != "info" && type != "log" && type != "remove" && type != "export" && type != "import") ||
      (type != "import" && pgidstr.length() == 0)) {
    cerr << "Invalid params" << std::endl;
    exit(1);
  }

  if (type == "import" && pgidstr.length()) {
    cerr << "--pgid option invalid with import" << std::endl;
    exit(1);
  }

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
    parsed.options, po::include_positional);
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  //Suppress derr() output to stderr by default
  if (!vm.count("debug")) {
    close(STDERR_FILENO);
    (void)open("/dev/null", O_WRONLY);
    debug = false;
  } else {
    debug = true;
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_OSD,
    CODE_ENVIRONMENT_UTILITY, 0);
    //CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);
  g_conf = g_ceph_context->_conf;

  //Verify that fspath really is an osd store
  struct stat st;
  if (::stat(fspath.c_str(), &st) == -1) {
     perror("fspath");
     invalid_path(fspath);
  }
  if (!S_ISDIR(st.st_mode)) {
    invalid_path(fspath);
  }
  string check = fspath + "/whoami";
  if (::stat(check.c_str(), &st) == -1) {
     perror("whoami");
     invalid_path(fspath);
  }
  if (!S_ISREG(st.st_mode)) {
    invalid_path(fspath);
  }
  check = fspath + "/current";
  if (::stat(check.c_str(), &st) == -1) {
     perror("current");
     invalid_path(fspath);
  }
  if (!S_ISDIR(st.st_mode)) {
    invalid_path(fspath);
  }

  pg_t pgid;
  if (pgidstr.length() && !pgid.parse(pgidstr.c_str())) {
    cout << "Invalid pgid '" << pgidstr << "' specified" << std::endl;
    exit(1);
  }

  ObjectStore *fs = new FileStore(fspath, jpath);
  
  int r = fs->mount();
  if (r < 0) {
    if (r == -EBUSY) {
      cout << "OSD has the store locked" << std::endl;
    } else {
      cout << "Mount failed with '" << cpp_strerror(-r) << "'" << std::endl;
    }
    return 1;
  }

  int ret = 0;
  vector<coll_t> ls;
  vector<coll_t>::iterator it;
  infos_oid = OSD::make_infos_oid();

  if (type == "import") {
#if 0
  if (getuid() != 0 || getgid() != 0) {
    cout << "Please use sudo to import" << std::endl;
    exit(1);
  }
#endif

    ret = do_import(fs);
    goto out;
  }

  log_oid = OSD::make_pg_log_oid(pgid);
  biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

  if (type == "remove") {
    uint64_t next_removal_seq = 0;	//My local seq
    finish_remove_pgs(fs, &next_removal_seq);
    int r = initiate_new_remove_pg(fs, pgid, &next_removal_seq);
    if (r) {
      cout << "PG '" << pgid << "' not found" << std::endl;
      ret = 1;
      goto out;
    }
    finish_remove_pgs(fs, &next_removal_seq);
    cout << "Remove successful" << std::endl;
    goto out;
  }

  r = fs->list_collections(ls);
  if (r < 0) {
    cout << "failed to list pgs: " << cpp_strerror(-r) << std::endl;
    exit(1);
  }

  for (it = ls.begin(); it != ls.end(); ++it) {
    snapid_t snap;
    pg_t tmppgid;

    if (!it->is_pg(tmppgid, snap)) {
      continue;
    }

    if (tmppgid != pgid) {
      continue;
    }
    if (snap != CEPH_NOSNAP && debug) {
      cerr << "skipping snapped dir " << *it
	       << " (pg " << pgid << " snap " << snap << ")" << std::endl;
      continue;
    }

    //Found!
    break;
  }

  epoch_t map_epoch;
  if (it != ls.end()) {
  
    coll_t coll = *it;
  
    bufferlist bl;
    map_epoch = PG::peek_map_epoch(fs, coll, infos_oid, &bl);
    if (debug)
      cerr << "map_epoch " << map_epoch << std::endl;

    pg_info_t info(pgid);
    map<epoch_t,pg_interval_t> past_intervals;
    hobject_t biginfo_oid = OSD::make_pg_biginfo_oid(pgid);
    interval_set<snapid_t> snap_collections;
  
    __u8 struct_v;
    r = PG::read_info(fs, coll, bl, info, past_intervals, biginfo_oid,
      infos_oid, snap_collections, struct_v);
    if (r < 0) {
      cout << "read_info error " << cpp_strerror(-r) << std::endl;
      ret = 1;
      goto out;
    }
    if (debug)
      cerr << "struct_v " << (int)struct_v << std::endl;

    if (type == "export") {
      ret = do_export(fs, coll, pgid, info, map_epoch);
    } else if (type == "info") {
      formatter->open_object_section("info");
      info.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    } else if (type == "log") {
      PG::IndexedLog log;
      pg_missing_t missing;
      ret = get_log(fs, coll, pgid, info, log, missing);
      if (ret > 0)
          goto out;
  
      formatter->open_object_section("log");
      log.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
      formatter->open_object_section("missing");
      missing.dump(formatter);
      formatter->close_section();
      formatter->flush(cout);
      cout << std::endl;
    }
  } else {
    cout << "PG '" << pgid << "' not found" << std::endl;
    ret = 1;
  }

out:
  if (fs->umount() < 0) {
    cout << "umount failed" << std::endl;
    return 1;
  }

  return ret;
}

