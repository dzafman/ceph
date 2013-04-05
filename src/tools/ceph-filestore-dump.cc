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

typedef uint64_t mysize_t;
const mysize_t max_read = 1024 * 1024;
const int fd_none = INT_MIN;

hobject_t infos_oid;
hobject_t biginfo_oid, log_oid;

int file_fd = fd_none;
bool debug;

static void
corrupt()
{
  cout << "Corrupt input for import" << std::endl;
  exit(1);
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
    0, true);
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

void write_section(bufferlist &bl)
{
  bufferlist lenbuf;

  mysize_t len = bl.length();
  ::encode(len, lenbuf);

  lenbuf.write_fd(file_fd);
  bl.write_fd(file_fd);
}

int export_file(ObjectStore *store, coll_t cid, hobject_t &obj)
{
  struct stat st;
  mysize_t total;
  ostringstream objname;

  int ret = store->stat(cid, obj, &st);
  if (ret < 0)
    return ret;

  {
    bufferlist sizebl, hobjbl;
    objname << obj;
    if (debug && file_fd != STDOUT_FILENO)
      cout << "objname=" << objname.str() << std::endl;

    total = st.st_size;
    if (debug && file_fd != STDOUT_FILENO)
      cout << "size=" << total << std::endl;

    ::encode(obj, hobjbl);
    mysize_t hobjdatlen = total + hobjbl.length();

    ::encode(hobjdatlen, sizebl);
    sizebl.write_fd(file_fd);
    hobjbl.write_fd(file_fd);
  }

  uint64_t offset = 0;
  while(total > 0) {
    mysize_t len = max_read;
    if (len > total)
      len = total;
    //XXX: If I knew how to clear a bufferlist, I wouldn't need to reallocate
    bufferlist bl(len);

    ret = store->read(cid, obj, offset, len, bl);
    if (ret < 0)
      return ret;
    if (ret == 0)
      return -EINVAL;
    total -= ret;
    offset += ret;
    bl.write_fd(file_fd);
  }

  //Handle snapshots for this object
  {
    bufferlist bl;

    store->getattr(cid, obj, SS_ATTR, bl);
    write_section(bl);
  }

  //Handle attrs for this object
  {
    bufferlist bl;

    store->getattr(cid, obj, OI_ATTR, bl);
    write_section(bl);
  }

  //Handle omap information
  bufferlist omapbuf, lenbuf;
  bufferlist hdrbuf;
  map<string, bufferlist> out;
  ret = store->omap_get(cid, obj, &hdrbuf, &out);
  if (ret < 0)
    return ret;

  ::encode(hdrbuf, omapbuf);
  ::encode(out, omapbuf);

  write_section(omapbuf);

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
  mysize_t size;

  bytes = ebl.read_fd(file_fd, sizeof(size));
  if (bytes != sizeof(size))
    corrupt();

  ::decode(size, ebliter);

  do {
    mysize_t read_len = size;
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
    mysize_t bytes;
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
  
    mysize_t read_len = hobjdatlen;
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
    bufferlist ebl;
    bufferlist::iterator ebliter = ebl.begin();
    pg_info_t info;
    PG::IndexedLog log;
    epoch_t epoch;

#if 0
    if (getuid() != 0 || getgid() != 0) {
      cout << "Please use sudo to import" << std::endl;
      exit(1);
    }
#endif

    get_section(ebl, ebliter);

    ::decode(epoch, ebliter);

    info.decode(ebliter);
    pgid = info.pgid;
    coll_t coll(pgid);
    cout << "Importing pgid " << pgid << std::endl;
    log_oid = OSD::make_pg_log_oid(pgid);
    biginfo_oid = OSD::make_pg_biginfo_oid(pgid);

    log.decode(ebliter);
 
    //XXX: Check for PG already present.  Require use to remove before import

    //XXX: Should somehow write everything to a temporary location

    ObjectStore::Transaction *t = new ObjectStore::Transaction;

    write_pg(*t, epoch, info, log);
    fs->apply_transaction(*t);
    delete t;

    t = new ObjectStore::Transaction;
    t->create_collection(coll);
    fs->apply_transaction(*t);
    delete t;

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
      PG::IndexedLog log;
      pg_missing_t missing;
      bufferlist ebl, sizebl;
      mysize_t size;
  
      ret = get_log(fs, coll, pgid, info, log, missing);
      if (ret > 0)
          goto out;
  
      ::encode(map_epoch, ebl);
      info.encode(ebl);
      log.encode(ebl);
      size = ebl.length();
      ::encode(size, sizebl);
      assert(sizebl.length() == sizeof(size));
      
      sizebl.write_fd(file_fd);
      ebl.write_fd(file_fd);

      {
        bufferlist ebl, sbl;
        mysize_t size;

        ret = fs->collection_getattr(coll, "info", ebl);
        if (ret < 0)
          goto out;

        size = ebl.length();
        ::encode(size, sbl);

        sbl.write_fd(file_fd);
        ebl.write_fd(file_fd);
      }

      export_files(fs, coll);

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

