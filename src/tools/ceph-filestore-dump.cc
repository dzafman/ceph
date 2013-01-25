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
#include <iostream>

#include "common/Formatter.h"

#include "global/global_init.h"
#include "os/FileStore.h"
#include "common/perf_counters.h"

namespace po = boost::program_options;
using namespace std;

#if 0
struct MorePrinting : public DetailedStatCollector::AdditionalPrinting {
  CephContext *cct;
  MorePrinting(CephContext *cct) : cct(cct) {}
  void operator()(std::ostream *out) {
    bufferlist bl;
    cct->get_perfcounters_collection()->write_json_to_buf(bl, 0);
    bl.append('\0');
    *out << bl.c_str() << std::endl;
  }
};
#endif

static void invalid_path(string &path)
{
  cout << "Invalid path to osd store specified: " << path << "\n";
  exit(1);
}

int main(int argc, char **argv)
{
  string fspath, jpath, pgid, type;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    //("op-dump-file", po::value<string>()->default_value(""),
    // "set file for dumping op details, omit for stderr")
    ("filestore-path", po::value<string>(&fspath)->required(),
     "path to filestore directory, mandatory")
    ("journal-path", po::value<string>(&jpath)->required(),
     "path to journal, mandatory")
    ("pgid", po::value<string>(&pgid)->required(),
     "PG id, mandatory")
    ("type", po::value<string>(&type)->required(),
     "Type which is 'info' or 'log'")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store(
    parsed,
    vm);
  po::notify(vm);

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
    parsed.options, po::include_positional);
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (!vm.count("filestore-path") || !vm.count("journal-path")) {
    cout << "Must provide filestore-path and journal-path" << std::endl
	 << desc << std::endl;
    return 1;
  }

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  if (fspath.length() == 0 || jpath.length() == 0 || pgid.length() == 0 ||
    (type != "info" && type != "log")) {
    cerr << "Invalid params" << std::endl;
    exit(1);
  }

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

  //CephToolCtx *ctx = ceph_tool_common_init(mode, concise);
  //if (!ctx) {
  //  derr << "ceph_tool_common_init failed." << dendl;
  //  return 1;
  //}

  int ret = 0;
  cout << "args fspath " + fspath + " jpath " + jpath + " pgid " + pgid + " type " + type + "\n";

  //if (ceph_tool_common_shutdown(ctx))
  //  ret = 1;
  return ret;
}
#if 0
  rngen_t rng;
  if (vm.count("seed"))
    rng = rngen_t(vm["seed"].as<unsigned>());

  set<pair<double, Bencher::OpType> > ops;
  ops.insert(make_pair(vm["write-ratio"].as<double>(), Bencher::WRITE));
  ops.insert(make_pair(1-vm["write-ratio"].as<double>(), Bencher::READ));

  FileStore fs(vm["filestore-path"].as<string>(),
	       vm["journal-path"].as<string>());
  fs.mkfs();
  fs.mount();

  ostream *detailed_ops = 0;
  ofstream myfile;
  if (vm["disable-detailed-ops"].as<bool>()) {
    detailed_ops = 0;
  } else if (vm["op-dump-file"].as<string>().size()) {
    myfile.open(vm["op-dump-file"].as<string>().c_str());
    detailed_ops = &myfile;
  } else {
    detailed_ops = &cerr;
  }

  std::tr1::shared_ptr<StatCollector> col(
    new DetailedStatCollector(
      1, new JSONFormatter, detailed_ops, &cout,
      new MorePrinting(g_ceph_context)));

  cout << "Creating objects.." << std::endl;
  bufferlist bl;
  for (uint64_t i = 0; i < vm["object-size"].as<unsigned>(); ++i) {
    bl.append(0);
  }

  for (uint64_t num = 0; num < vm["num-colls"].as<unsigned>(); ++num) {
    stringstream coll;
    coll << "collection_" << num;
    std::cout << "collection " << coll.str() << std::endl;
    ObjectStore::Transaction t;
    t.create_collection(coll_t(coll.str()));
    fs.apply_transaction(t);
  }
  {
    ObjectStore::Transaction t;
    t.create_collection(coll_t(string("meta")));
    fs.apply_transaction(t);
  }

  vector<std::tr1::shared_ptr<Bencher> > benchers(
    vm["num-writers"].as<unsigned>());
  for (vector<std::tr1::shared_ptr<Bencher> >::iterator i = benchers.begin();
       i != benchers.end();
       ++i) {
    set<string> objects;
    for (uint64_t num = 0; num < vm["num-objects"].as<unsigned>(); ++num) {
      unsigned col_num = num % vm["num-colls"].as<unsigned>();
      stringstream coll, obj;
      coll << "collection_" << col_num;
      obj << "obj_" << num << "_bencher_" << (i - benchers.begin());
      objects.insert(coll.str() + string("/") + obj.str());
    }
    Distribution<
      boost::tuple<string, uint64_t, uint64_t, Bencher::OpType> > *gen = 0;
    if (vm["sequential"].as<bool>()) {
      std::cout << "Using Sequential generator" << std::endl;
      gen = new SequentialLoad(
	objects,
	vm["object-size"].as<unsigned>(),
	vm["io-size"].as<unsigned>(),
	new WeightedDist<Bencher::OpType>(rng, ops)
	);
    } else {
      std::cout << "Using random generator" << std::endl;
      gen = new FourTupleDist<string, uint64_t, uint64_t, Bencher::OpType>(
	new RandomDist<string>(rng, objects),
	new Align(
	  new UniformRandom(
	    rng,
	    0,
	    vm["object-size"].as<unsigned>() - vm["io-size"].as<unsigned>()),
	  vm["offset-align"].as<unsigned>()
	  ),
	new Uniform(vm["io-size"].as<unsigned>()),
	new WeightedDist<Bencher::OpType>(rng, ops)
	);
    }

    Bencher *bencher = new Bencher(
      gen,
      col,
      new FileStoreBackend(&fs, vm["write-infos"].as<bool>()),
      vm["num-concurrent-ops"].as<unsigned>(),
      vm["duration"].as<unsigned>(),
      vm["max-ops"].as<unsigned>());

    bencher->init(objects, vm["object-size"].as<unsigned>(), &std::cout);
    cout << "Created objects..." << std::endl;
    (*i).reset(bencher);
  }

  for (vector<std::tr1::shared_ptr<Bencher> >::iterator i = benchers.begin();
       i != benchers.end();
       ++i) {
    (*i)->create();
  }
  for (vector<std::tr1::shared_ptr<Bencher> >::iterator i = benchers.begin();
       i != benchers.end();
       ++i) {
    (*i)->join();
  }

  fs.umount();
  if (vm["op-dump-file"].as<string>().size()) {
    myfile.close();
  }
  return 0;
}
#endif

