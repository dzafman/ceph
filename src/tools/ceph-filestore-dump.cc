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

namespace po = boost::program_options;
using namespace std;

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <sys/socket.h>
#include <linux/un.h>
#include <unistd.h>
#include <string.h>

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/config.h"
#include "tools/common.h"

#include "include/compat.h"
#include "include/assert.h"

using std::vector;

void do_status(CephToolCtx *ctx, bool shutdown = false);

#if 0
static void usage()
{
  cout << "usage:\n";
  cout << " ceph-filestore-dump <osd-path> <pgid> [info|log]\n";
#if 0
  generic_client_usage(); // Will exit()
#else
  exit(1);
#endif
}
#endif

static void invalid_path(string &path)
{
  cout << "Invalid path to osd store specified: " << path << "\n";
  exit(1);
}

#if 0
static void parse_cmd_args(vector<const char*> &args, string &path, string &pgid, string &type)
{
  std::vector<const char*>::iterator i;
  std::string val;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
    } else {
      break;
    }
  }
  if (i == args.end())
    usage();
  path = *i++;
  if (i == args.end())
    usage();
  pgid = *i++;
  if (i == args.end())
    usage();
  type = *i++;
  if (type != "info" && type != "log")
    usage();
  if (i != args.end())
    usage();
}
#endif

int main(int argc, const char **argv)
{
  std::string in_file, out_file;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  string path, pgid, type;

  namespace po = boost::program_options;
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("path", po::value<string>(&path), "")
    ("pgid", po::value<string>(&pgid), "")
    ("type", po::value<string>(&type), "")
    ;

  po::positional_options_description positionalOptions;
  positionalOptions.add("path",1);
  positionalOptions.add("pgid",1);
  positionalOptions.add("type",1); //info or log

  po::variables_map vm;
  po::parsed_options parsed = po::command_line_parser(argc, argv)
						.options(desc)
						.positional(positionalOptions)
						.run();
  po::store(parsed, vm);
  //po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  // initialize globals
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  if (path.length() == 0 || pgid.length() == 0 ||
    (type != "info" && type != "log")) {
    cerr << "Invalid params" << std::endl;
    exit(1);
  }

  //Verify that path really is an osd store
  struct stat st;
  if (::stat(path.c_str(), &st) == -1) {
     perror("path");
     invalid_path(path);
  }
  if (!S_ISDIR(st.st_mode)) {
    invalid_path(path);
  }
  string check = path + "/whoami";
  if (::stat(check.c_str(), &st) == -1) {
     perror("whoami");
     invalid_path(path);
  }
  if (!S_ISREG(st.st_mode)) {
    invalid_path(path);
  }
  check = path + "/current";
  if (::stat(check.c_str(), &st) == -1) {
     perror("current");
     invalid_path(path);
  }
  if (!S_ISDIR(st.st_mode)) {
    invalid_path(path);
  }

  path = path + "/current/meta";

  //CephToolCtx *ctx = ceph_tool_common_init(mode, concise);
  //if (!ctx) {
  //  derr << "ceph_tool_common_init failed." << dendl;
  //  return 1;
  //}

  int ret = 0;
  cout << "args path " + path + " pgid " + pgid + " type " + type + "\n";

  //if (ceph_tool_common_shutdown(ctx))
  //  ret = 1;
  return ret;
}
