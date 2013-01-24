// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 * Copyright (C) 2010 Dreamhost
 * Copyright (C) 2013 Inktank
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


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

static void invalid_path(string &path)
{
  cout << "Invalid path to osd store specified: " << path << "\n";
  exit(1);
}

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

int main(int argc, const char **argv)
{
  std::string in_file, out_file;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  string path, pgid, type;

  // initialize globals
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  // parse user input
  parse_cmd_args(args, path, pgid, type);

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


  //CephToolCtx *ctx = ceph_tool_common_init(mode, concise);
  //if (!ctx) {
  //  derr << "ceph_tool_common_init failed." << dendl;
  //  return 1;
  //}
  signal(SIGINT, SIG_DFL);
  signal(SIGTERM, SIG_DFL);

  bufferlist outbl;
  int ret = 0;
  string output_line = "args path " + path + " pgid " + pgid + " type " + type + "\n";
  ::encode(output_line, outbl);
 
  if (ret == 0 && outbl.length()) {
    // output
    int err;
    if (out_file.empty() || out_file == "-") {
      err = outbl.write_fd(STDOUT_FILENO);
    } else {
      int out_fd = TEMP_FAILURE_RETRY(::open(out_file.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644));
      if (out_fd < 0) {
	int ret = errno;
	derr << " failed to create file '" << out_file << "': "
	     << cpp_strerror(ret) << dendl;
	return 1;
      }
      err = outbl.write_fd(out_fd);
      ::close(out_fd);
    }
    if (err) {
      derr << " failed to write " << outbl.length() << " bytes to " << out_file << ": "
	   << cpp_strerror(err) << dendl;
      ret = 1;
    }
  }

  //if (ceph_tool_common_shutdown(ctx))
  //  ret = 1;
  return ret;
}
