// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/util.h"
#include "RadosDump.h"

int RadosDump::read_super()
{
  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, super_header::FIXED_LENGTH);
  if ((size_t)bytes != super_header::FIXED_LENGTH) {
    cerr << "Unexpected EOF" << std::endl;
    return -EFAULT;
  }

  sh.decode(ebliter);

  return 0;
}


int RadosDump::get_header(header *h)
{
  assert (h != NULL);

  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sh.header_size);
  if ((size_t)bytes != sh.header_size) {
    cerr << "Unexpected EOF" << std::endl;
    return -EFAULT;
  }

  h->decode(ebliter);

  return 0;
}

int RadosDump::get_footer(footer *f)
{
  assert(f != NULL);

  bufferlist ebl;
  bufferlist::iterator ebliter = ebl.begin();
  ssize_t bytes;

  bytes = ebl.read_fd(file_fd, sh.footer_size);
  if ((size_t)bytes != sh.footer_size) {
    cerr << "Unexpected EOF" << std::endl;
    return EFAULT;
  }

  f->decode(ebliter);

  if (f->magic != endmagic) {
    cerr << "Bad footer magic" << std::endl;
    return -EFAULT;
  }

  return 0;
}

int RadosDump::read_section(sectiontype_t *type, bufferlist *bl)
{
  header hdr;
  ssize_t bytes;

  int ret = get_header(&hdr);
  if (ret)
    return ret;

  *type = hdr.type;

  bl->clear();
  bytes = bl->read_fd(file_fd, hdr.size);
  if (bytes != hdr.size) {
    cerr << "Unexpected EOF" << std::endl;
    return -EFAULT;
  }

  if (hdr.size > 0) {
    footer ft;
    ret = get_footer(&ft);
    if (ret)
      return ret;
  }

  return 0;
}


int RadosDump::skip_object(bufferlist &bl)
{
  bufferlist::iterator ebliter = bl.begin();
  bufferlist ebl;
  bool done = false;
  while(!done) {
    sectiontype_t type;
    int ret = read_section(&type, &ebl);
    if (ret)
      return ret;

    ebliter = ebl.begin();
    if (type >= END_OF_TYPES) {
      cout << "Skipping unknown object section type" << std::endl;
      continue;
    }
    switch(type) {
    case TYPE_DATA:
    case TYPE_ATTRS:
    case TYPE_OMAP_HDR:
    case TYPE_OMAP:
#ifdef DIAGNOSTIC
      cerr << "Skip type " << (int)type << std::endl;
#endif
      break;
    case TYPE_OBJECT_END:
      done = true;
      break;
    default:
      cerr << "Can't skip unknown type: " << type << std::endl;
      return -EFAULT;
    }
  }
  return 0;
}

//Write super_header with its fixed 16 byte length
void RadosDump::write_super()
{
  if (dry_run) {
    return;
  }

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
  assert(super_header::FIXED_LENGTH == superbl.length());
  superbl.write_fd(file_fd);
}

ostream& operator<<(ostream& out, const pg_begin& pgb)
{
  out << "pgid " << pgb.pgid << " super block " << pgb.superblock;
  return out;
}

ostream& operator<<(ostream& out, const object_begin& ob)
{
  out << "oid " << ob.hoid << " info " << ob.oi;
  return out;
}

ostream& operator<<(ostream& out, const data_section& ds)
{
  out << "\toffset " << ds.offset << " len " << ds.len;
  // For now don't put put ds.databl
  return out;
}
ostream& operator<<(ostream& out, const attr_section& as)
{
  //map<string,bufferlist> data;
  int first = true;
  for (auto a : as.data) {
    if (first) {
      out << "attr keys: ";
      first = false;
    } else {
      out << ", ";
    }
    out << a.first;
  }
  return out;
}

ostream& operator<<(ostream& out, const omap_hdr_section& ohs)
{
  string hdr = ohs.hdr.to_str();
  out << "omap_header: " << cleanbin(hdr);
  return out;
}

ostream& operator<<(ostream& out, const omap_section& os)
{
  int first = true;
  for (auto o : os.omap) {
    if (first) {
      out << "omap keys: ";
      first = false;
    } else {
      out << ", ";
    }
    out << o.first;
  }
  return out;
}

ostream& operator<<(ostream& out, const metadata_section& ms)
{
  //__u8 struct_ver;  // for reference
  //epoch_t map_epoch;
  //pg_info_t info;
  //pg_log_t log;
  //map<epoch_t,pg_interval_t> past_intervals;
  //OSDMap osdmap;
  //bufferlist osdmap_bl;  // Used in lieu of encoding osdmap due to crc checking
  //map<eversion_t, hobject_t> divergent_priors;

  out << "metadata: ver " << ms.struct_ver << " epoch " << ms.map_epoch << " info " << ms.info;
  return out;
} 
