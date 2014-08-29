#ifndef CEPH_RADOS_TYPES_HPP
#define CEPH_RADOS_TYPES_HPP

#include <utility>
#include <vector>
#include <stdint.h>
#include "buffer.h"
#include "include/encoding.h"
#include "include/cmp.h"

#include <include/rados/rados_types.h>

namespace librados {

typedef uint64_t snap_t;

enum {
  SNAP_HEAD = (uint64_t)(-2),
  SNAP_DIR = (uint64_t)(-1)
};

struct clone_info_t {
  snap_t cloneid;
  std::vector<snap_t> snaps;          // ascending
  std::vector< std::pair<uint64_t,uint64_t> > overlap;  // with next newest
  uint64_t size;
  clone_info_t() : cloneid(0), size(0) {}
};

struct snap_set_t {
  std::vector<clone_info_t> clones;   // ascending
  snap_t seq;   // newest snapid seen by the object
  snap_set_t() : seq(0) {}
};

/**
 * @var all_nspaces
 * Pass as nspace argument to IoCtx::set_namespace()
 * before calling nobjects_begin() to iterate
 * through all objects in all namespaces.
 */
const std::string all_nspaces(LIBRADOS_ALL_NSPACES);

/**
 * @struct ListObject_t
 * Type dereferenced from a NObjectIterator
 */
struct ListObject_t {
  std::string nspace;
  std::string oid;
  std::string locator;
  ListObject_t(std::string n, std::string o, std::string l) : nspace(n), oid(o), locator(l) {}
  ListObject_t() {}
  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    ::encode(nspace, bl);
    ::encode(oid, bl);
    ::encode(locator, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::iterator &bl) {
    DECODE_START(1, bl);
    ::decode(nspace, bl);
    ::decode(oid, bl);
    ::decode(locator, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(ListObject_t)

WRITE_EQ_OPERATORS_3(ListObject_t, nspace, oid, locator)
WRITE_CMP_OPERATORS_3(ListObject_t, nspace, oid, locator)
}

inline std::ostream& operator<<(std::ostream& out, const struct librados::ListObject_t& lop) {
  out << (lop.nspace.size() ? lop.nspace + "/" : "") << lop.oid
      << (lop.locator.size() ? "@" + lop.locator : "");
  return out;
}
#endif
