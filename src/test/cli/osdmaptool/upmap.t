  $ osdmaptool --create-from-conf om -c $TESTDIR/ceph.conf.withracks --with-default-pool
  osdmaptool: osdmap file 'om'
  osdmaptool: writing epoch 1 to om
  $ osdmaptool --osd_calc_pg_upmaps_aggressively=false om --mark-up-in --upmap-max 11 --upmap c
  osdmaptool: osdmap file 'om'
  marking all OSDs up and in
  writing upmap command output to: c
  checking for upmap cleanups
  upmap, max-count 11, max deviation 0.01
  pools 1 
  $ cat c
  ceph osd pg-upmap-items 1.7 142 147
  ceph osd pg-upmap-items 1.8 219 223
  ceph osd pg-upmap-items 1.17 171 173 201 202
  ceph osd pg-upmap-items 1.1a 201 202
  ceph osd pg-upmap-items 1.1c 171 173 201 202
  ceph osd pg-upmap-items 1.20 88 87 201 202
  ceph osd pg-upmap-items 1.51 201 202
  ceph osd pg-upmap-items 1.62 219 223
  $ rm -f om c
