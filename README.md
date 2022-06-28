# RADOS VOL

## Dependencies
- HDF5
- MPI
and
either Mobject or Rados
if Mobject:
- Mobject
- Margo
- Mercury
- ABT

if Rados:
- Ceph
- librados

## Commands
## Build with Rados instead of MObject (MObject is default)
`mkdir build; cd build`
`cmake .. -DHDF5_VOL_RADOS_USE_MOBJECT=OFF`
