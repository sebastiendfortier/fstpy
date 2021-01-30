
import rpnpy.librmn.all as rmn

def dtype_fst2numpy(datyp, nbits=None):
  from rpnpy.librmn.fstd98 import dtype_fst2numpy
  if datyp == 0:
    print("Raw binary records detected.  The values may not be properly decoded if you're opening on a different platform.")
    datyp = 5
  return dtype_fst2numpy(datyp, nbits)

# Decorator for efficiently converting a scalar function to a vectorized
# function.


def vectorize(f):
  from functools import wraps
  try:
    from pandas import Series, unique
    import numpy as np

    @wraps(f)
    def vectorized_f(x):
      # If we're given a scalar value, then simply return it.
      if not hasattr(x, '__len__'):
        return f(x)
      # Get unique values
      x = np.array(x, copy=True)
      inputs = unique(x)
      outputs = map(f, inputs)
      table = dict(zip(inputs, outputs))
      result = Series(x).map(table)
      return result.values
  except ImportError:
    def cached_f(x, cache={}):
      if x not in cache:
        cache[x] = f(x)
      return cache[x]

    @wraps(f)
    def vectorized_f(x):
      # If we're given a scalar value, then simply return it.
      if not hasattr(x, '__len__'):
        return cached_f(x)
      return list(map(cached_f, x))
  return vectorized_f


class _base_type (object):
  @property
  def shape(self):
    return tuple(map(len, self.axes))

  @property
  def dims(self):
    return tuple(a.name for a in self.axes)

  def getaxis(self, name):
    for a in self.axes:
      if a.name == name:
        return a
    return None

  def __iter__(self):
    return (getattr(self, n) for n in self.__slots__)

# Data that's loaded in memory.


class _var_type (_base_type):
  __slots__ = ('name', 'atts', 'axes', 'array', 'deps')

  def __init__(self, name, atts, axes, array):
    self.name = name
    self.atts = atts
    self.axes = list(axes)
    self.array = array
    self.deps = []

# Axis data.


class _axis_type (_base_type):
  __slots__ = ('name', 'atts', 'axes', 'array', 'deps')

  def __init__(self, name, atts, array):
    self.name = name
    self.atts = atts
    self.axes = [self]
    self.array = array
    self.deps = []

  def __len__(self):
    return len(self.array)

# Dimension (without coordinate values).


class _dim_type (object):
  __slots__ = ('name', 'length', 'deps')

  def __init__(self, name, length):
    self.name = name
    self.length = length
    self.deps = []

  def __len__(self):
    return self.length

# Data that resides in FST records on disk.


class _iter_type (_base_type):
  __slots__ = ('name', 'atts', 'axes', 'dtype', 'record_id', 'deps')

  def __init__(self, name, atts, axes, dtype, record_id):
    self.name = name
    self.atts = atts
    self.axes = axes
    self.dtype = dtype
    self.record_id = record_id
    self.deps = []







# Define a class for encoding / decoding FSTD data.
# Each step is placed in its own "mixin" class, to make it easier to patch in
# new behaviour if more exotic FSTD files are encountered in the future.


class BufferBase (object):

  # Keep a reference to fstd98 so it's available during cleanup.
  try:
    from rpnpy.librmn import fstd98 as _fstd98
  except ImportError:
    pass

  # Names of records that should be kept separate (never grouped into
  # multidimensional arrays).
  _meta_records = ()
  # Other records that should also be kept separate, but only if they are
  # 1-dimensional.  If they are 2D, then they should be processed as a normal
  # variable.
  _maybe_meta_records = ()

  # Attributes which could potentially be used as axes.
  _outer_axes = ()

  # Attributes which could be used as auxiliary coordinates for the outer
  # axes.  The dictionary keys are the outer axis names, and the values are
  # a list of columns which can act as coordinates.
  from collections import OrderedDict
  _outer_coords = OrderedDict()
  del OrderedDict

  # Attributes which uniquely identify a variable.
  # Note: nomvar should always be the first attribute
  _var_id = ('nomvar', 'ni', 'nj', 'nk')

  # Similar to above, but a human-readable version of the id.
  # Could be used for appending suffixes to variable names to make them unique.
  # Uses string formatting operations on the variable metadata.
  _human_var_id = ('%(nomvar)s', '%(ni)sx%(nj)s', '%(nk)sL')

  # Field names and types for storing record headers in a structured array.
  _headers_dtype = [
      ('file_id', 'int32'), ('key', 'int32'),
      ('dateo', 'int32'), ('datev', 'int32'), ('deet', 'int32'), ('npas', 'int32'),
      ('ni', 'int32'), ('nj', 'int32'), ('nk', 'int32'), ('nbits', 'int32'),
      ('datyp', 'int32'), ('ip1', 'int32'), ('ip2', 'int32'), ('ip3', 'int32'),
      ('typvar', '|S2'), ('nomvar', '|S4'), ('etiket', '|S12'), ('grtyp', '|S1'),
      ('ig1', 'int32'), ('ig2', 'int32'), ('ig3', 'int32'), ('ig4', 'int32'),
      ('swa', 'int32'), ('lng', 'int32'), ('dltf', 'int32'), ('ubc', 'int32'),
      ('xtra1', 'int32'), ('xtra2', 'int32'), ('xtra3', 'int32'),
  ]

  # Record parameters which should not be used as nc variable attributes.
  # (They're either internal to the file, or part of the data, not metadata).
  _ignore_atts = ('file_id', 'swa', 'lng', 'dltf', 'ubc',
                  'xtra1', 'xtra2', 'xtra3', 'key', 'shape', 'd')



  # Clean up a buffer (close any attached files, etc.)
  def __del__(self):
    try:
      self._close()
      from rpnpy.librmn.fstd98 import fstcloseall
      fstcloseall(self._meta_funit)
    except (ImportError, AttributeError):
      pass  # May fail if Python is doing a final cleanup of everything.
      # Or, if buffer wasn't fully initialized yet.

  # Extract metadata from a particular header.
  def _get_header_atts(self, header):
    for n, v in header.items():
      if n in self._ignore_atts:
        continue
      # Python 3: convert bytes to str
      if isinstance(v, bytes):
        v = str(v.decode())
      if isinstance(v, str):
        v = v.strip()
      yield (n, v)

  # Open the specified file (by index)
  def _open(self, file_id):
    from rpnpy.librmn.base import fnom
    from rpnpy.librmn.fstd98 import fstouv
    from rpnpy.librmn.const import FST_RO
    
    opened_file_id = getattr(self, '_opened_file_id', -1)
    # Check if this file already opened.
    if opened_file_id == file_id:
      return self._opened_funit
    # Close any open files before continuing.
    self._close()
    filename = self._files[file_id]
    # Open the file.
    self._opened_file_id = file_id
    self._opened_funit = fnom(filename, FST_RO)
    fstouv(self._opened_funit, FST_RO)
    self._opened_librmn_index = rmn.file_index(self._opened_funit)
    return self._opened_funit

  # Close any currently opened file.
  def _close(self):
    from rpnpy.librmn.base import fclos
    from rpnpy.librmn.fstd98 import fstfrm
    opened_funit = getattr(self, '_opened_funit', -1)
    if opened_funit >= 0:
      fstfrm(opened_funit)
      fclos(opened_funit)
    self._opened_file_id = -1
    self._opened_funit = -1
    self._opened_librmn_index = -1

  ###############################################
  # Basic flow for reading data

  # Filter out any Buffer arguments from object.__new__.
  # Otherwise, get an error when running with Python3.

  def __new__(cls, *args, **kwargs):
    return object.__new__(cls)

  def __init__(self):
    """
    Read raw records from FSTD files, into the buffer.
    Multiple files can be read simultaneously.

    Parameters
    ----------
    filename : str or list
        The RPN standard file(s) to convert.
    progress : bool, optional
        Display a progress bar during the conversion, if the "progress"
        module is installed.
    rpnstd_metadata : bool, optional
        Include all RPN record attributes in the output metadata.
    rpnstd_metadata_list : str or list, optional
        Specify a minimal set of RPN record attributes to include in the
        output file.
    ignore_typvar : bool, optional
        Tells the converter to ignore the typvar when deciding if two
        records are part of the same field.  Default is to split the
        variable on different typvars.
    ignore_etiket : bool, optional
        Tells the converter to ignore the etiket when deciding if two
        records are part of the same field.  Default is to split the
        variable on different etikets.
    """
    from rpnpy.librmn.fstd98 import fstnbr, fstinl, fstprm, fstopenall
    from rpnpy.librmn.const import FST_RO
    from collections import Counter
    import numpy as np
    from glob import glob, has_magic
    import os
    import warnings
    minimal_metadata=None
    rpnstd_metadata=True
    rpnstd_metadata_list=None

    # Set default for minimal_metadata
    if rpnstd_metadata is not None:
      minimal_metadata = not rpnstd_metadata
    if minimal_metadata is None:
      minimal_metadata = True
    # Set default for rpnstd_metadata_list
    if minimal_metadata is True and rpnstd_metadata_list is None:
      rpnstd_metadata_list = ''
    if isinstance(rpnstd_metadata_list, str):
      rpnstd_metadata_list = rpnstd_metadata_list.replace(',', ' ')
      rpnstd_metadata_list = rpnstd_metadata_list.split()
    if hasattr(rpnstd_metadata_list, '__len__'):
      rpnstd_metadata_list = tuple(rpnstd_metadata_list)
    self._rpnstd_metadata_list = rpnstd_metadata_list


    self._var_id = self._var_id[0:1] + ('typvar',) + self._var_id[1:]
    self._human_var_id = self._human_var_id[0:1] + ('%(typvar)s',) + self._human_var_id[1:]

    self._var_id = self._var_id[0:1] + ('etiket',) + self._var_id[1:]
    self._human_var_id = self._human_var_id[0:1] + ('%(etiket)s',) + self._human_var_id[1:]

    
    # Inspect all input files, and extract the headers from valid RPN files.
    headers = []
    self._files = []
    header_cache = {}



    # Read the headers from the file(s) and store the info in the table.
    filenum = len(self._files)
    self._files.append('/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std')

    funit = rmn.fnom('/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std', FST_RO)
    rmn.fstouv(funit, FST_RO)
    self._opened_librmn_index = rmn.file_index(funit)

    nrecs = fstnbr(funit)
    h = np.zeros(nrecs, dtype=self._headers_dtype)


    from std.standardfile import all_params
    params = all_params(funit, out=h)
    # get all the keys
    keys = params['key']

    # Encode the keys without the file index info.
    h['key'] = keys
    h['key'] >>= 10
    header_cache['/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std'] = h

    h = header_cache['/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std']
    # The file info will be an index into a separate file list.
    h['file_id'] = filenum

    headers.append(h)

    nfiles = len(headers)
    
    print("Found %d RPN input file(s)" % nfiles)

    self._headers = np.ma.concatenate(headers)

    # Find all unique meta (coordinate) records, and link a subset of files
    # that provide all unique metadata records.
    # This will make it easier to look up the meta records later.
    meta_mask = np.zeros(len(self._headers), dtype='bool')
    for meta_name in self._meta_records + self._maybe_meta_records:
      meta_name = (meta_name+'   ')[:4]
      meta_mask |= (self._headers['nomvar'] == meta_name)
    meta_recids = np.where(meta_mask)[0]
    # Use the same unique parameters as regular variables.
    # Plus, ig1,ig2,ig3,ig4.
    # Suppress FutureWarning from numpy about doing this.  Probably benign...
    meta_keys = self._headers.data[meta_mask][list(self._var_id)+['ip1', 'ip2', 'ip3', 'ig1', 'ig2', 'ig3', 'ig4']]
    meta_keys, ind = np.unique(meta_keys, return_index=True)
    meta_recids = meta_recids[ind]
    # Find the files that give these unique coord records.
    file_ids = sorted(set(self._headers['file_id'][meta_recids]))
    filenames = [self._files[f] for f in file_ids]
    
    # Open these files and link them together
    self._meta_funit = fstopenall(filenames, FST_RO)

  # Generate structured variables from FST records.

  # Normal version (without use of pandas).

  

  # Faster version of iterator (using pandas).
  def _makevars_pandas(self):
    from collections import OrderedDict
    import numpy as np
    import pandas as pd
    import warnings

    nrecs = len(self._headers)

    # Degenerate case: no data in buffer
    if nrecs == 0:
      return

    # Convert records to a pandas DataFrame, which is faster to operate on.
    records = pd.DataFrame.from_records(self._headers)
    # Keep track of original dtypes (may need to re-cast later).
    original_dtypes = dict(self._headers_dtype)

    # Ignore deleted / invalidated records.
    records = records[records['dltf'] == 0]

    # Keep track of any axes that were generated.
    known_axes = dict()

    # Keep track of any auxiliary coordinates that were generated.
    known_coords = dict()

    # Iterate over each variable.
    # Variables are defined by the entries in _var_id.
    self._varlist = []
    for var_id, var_records in records.groupby(list(self._var_id)):
      var_id = OrderedDict(zip(self._var_id, var_id))
      nomvar = var_id['nomvar'].strip()
      nomvar = str(nomvar.decode())  # Python3: convert bytes to str.
      # Ignore meta records.
      if nomvar in self._meta_records:
        continue
      if nomvar in self._maybe_meta_records and (var_id['ni'] == 1 or var_id['nj'] == 1):
        continue

      # Get the attributes, axes, and corresponding indices of each record.
      atts = OrderedDict()
      axes = OrderedDict()
      indices = OrderedDict()
      coordnames = []
      coord_axes = OrderedDict()
      coord_indices = OrderedDict()
      for n in records.columns:
        if n in self._ignore_atts:
          continue
        # Ignore columns which are masked out.
        # https://stackoverflow.com/questions/29530232/python-pandas-check-if-any-value-is-nan-in-dataframe
        if var_records[n].isnull().values.any():
          continue
        # Get the unique values, in order.
        # Coerce back to original dtype, since masked columns get upcasted to
        # float64 in pandas.DataFrame.from_records.
        try:
          column = var_records[n].astype(original_dtypes[n])
        except TypeError:
          # Some types may not be re-castable.
          # For instance, pandas < 0.23 can't convert between datetime64 with
          # different increments ([ns] and [s]).
          column = var_records[n]
        cat = pd.Categorical(column)
        # Is this column an outer axis?
        if n in self._outer_axes:
          values = tuple(cat.categories)
          if (n, values) not in known_axes:
            known_axes[(n, values)] = _axis_type(name=n, atts=OrderedDict(),
                                                 array=np.array(values, dtype=column.dtype))
          axes[n] = known_axes[(n, values)]
          indices[n] = cat.codes
          # Is this also an axis for an auxiliary coordinate?
          for coordname, coordaxes in self._outer_coords.items():
            if n in coordaxes:
              coordnames.append(coordname)
              coord_axes.setdefault(coordname, OrderedDict())[n] = axes[n]
              coord_indices.setdefault(coordname, OrderedDict())[n] = cat.codes
        # Otherwise, does it have a consistent value?
        # If so, can add it to the metadata.
        # Ignore outer coords, since the value is already encoded elsewhere.
        elif len(cat.categories) == 1 and n not in self._outer_coords:
          try:
            v = cat[0]
            # Python3: convert bytes to str.
            if isinstance(v, bytes):
              v = str(v.decode())
            # Trim string attributes (remove whitespace padding).
            if isinstance(v, str):
              v = v.strip()
            # Use regular integers for numeric types.
            elif np.can_cast(v, int):
              v = int(v)
            atts[n] = v
          except (TypeError, ValueError):
            pass

      # Recover the proper order for the outer axes.
      # Not necessarily the order of the header table columns.
      axes = OrderedDict((n, axes[n]) for n in self._outer_axes if n in axes)
      indices = tuple([indices[n] for n in self._outer_axes if n in indices])
      for coordname in coord_axes.keys():
        coord_axes[coordname] = OrderedDict(
            (n, coord_axes[coordname][n]) for n in self._outer_axes if n in coord_axes[coordname])
        coord_indices[coordname] = [coord_indices[coordname][n]
                                    for n in self._outer_axes if n in coord_indices[coordname]]

      # Construct a multidimensional array to hold the record keys.
      record_id = np.empty(list(map(len, axes.values())), dtype='int32')

      # Assume missing data (nan) unless filled in later.
      record_id[()] = -1

      # Arrange the record keys in the appropriate locations.
      record_id[indices] = var_records.index

      # Get the auxiliary coordinates.
      coords = []
      for n in coordnames:
        # Ignore auxiliary coordinates which are masked out.
        if var_records[n].isnull().values.any():
          continue
        # Unique key for this coordinate
        key = (n, tuple(coord_axes[n].items()))
        # Arrange the coordinate values in the appropriate location.
        shape = list(map(len, list(coord_axes[n].values())))
        values = np.zeros(shape, dtype=var_records[n].dtype)
        indices = tuple(coord_indices[n])
        values[indices] = var_records[n]
        if key not in known_coords:
          coord = _var_type(name=n, atts=OrderedDict(),
                            axes=list(coord_axes[n].values()),
                            array=values)
          known_coords[key] = coord
        coords.append(known_coords[key])
      if len(coords) > 0:
        atts['coordinates'] = coords

      # Check if we have full coverage along all axes.
      have_data = [k >= 0 for k in record_id.flatten()]
      if not np.all(have_data):
       print("Missing some records for %s." % nomvar)

      # Add dummy axes for the ni,nj,nk record dimensions.
      axes['k'] = _dim_type('k', int(var_id['nk']))
      axes['j'] = _dim_type('j', int(var_id['nj']))
      axes['i'] = _dim_type('i', int(var_id['ni']))

      # Determine the optimal data type to use.
      # First, find unique combos of datyp, nbits
      # (want to minimize calls to dtype_fst2numpy).
      x = var_records[['datyp', 'nbits']].drop_duplicates()
      datyp = map(int, x['datyp'])
      nbits = map(int, x['nbits'])
      dtype_list = map(dtype_fst2numpy, datyp, nbits)
      dtype = np.result_type(*dtype_list)

      var = _iter_type(name=nomvar, atts=atts,
                       axes=list(axes.values()),
                       dtype=dtype,
                       record_id=record_id)
      self._varlist.append(var)

      #TODO: Find a minimum set of partial coverages for the data.
      # (e.g., if we have surface-level output for some times, and 3D output
      # for other times).

  # Choose which method to iterate over the data
  # (depending on if pandas is installed).
  def _makevars(self):
    import pandas as pd
    self._makevars_pandas()
   

  

  
