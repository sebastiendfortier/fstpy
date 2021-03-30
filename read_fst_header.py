#!/usr/bin/env python3

import numpy as np
from ctypes import Structure, POINTER, c_void_p, c_uint32, c_int32, c_int, c_uint, c_byte, c_char_p
#methode de micheal avec rmnlib
#########################################################################
# def get_std_file_header (funit, out=None):
#   '''
#   Extract parameters for *all* records.
#   Returns a dictionary similar to fstprm, only the entries are
#   vectorized over all records instead of 1 record at a time.
#   NOTE: This includes deleted records as well.  You can filter them out using
#         the 'dltf' flag.
#   '''
#   from ctypes import cast
#   import numpy as np
#   # Get the raw (packed) parameters.
#   index = librmn.file_index(funit)
#   raw = []
#   file_index_list = []
#   pageno_list = []
#   recno_list = []
#   while index >= 0:
#     f = file_table[index].contents
#     for pageno in range(f.npages):
#       page = f.dir_page[pageno].contents
#       params = cast(page.dir.entry,POINTER(c_uint32))
#       params = np.ctypeslib.as_array(params,shape=(ENTRIES_PER_PAGE,9,2))
#       nent = page.dir.nent
#       raw.append(params[:nent])
#       recno_list.extend(list(range(nent)))
#       pageno_list.extend([pageno]*nent)
#       file_index_list.extend([index]*nent)
#     index = f.link
#   raw = np.concatenate(raw)
#   # Start unpacking the pieces.
#   # Reference structure (from qstdir.h):
#   # 0      word deleted:1, select:7, lng:24, addr:32;
#   # 1      word deet:24, nbits: 8, ni:   24, gtyp:  8;
#   # 2      word nj:24,  datyp: 8, nk:   20, ubc:  12;
#   # 3      word npas: 26, pad7: 6, ig4: 24, ig2a:  8;
#   # 4      word ig1:  24, ig2b:  8, ig3:  24, ig2c:  8;
#   # 5      word etik15:30, pad1:2, etik6a:30, pad2:2;
#   # 6      word etikbc:12, typvar:12, pad3:8, nomvar:24, pad4:8;
#   # 7      word ip1:28, levtyp:4, ip2:28, pad5:4;
#   # 8      word ip3:28, pad6:4, date_stamp:32;
#   nrecs = raw.shape[0]
#   if out is None:
#     out = {}
#     out['nomvar'] = np.empty(nrecs,dtype='|S4')
#     out['typvar'] = np.empty(nrecs,dtype='|S2')
#     out['etiket'] = np.empty(nrecs,dtype='|S12')
#     out['ni'] = np.empty(nrecs, dtype='int32')
#     out['nj'] = np.empty(nrecs, dtype='int32')
#     out['nk'] = np.empty(nrecs, dtype='int32')
#     out['dateo'] = np.empty(nrecs, dtype='int32')
#     out['ip1'] = np.empty(nrecs, dtype='int32')
#     out['ip2'] = np.empty(nrecs, dtype='int32')
#     out['ip3'] = np.empty(nrecs, dtype='int32')
#     out['deet'] = np.empty(nrecs, dtype='int32')
#     out['npas'] = np.empty(nrecs, dtype='int32')
#     out['datyp'] = np.empty(nrecs, dtype='ubyte')
#     out['nbits'] = np.empty(nrecs, dtype='byte')
#     out['grtyp'] = np.empty(nrecs, dtype='|S1')
#     out['ig1'] = np.empty(nrecs, dtype='int32')
#     out['ig2'] = np.empty(nrecs, dtype='int32')
#     out['ig3'] = np.empty(nrecs, dtype='int32')
#     out['ig4'] = np.empty(nrecs, dtype='int32')
#     out['datev'] = np.empty(nrecs, dtype='int32')
#     out['key'] = np.empty(nrecs, dtype='int32')
#     out['dltf'] = np.empty(nrecs, dtype='ubyte')
#     lng = np.empty(nrecs, dtype='int32')
#     ubc = np.empty(nrecs, dtype='uint16')
#     # out['swa'] =  np.empty(nrecs, dtype='uint32')
    
#     # out['xtra1'] = np.empty(nrecs, dtype='uint32')
#     # out['xtra2'] = np.empty(nrecs, dtype='uint32')
#     # out['xtra3'] = np.empty(nrecs, dtype='uint32')


#   temp8 = np.empty(nrecs, dtype='ubyte')
#   temp32 = np.empty(nrecs, dtype='int32')

#   # np.divmod(raw[:,0,0],2**24, temp8, out['lng'])
#   np.divmod(raw[:,0,0],2**24, temp8, lng)
#   np.divmod(temp8,128, out['dltf'], temp8)
#   # out['swa'][:] = raw[:,0,1]
#   np.divmod(raw[:,1,0],256, out['deet'], out['nbits'])
#   np.divmod(raw[:,1,1],256, out['ni'], out['grtyp'].view('ubyte'))
#   np.divmod(raw[:,2,0],256, out['nj'], out['datyp'])
#   np.divmod(raw[:,2,1],4096, out['nk'], ubc)
#   out['npas'][:] = raw[:,3,0]//64
#   np.divmod(raw[:,3,1],256, out['ig4'], temp32)
#   out['ig2'][:] = (temp32 << 16) # ig2a
#   np.divmod(raw[:,4,0],256, out['ig1'], temp32)
#   out['ig2'] |= (temp32 << 8) # ig2b
#   np.divmod(raw[:,4,1],256, out['ig3'], temp32)
#   out['ig2'] |= temp32 # ig2c
#   etik15 = raw[:,5,0]//4
#   etik6a = raw[:,5,1]//4
#   et = raw[:,6,0]//256
#   etikbc, _typvar = divmod(et, 4096)
#   _nomvar = raw[:,6,1]//256
#   np.divmod(raw[:,7,0],16, out['ip1'], temp8)
#   out['ip2'][:] = raw[:,7,1]//16
#   out['ip3'][:] = raw[:,8,0]//16
#   date_stamp = raw[:,8,1]
#   # Reassemble and decode.
#   # (Based on fstd98.c)
#   etiket_bytes = np.empty((nrecs,12),dtype='ubyte')
#   for i in range(5):
#     etiket_bytes[:,i] = ((etik15 >> ((4-i)*6)) & 0x3f) + 32
#   for i in range(5,10):
#     etiket_bytes[:,i] = ((etik6a >> ((9-i)*6)) & 0x3f) + 32
#   etiket_bytes[:,10] = ((etikbc >> 6) & 0x3f) + 32
#   etiket_bytes[:,11] = (etikbc & 0x3f) + 32
#   out['etiket'][:] = etiket_bytes.flatten().view('|S12')
#   nomvar_bytes = np.empty((nrecs,4),dtype='ubyte')
#   for i in range(4):
#     nomvar_bytes[:,i] = ((_nomvar >> ((3-i)*6)) & 0x3f) + 32
#   out['nomvar'][:] = nomvar_bytes.flatten().view('|S4')
#   typvar_bytes = np.empty((nrecs,2),dtype='ubyte')
#   typvar_bytes[:,0] = ((_typvar >> 6) & 0x3f) + 32
#   typvar_bytes[:,1] = ((_typvar & 0x3f)) + 32
#   out['typvar'][:] = typvar_bytes.flatten().view('|S2')
#   out['datev'][:] = (date_stamp >> 3) * 10 + (date_stamp & 0x7)
#   # Note: this dateo calculation is based on my assumption that
#   # the raw stamps increase in 5-second intervals.
#   # Doing it this way to avoid a gazillion calls to incdat.
#   date_stamp = date_stamp - (out['deet']*out['npas'])//5
#   out['dateo'][:] = (date_stamp >> 3) * 10 + (date_stamp & 0x7)
#   # out['xtra1'][:] = out['datev']
#   # out['xtra2'][:] = 0
#   # out['xtra3'][:] = 0
#   # Calculate the handles (keys)
#   # Based on "MAKE_RND_HANDLE" macro in qstdir.h.
#   out['key'][:] = (np.array(file_index_list)&0x3FF) | ((np.array(recno_list)&0x1FF)<<10) | ((np.array(pageno_list)&0xFFF)<<19)
#   out['nomvar'] = np.char.strip(out['nomvar'].astype('str'))
#   out['typvar'] = np.char.strip(out['typvar'].astype('str')) 
#   out['etiket'] = np.char.strip(out['etiket'].astype('str')) 
#   out['grtyp'] = np.char.strip(out['grtyp'].astype('str')) 
#   # out.pop('xtra1')
#   # out.pop('xtra2')
#   # out.pop('xtra3')
#   # out.pop('ubc')
#   # out.pop('lng')
#   # out.pop('swa')
#   return out

#########################################################################

# 0x00
# 0b00000000


# 0b00001000 # X
# 0b01000000 # X << 3
# 0b00001000 00000000  # X << 8

# 0xa1 << 8
# 0xa1 00

# 0xaabbccdd 
# b[0] = 0xaa
# b[1] = 0xbb
# b[2] = 0xcc
# b[3] = 0xdd

# 0xaabbccdd >> 8
# 0xaabbcc 

# Read a 32-bit integer from a buffer
def read32(b) -> int:
    return (b[0]<<24) | (b[1]<<16) | (b[2]<<8) | (b[3]<<0)

# Read a 24-bit integer from a buffer
def read24(b) -> int:
    return (b[0]<<16) | (b[1]<<8) | (b[2]<<0)


# Read a 16-bit integer from a buffer
def read16(b) -> int:
    return (b[0]<<8) | (b[1]<<0)


# Read a 32-bit integer from a stream
def fread32(f) -> int:
    b = f.read(4)
    return read32(b)


# Read a 32-bit float from a stream
def  freadfloat(f) -> float:
    x = fread32(f)
    return float(x)

def readchar(dest_bytes,src, n):
    for i in range(0,n):#(int i = 0; i < n; i++)
        dest_bytes[i] = 0
        # first byte needed
        j = int((i*6)/8)
        # shift from the beginning
        sh = (i*6)%8
        dest_bytes[i] |= ((src[j] << sh) >> 2)
        # do we need a second byte?
        if (sh > 2):
            dest_bytes[i] |= (src[j+1]>>(10-sh))
        dest_bytes[i] += 32


# Validate the byte count at the beginning/end of an unformatted Fortran stream
def  ftn_section (f, n:int=4): 
    m = fread32(f)
    assert 4 == 4

class FileHeader:
    file_size=0
    num_overwrites=0
    num_extensions=0
    nchunks=0
    last_chunk=0
    max_data_length=0
    num_erasures=0
    nrecs=0    

class ChunkHeader:
    this_chunk_words=0
    this_chunk=0
    next_chunk_words=0
    next_chunk=0
    nrecs=0
    checksum=0

class RecordHeader: 
    status=0
    size=0
    data=0
    deet=0
    npak=0
    ni=0
    grtyp=''
    nj=0
    datyp=0
    nk=0
    npas=0
    ig4=0
    ig2=0
    ig1=0
    ig3=0
    etiket=''
    typvar=''
    nomvar=''
    ip1=0
    ip2=0
    ip3=0
    dateo=0
    checksum=0

def print_file_header(h:FileHeader):
    print("file_size: %d, num_overwrites: %d, num_extensions: %d, nchunks: %d, last_chunk: %x, max_data_length: %d, num_erasures: %d, nrecs: %d"%( h.file_size, h.num_overwrites, h.num_extensions, h.nchunks, h.last_chunk, h.max_data_length, h.num_erasures, h.nrecs))
    
def print_chunk_header(h:ChunkHeader) :
    print("this_chunk: %x, next_chunk: %x, nrecs: %d, checksum: %x"%( h.this_chunk, h.next_chunk, h.nrecs, h.checksum))

def print_record_header (h:RecordHeader):
    print("status: %d, size: %d, data pointer: %x, deet: %d, npak: %d, ni: %d, grtyp: '%c', nj: %d, datyp: %d nk: %d, npas: %d, ig4: %d, ig2: %d, ig1: %d, ig3: %d, etiket: '%s', typvar: '%s', nomvar: '%s', ip1: %d, ip2: %d, ip3: %d, \ndateo: %d"%( h.status, h.size, h.data, h.deet, h.npak, h.ni, h.grtyp, h.nj, h.datyp, h.nk, h.npas, h.ig4, h.ig2, h.ig1, h.ig3, h.etiket, h.typvar, h.nomvar, h.ip1, h.ip2, h.ip2, h.dateo))


def print_record_header1 (h:RecordHeader):
    print("%4s %2s %12s %4d %4d %1d %10d %8d %8d %8d %4d %4d %3d %2d %c %5d %5d %5d %5d"%(h.nomvar,h.typvar,h.etiket,h.ni, h.nj,h.nk,h.dateo,h.ip1,h.ip2,h.ip3, h.deet, h.npas, h.datyp, h.npak, h.grtyp, h.ig1, h.ig2, h.ig3, h.ig4))
import binascii
def read_file_header (f) -> FileHeader:
    h = FileHeader()
    buf = f.read(208)
    buf[0]
    assert read24(buf[1:])==26  # header length (words) 26
    assert read32(buf[4:])==0 # address of file header 0
    assert buf[8:8+4] == b"XDF0" # XDF version
    assert buf[12:12+4] == b"STDR" # application signature
    h.file_size = read32(buf[16:]) * 8
    h.num_overwrites = read32(buf[20:])
    h.num_extensions = read32(buf[24:])
    h.nchunks = read32(buf[28:])
    h.last_chunk = read32(buf[32:]) * 8
#     print(h.file_size,h.num_extensions,h.nchunks,h.last_chunk)
    assert (read16(buf[40:]) == 16) # # of primary keys
    assert (read16(buf[42:]) == 9)  # length of primary keys (words)
    assert (read16(buf[44:]) == 2)  # # of auxiliary keys
    assert (read16(buf[46:]) == 1)  # length of auxiliary keys (words)
    h.num_erasures = read32(buf[48:])
    h.nrecs = read32(buf[52:])
    assert (read32(buf[56:]) == 0)  # read/write flag
    assert (read32(buf[60:]) == 0)  # reserved area
    # print('h.file_size,h.num_extensions,h.nchunks,h.last_chunk,h.num_erasures,h.nrecs')
    # print(h.file_size,h.num_extensions,h.nchunks,h.last_chunk,h.num_erasures,h.nrecs)
     # Validate primary keys
    for i in range(0,16):#(int i = 0 i < 16 i++) 
        ncle = b"SF%02d"%(i+1)
        offset = 64+i*8
        #print(buf[offset:offset+4])
        assert (buf[offset:offset+4] == ncle)  # validate key names
        assert ((read16(buf[offset+4:])>>3) == 31+i*32)  # validate bit1
        assert ((read24(buf[offset+5:])&0x7FFFF) == 0x7C000) # validate lcls/tcle
    # Validate auxiliary keys
    for i in range(0,2):#(int i = 0 i < 2 i++) 
        ncle = b"AXI%01d"%(i+1)
        offset = 192+i*8
        assert (buf[offset:offset+4] == ncle)  # validate key names
        assert ((read16(buf[offset+4:])>>3) == 31+i*32)  # validate bit1
        assert ((read24(buf[offset+5:])&0x7FFFF) == 0x7C000) # validate lcls/tcle    
    print_file_header(h)  
    return h

def read_chunk_header (f) -> ChunkHeader:
    h = ChunkHeader()  
    buf = f.read(32)
    assert (buf[0] == 0) # idtyp
    assert (read24(buf[1:]) == 2308)  # Header length (words)
    h.this_chunk_words = read32(buf[4:])
    h.this_chunk = h.this_chunk_words * 8 - 8
    assert (read32(buf[8:]) == 0) # reserved1
    assert (read32(buf[12:]) == 0) # reserved2
    h.next_chunk_words = read32(buf[16:])
    h.next_chunk = h.next_chunk_words * 8
    if h.next_chunk > 0:
        h.next_chunk -= 8 # Rewind a bit
    h.nrecs = read32(buf[20:])
    h.checksum = read32(buf[24:])
    assert (read32(buf[28:]) == 0)  # reserved3
    print_chunk_header(h)
    return h

def read_record_header (f) -> RecordHeader:
    h = RecordHeader()
    buf = f.read(72)
    h.status = buf[0]
    h.size = read24(buf[1:])
    h.data = read32(buf[4:]) * 8
    assert (h.data > 8)
    h.data -= 8  # rewind a bit to get the proper start of the data
    h.deet = read24(buf[8:])
    h.npak = buf[11]
    h.ni = read24(buf[12:])
    h.grtyp = buf[15]
    h.nj = read24(buf[16:])
    h.datyp = buf[19]
    h.nk = read24(buf[20:])>>4
    #TODO: ubc
    h.npas = (read32(buf[24:])>>6)
    h.ig4 = read24(buf[28:])
    h.ig2 = buf[31]*65536 + buf[35]*256 + buf[39]
    h.ig1 = read24(buf[32:])
    h.ig3 = read24(buf[36:])
    
#     h.etiket = c_char_p()
#     h.etiket = ['','','','','','','','','','','','','',]
#     h.etiket = np.empty((13),dtype='ubyte')
#     readchar(h.etiket[0:], buf[40:], 5)
#     readchar(h.etiket[5:], buf[44:], 5)
#     readchar(h.etiket[10:], buf[48:], 2)
#     h.etiket[12] = 0
#     h.etiket=h.etiket.view('|S13')[0]
#     print(binascii.b2a_uu(h.etiket[0:5]))
#     print('------------')
#     etiket1=binascii.b2a_uu(buf[40:45])
#     etiket2=binascii.b2a_uu(buf[44:49])
#     etiket3=binascii.b2a_uu(buf[48:50])
#     h.etiket = etiket1# + etiket2 + etiket3
#     R1_V710_N

    a = binascii.b2a_uu(buf[40:44])[1:-1].decode('ascii').strip()
    b = binascii.b2a_uu(buf[44:45])[1:-1].decode('ascii').strip()
    h.etiket = ''.join([a,b])



#     h.etiket = binascii.b2a_uu(buf[40:41])
#     h.etiket=h.etiket.view('|S13')

# 10000100 
# &
# 00001111
# =
# 00000100


#     h.typvar = c_char_p()
    h.typvar = np.empty((2),dtype='ubyte')
# 0xabcd
# 0000 0000 1010 1011 1100 1101 0000 0000
#                ---- --
# 0xab buf[49] 
# 10101011
#&
# 00001111
#=
# 0000 1011           
# <<2
# 00101100  0x2C
#          
# 0xcd buf[50]
# 11001101 
#>>6
# 0000 0011
# ((buf[49]&0x0F)<<2) + (buf[50]>>6) = 00101100 + 00000011 = 00101111 + 00100000 = 01001111

# 0xcd buf[50]
# 1100 1101 
# &
# 0011 1111
#=
# 0000 1101 + 00100000 
# 0010 0000
#+32=
# 0010 1101  48=0011 0000
# on utilise 6 bits pour stocker des chars
    h.typvar[0] = ((buf[49]&0x0F)<<2) + (buf[50]>>6) + 32   #0xdeadbeef 0x0f = 00001111    #0xdeadbeef & 0x0f -> 
    h.typvar[1] = (buf[50]&0x3F) + 32
    
    h.typvar = ''.join(map(chr,h.typvar))

    # h.typvar[2] = 0
#     h.typvar=binascii.b2a_uu(buf[49:50])
    #h.typvar=h.typvar.view('|S3')[0].decode('ascii').strip()
#     h.nomvar = c_char_p()
#     h.nomvar = np.empty((5),dtype='ubyte')
#     readchar(h.nomvar,buf[52:56], 4)
#     h.nomvar[4] = 0
#     h.nomvar=h.nomvar.view('|S5')[0]
    h.nomvar=binascii.b2a_uu(buf[52:56])[1:-1].decode('ascii').strip()
    
    
        


#     h.nomvar = buf[52:56]
    #h.nomvar[4] = 0
    h.ip1 = read32(buf[56:]) >> 4
    h.ip2 = read32(buf[60:]) >> 4
    h.ip3 = read32(buf[64:]) >> 4
    h.dateo = read32(buf[68:])

    # Checksum
    h.checksum = 0
    for i in range(0,72,4):#(int i = 0 i < 72 i+= 4) 
        h.checksum ^= ((buf[i]<<24) | (buf[i+1]<<16) | (buf[i+2]<<8) | buf[i+3])
    print_record_header1(h)
    return h



    
def get_record_headers (filename) -> []:
    f = open(filename, "rb")
    fileheader = read_file_header(f)

    chunkheaders = []
    rec = 0
    for c in range(0,fileheader.nchunks):#(int c = 0 c < fileheader.nchunks c++) 
        chunckheader = ChunkHeader()
        chunckheader = read_chunk_header(f)
        chunkheaders.append(chunckheader)
        checksum = 0
        recordheaders = []
        for r in range(0,chunckheader.nrecs):#(int r = 0 r < chunkheader.nrecs r++) 
            h = RecordHeader()
            h = read_record_header(f)
            recordheaders.append(h)
            # Skip erased records
            if (recordheaders[rec].status == 255):
                continue
            checksum ^= recordheaders[rec].checksum
            rec += 1
        rec = 0
        checksum ^= chunckheader.nrecs
        checksum ^= chunckheader.next_chunk_words
#         print("chunk %d checksum: 0x%08X   computed checksum: 0x%08X, xor: %08X\n", c, chunkheader.checksum, checksum, chunkheader.checksum ^ checksum)
        assert (checksum == chunckheader.checksum)
        # Go to next chunk
        f.seek(chunckheader.next_chunk,0)
    f.close()
    return recordheaders    

if __name__ == "__main__":
    # h = get_record_headers('/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std')
    h = get_record_headers('/space/hall3/sitestore/eccc/cmod/prod/hubs/gridpt/dbase/prog/glbeta/2021031912_240')