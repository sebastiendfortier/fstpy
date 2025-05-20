#!/usr/bin/env python3

import numpy as np
import sys
import logging


# Read a 32-bit integer from a buffer
def read32(b):  # -> int:
    return (b[0] << 24) | (b[1] << 16) | (b[2] << 8) | (b[3] << 0)


# Read a 24-bit integer from a buffer
def read24(b):  # -> int:
    return (b[0] << 16) | (b[1] << 8) | (b[2] << 0)


# Read a 16-bit integer from a buffer
def read16(b):  # -> int:
    return (b[0] << 8) | (b[1] << 0)


# Read a 32-bit integer from a stream
def fread32(f):  # -> int:
    b = f.read(4)
    return read32(b)


# Read a 32-bit float from a stream
def freadfloat(f):  # -> float:
    x = fread32(f)
    return float(x)


def readchar(dest_bytes, src, n):  # 4 cas, la cle est {j:sh}
    for i in range(0, n):  # (int i = 0; i < n; i++)
        dest_bytes[i] = 0
        # first byte needed
        j = (i * 6) // 8
        # shift from the beginning
        sh = (i * 6) % 8
        dest_bytes[i] |= ((src[j] << sh) & 0xFF) >> 2
        # do we need a second byte?
        if sh > 2:
            dest_bytes[i] |= src[j + 1] >> (10 - sh)
        dest_bytes[i] += 32


class FileHeader:
    file_size = 0
    num_overwrites = 0
    num_extensions = 0
    nchunks = 0
    last_chunk = 0
    max_data_length = 0
    num_erasures = 0
    nrecs = 0


class ChunkHeader:
    this_chunk_words = 0
    this_chunk = 0
    next_chunk_words = 0
    next_chunk = 0
    nrecs = 0
    checksum = 0


class RecordHeader:
    status = 0
    size = 0
    data = 0
    deet = 0
    npak = 0
    ni = 0
    grtyp = ""
    nj = 0
    datyp = 0
    nk = 0
    npas = 0
    ig4 = 0
    ig2 = 0
    ig1 = 0
    ig3 = 0
    etiket = ""
    typvar = ""
    nomvar = ""
    ip1 = 0
    ip2 = 0
    ip3 = 0
    dateo = 0
    checksum = 0
    ubc = 0
    swa = 0
    dltf = 0


def print_file_header(h):
    logging.info(
        "file_size: %d, num_overwrites: %d, num_extensions: %d, nchunks: %d, last_chunk: %x, max_data_length: %d, num_erasures: %d, nrecs: %d"
        % (
            h.file_size,
            h.num_overwrites,
            h.num_extensions,
            h.nchunks,
            h.last_chunk,
            h.max_data_length,
            h.num_erasures,
            h.nrecs,
        )
    )


def print_chunk_header(h):
    logging.info(
        "this_chunk: %x, next_chunk: %x, nrecs: %d, checksum: %x" % (h.this_chunk, h.next_chunk, h.nrecs, h.checksum)
    )


def print_record_header(i, h):
    logging.info(
        "%5d- status: %d, size: %d, data pointer: %x, deet: %d, npak: %d, ni: %d, grtyp: '%c', nj: %d, datyp: %d nk: %d, npas: %d, ig4: %d, ig2: %d, ig1: %d, ig3: %d, etiket: '%s', typvar: '%s', nomvar: '%s', ip1: %d, ip2: %d, ip3: %d, dateo: %d, swa: %d, ubc: %d, dltf: %d"
        % (
            i,
            h.status,
            h.size,
            h.data,
            h.deet,
            h.npak,
            h.ni,
            h.grtyp,
            h.nj,
            h.datyp,
            h.nk,
            h.npas,
            h.ig4,
            h.ig2,
            h.ig1,
            h.ig3,
            h.etiket,
            h.typvar,
            h.nomvar,
            h.ip1,
            h.ip2,
            h.ip2,
            h.dateo,
            h.swa,
            h.ubc,
            h.dltf,
        )
    )


def print_record_header1(i, h):
    logging.info(
        "%5d- %-4s %-2s %-12s %8d%8d%6d%10d %19d %9d%10d%9d%9d %3d %2d %c %5d %5d %5d %5d"
        % (
            i,
            h.nomvar,
            h.typvar,
            h.etiket,
            h.ni,
            h.nj,
            h.nk,
            h.dateo,
            h.ip1,
            h.ip2,
            h.ip3,
            h.deet,
            h.npas,
            h.datyp,
            h.npak,
            h.grtyp,
            h.ig1,
            h.ig2,
            h.ig3,
            h.ig4,
        )
    )


def read_file_header(f):  # -> FileHeader:
    h = FileHeader()
    buf = f.read(208)
    # print(buf)
    buf[0]
    assert read24(buf[1:]) == 26  # header length (words) 26
    assert read32(buf[4:]) == 0  # address of file header 0
    assert buf[8 : 8 + 4] == b"XDF0"  # XDF version
    assert buf[12 : 12 + 4] == b"STDR"  # application signature
    h.file_size = read32(buf[16:]) * 8
    h.num_overwrites = read32(buf[20:])
    h.num_extensions = read32(buf[24:])
    h.nchunks = read32(buf[28:])
    h.last_chunk = read32(buf[32:]) * 8
    #     print(h.file_size,h.num_extensions,h.nchunks,h.last_chunk)
    assert read16(buf[40:]) == 16  # # of primary keys
    assert read16(buf[42:]) == 9  # length of primary keys (words)
    assert read16(buf[44:]) == 2  # # of auxiliary keys
    assert read16(buf[46:]) == 1  # length of auxiliary keys (words)
    h.num_erasures = read32(buf[48:])
    h.nrecs = read32(buf[52:])
    assert read32(buf[56:]) == 0  # read/write flag
    assert read32(buf[60:]) == 0  # reserved area
    # print('h.file_size,h.num_extensions,h.nchunks,h.last_chunk,h.num_erasures,h.nrecs')
    # print(h.file_size,h.num_extensions,h.nchunks,h.last_chunk,h.num_erasures,h.nrecs)
    # Validate primary keys
    for i in range(0, 16):  # (int i = 0 i < 16 i++)
        ncle = b"SF%02d" % (i + 1)
        offset = 64 + i * 8
        # print(buf[offset:offset+4])
        assert buf[offset : offset + 4] == ncle  # validate key names
        assert (read16(buf[offset + 4 :]) >> 3) == 31 + i * 32  # validate bit1
        assert (read24(buf[offset + 5 :]) & 0x7FFFF) == 0x7C000  # validate lcls/tcle
    # Validate auxiliary keys
    for i in range(0, 2):  # (int i = 0 i < 2 i++)
        ncle = b"AXI%01d" % (i + 1)
        offset = 192 + i * 8
        assert buf[offset : offset + 4] == ncle  # validate key names
        assert (read16(buf[offset + 4 :]) >> 3) == 31 + i * 32  # validate bit1
        assert (read24(buf[offset + 5 :]) & 0x7FFFF) == 0x7C000  # validate lcls/tcle
    print_file_header(h)
    return h


def read_chunk_header(f):  # -> ChunkHeader:
    buf = f.read(32)
    import binascii

    # print( [int(i) for i in buf])
    assert buf[0] == 0  # idtyp
    assert read24(buf[1:]) == 2308  # Header length (words)
    h = ChunkHeader()
    h.this_chunk_words = read32(buf[4:])
    h.this_chunk = h.this_chunk_words * 8 - 8
    assert read32(buf[8:]) == 0  # reserved1
    assert read32(buf[12:]) == 0  # reserved2
    h.next_chunk_words = read32(buf[16:])
    h.next_chunk = h.next_chunk_words * 8
    if h.next_chunk > 0:
        h.next_chunk -= 8  # Rewind a bit
    h.nrecs = read32(buf[20:])
    h.checksum = read32(buf[24:])
    assert read32(buf[28:]) == 0  # reserved3
    print_chunk_header(h)
    return h


#    word swa : 32, epce1 : 4, nk : 12, npas1 : 16;
#    word nj : 16, ni : 16, nbits : 8, typvar : 8, nomvar : 16;
#    word ip2 :16, ip1 : 16, npas2 : 8, dltf : 1, epce2 : 7, ip3 : 16;
#    word etiq14 :32, etiq78 : 16, etiq56 : 16;
#    word epce3 : 32, ig2 : 16, epce4 : 16;
#    word ig4 : 16, ig3 : 16, ig1 : 16, datyp : 8, grtyp : 8;
#    word date : 32, deet : 16, ubc : 16;
#    word lng : 32;


def read_record_header(f):  # -> RecordHeader:
    h = RecordHeader()
    buf = f.read(72)

    h.status = buf[0]

    h.size = read24(buf[1:])  # 1,2,3

    h.data = read32(buf[4:]) * 8  # 4,5,6,7
    assert h.data > 8
    h.data -= 8  # rewind a bit to get the proper start of the data

    h.deet = read24(buf[8:])  # 8,9,10  lenght=16

    h.npak = buf[11]  # lenght=8

    h.ni = read24(buf[12:])  # 12,13,14 lenght=16

    h.grtyp = buf[15]  # lenght=8

    h.nj = read24(buf[16:])  # 16,17,18 lenght=16

    h.datyp = buf[19]  # lenght=8

    h.nk = read24(buf[20:]) >> 4  # 20,21,22 lenght=12

    # h.ubc = read24(buf[23:]) #lenght=16
    # TODO: ubc

    h.npas = read32(buf[24:]) >> 6  # 24,25,26,27 lenght=8

    h.ig4 = read24(buf[28:])  # 28,29,30 lenght=16

    h.ig2 = buf[31] * 65536 + buf[35] * 256 + buf[39]  # lenght=16

    h.ig1 = read24(buf[32:])  # 32,33,34 lenght=16

    h.ig3 = read24(buf[36:])  # 36,37,38 lenght=16

    h.etiket = np.empty((12), dtype="ubyte")
    readchar(h.etiket, buf[40:], 5)  # 40,41,42,43
    readchar(h.etiket[5:], buf[44:], 5)  # 44,45,46,47
    readchar(h.etiket[10:], buf[48:], 2)  # 48
    h.etiket = "".join(map(chr, h.etiket)).strip()

    h.typvar = np.empty((2), dtype="ubyte")
    h.typvar[0] = ((buf[49] & 0x0F) << 2) + (buf[50] >> 6) + 32  # 0xdeadbeef 0x0f = 00001111    #0xdeadbeef & 0x0f ->
    h.typvar[1] = (buf[50] & 0x3F) + 32
    h.typvar = "".join(map(chr, h.typvar)).strip()
    ####################
    # 51 not used?
    ####################
    # h.swa = read32(buf[23:])

    h.nomvar = np.empty((4), dtype="ubyte")
    readchar(h.nomvar, buf[52:], 4)  # 52,53,54
    h.nomvar = "".join(map(chr, h.nomvar)).strip()

    ####################
    # 55 not used?
    ####################
    # h.dltf = buf[55]>>7

    h.ip1 = read32(buf[56:]) >> 4  # 56,57,58,59

    h.ip2 = read32(buf[60:]) >> 4  # 60,61,62,63

    h.ip3 = read32(buf[64:]) >> 4  # 64,65,66,67

    h.dateo = read32(buf[68:])  # 68,69,70,71

    # Checksum
    h.checksum = 0
    for i in range(0, 72, 4):  # (int i = 0 i < 72 i+= 4)
        h.checksum ^= (buf[i] << 24) | (buf[i + 1] << 16) | (buf[i + 2] << 8) | buf[i + 3]

    # print_record_header1(h)
    return h


def get_record_headers(filename):  # -> []:
    f = open(filename, "rb")
    fileheader = read_file_header(f)

    chunkheaders = []
    rec = 0
    all_recs = []
    for c in range(0, fileheader.nchunks):  # (int c = 0 c < fileheader.nchunks c++)
        # chunckheader = ChunkHeader()
        chunckheader = read_chunk_header(f)
        chunkheaders.append(chunckheader)
        checksum = 0
        recordheaders = []
        for r in range(0, chunckheader.nrecs):  # (int r = 0 r < chunkheader.nrecs r++)
            # h = RecordHeader()
            h = read_record_header(f)
            recordheaders.append(h)
            all_recs.append(h)
            # Skip erased records
            if recordheaders[rec].status == 255:
                continue
            checksum ^= recordheaders[rec].checksum
            rec += 1
        rec = 0
        checksum ^= chunckheader.nrecs
        checksum ^= chunckheader.next_chunk_words
        #         print("chunk %d checksum: 0x%08X   computed checksum: 0x%08X, xor: %08X\n", c, chunkheader.checksum, checksum, chunkheader.checksum ^ checksum)
        assert checksum == chunckheader.checksum
        # Go to next chunk
        f.seek(chunckheader.next_chunk, 0)
    f.close()
    return all_recs


if __name__ == "__main__":
    if len(sys.argv) == 2:
        path = sys.argv[1]  #'/fs/site4/eccc/cmd/w/sbf000/source_data_5005.std'
    else:
        logging.critical("please provide a valid file")
        sys.exit()
    # print(f'voir on {path}')

    h_list = get_record_headers(path)
    i = 0
    logging.info(
        "       NOMV TV   ETIQUETTE        NI      NJ    NK (DATE-O  h m s)           IP1       IP2       IP3     DEET     NPAS  DTY   G   IG1   IG2   IG3   IG4"
    )
    for rec in h_list:
        # print_record_header1(i,rec)
        i += 1

    # import shutil
    # print(shutil.get_terminal_size().lines)
    # h = get_record_headers('/space/hall5/sitestore/eccc/prod/hubs/gridpt/dbase/prog/glbeta/2021031912_240')


#     // Read a chunk of data
# int read_data (char *filename, int nrecs, RecordHeader *headers, int recsize, float *out) {

#   FILE *file = fopen (filename, "rb");

#   // Loop over all records
#   for (int rec = 0; rec < nrecs; rec++) {

#       unsigned long long offset = headers[rec].data;
# //      printf ("offset: %llx\n", offset);
#       RecordHeader h;
#       fseek (file, offset, SEEK_SET);
#       read_record_header (file, &h);
# //      print_record_header (&h);
#       // Make sure the header matches
#       assert (receq(headers+rec, &h) == 1);
#       assert (h.datyp == 1 || h.datyp == 5 || h.datyp == 133); //currently only support packed floating-point/IEEE floating point/compressed IEEE floating point

#       byte b[4];
#       fread (b, 1, 4, file);
#       assert (read32(b) == 0);
#       fread (b, 1, 4, file);
#       assert (read32(b) == 0);

#       // Easiest case: 32-bit IEEE float
#       if (h.datyp == 5) {
#         assert (h.npak == 32);  // must be 32-bit?!
#         assert (sizeof(float) == sizeof(uint32_t));
#         byte *raw = malloc(4*recsize);
#         fread (raw, 4, recsize, file);
#         for (int i = 0; i < recsize; i++) {
#           ((uint32_t*)(out))[i] = read32(raw+4*i);
#         }
#         free(raw);
# //        for (int i = 0; i < recsize; i++) {
# //          printf ("%g  ", out[i]);
# //        }
# //        printf ("\n");
#       }

#       // Compressed?
#       else if (h.datyp == 133) {
#         unsigned char buf[h.size*8];
#         fread (buf, 1, h.size*8, file);
#         read_compress32 (buf, &h, recsize, out);
#       }

#       // Other supported case: packed float
#       else if (h.datyp == 1) {

#         // Get and validate record size
#         fread (b, 1, 4, file);
#         unsigned int marker_and_recsize = read32(b);
#         int marker = (marker_and_recsize >> 20);
#         int recsize_ = marker_and_recsize & 0xFFFFF;
#         assert (marker == 0x7ff);  // Check if supported header type
#         assert (recsize_ == recsize);

#         // Get the exponent used in the encoding
#         fread (b, 1, 2, file);
#         int range_exp = read16(b) - 4096;

#         // Get the min value exponent and sign
#         fread (b, 1, 2, file);
#         int min_expo_sign = read16(b);
#         int min_expo = (min_expo_sign >> 4) + 127 - 1024 + 48;  // based on compact_h.c
#         int min_sign = min_expo_sign & 0x000F;

#         // Get the min value mantissa
#         fread (b, 1, 4, file);
#         int min_mantissa = read24(b);  // skipping last byte (not used in 32-bit float encoding)
#         assert (b[3] == 0);  // the last byte in the encoded mantissa should be zero?

#         // Construct the min value
#         // note: already includes the '1' in '1.xxxxx' (24th bit)
#         float min = (min_mantissa / 8388608.) * pow(2,min_expo-127);
#         if (min_sign == 1) min *= -1;
#         // Special case - min is 0
#         //if (min_mantissa == 0 || min_expo < 849) min = 0;

#         // Skip over the 16-bit mantissa argument (not used?)
#         fread (b, 1, 2, file);
#         assert (b[0] == 0 && b[1] == 0);  // don't know what to do with a 16-bit mantissa

#         // Get and verify npak
#         fread (b, 1, 1, file);
#         int npak = b[0];
#         assert (npak == h.npak);
# //        printf ("npak: %d\n", npak);

#         // Fast case: pack=16
#         if (npak == 16) {
#           byte *raw = malloc(2*recsize);
#           fread (raw, 2, recsize, file);
#           for (int i = 0; i < recsize; i++) {
#             out[i] = read16(raw+2*i);
#           }
#           free (raw);
#         }

#         // Fast case: pack=24
#         else if (npak == 24) {
#           byte *raw = malloc(3*recsize);
#           fread (raw, 3, recsize, file);
#           for (int i = 0; i < recsize; i++) {
#             out[i] = read24(raw+3*i);
#           }
#           free (raw);
#         }

#         // Fast case: pack=32
#         else if (npak == 32) {
#           byte *raw = malloc(4*recsize);
#           fread (raw, 4, recsize, file);
#           for (int i = 0; i < recsize; i++) {
#             out[i] = read32(raw+4*i);
#           }
#           free (raw);
#         }

#         // Slow case: other packing density
#         else {
#           printf ("warning: slow unpacking!\n");
#           // Read the data into a buffer
#           byte *raw = malloc(recsize * npak / 8);
#           byte *bits = malloc(recsize * npak);
#           unsigned int *codes = malloc(sizeof(unsigned int) * recsize);
#           // First, read in bytes
#           fread (raw, 1, recsize*npak/8, file);
#           // Next, expand bytes into bit array
#           for (int i = 0; i < recsize * npak / 8; i++) {
#             byte x = raw[i];
#             for (int j = 7; j >= 0; j--) {
#               bits[i*8+j] = x & 0x01;
#               x >>= 1;
#             }
#           }
#           // Now, collapse this back into an integer code of size <npak>
#           for (int i = 0; i < recsize; i++) {
#             unsigned int x = 0;
#             for (int j = 0; j < npak; j++) x = (x<<1) + bits[i*npak + j];
#             codes[i] = x;
# //            printf ("%d ", x);
#           }
# //          printf ("\n");
#           // Decode this into a float
#           for (int i = 0; i < recsize; i++) {
#             out[i] = codes[i];
#           }
# //          printf ("\n");
#           free (codes);
#           free (bits);
#           free (raw);
#         }

#         // Finish decoding the values
#         float mulfactor = pow(2,range_exp);
#         for (int i = 0; i < recsize; i++) {

#           out[i] = out[i] * mulfactor + min;
#           // note: original code (compact_h.c) multiplies mulfactor by 1.0000000000001.  why???

#         }

#       }


#       out += recsize;
#   }

#   fclose (file);

#   return 0;
# }
