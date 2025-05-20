#!/usr/bin/env python3
import pandas as pd
import numpy as np
import shutil
# print(shutil.get_terminal_size().lines)
# Quick and dirty reading of FST record headers.
# Use at own risk.

pd.set_option(
    "display.max_rows", None, "display.max_columns", None, "display.width", shutil.get_terminal_size().columns
)


def read_headers(filename):
    # file_index_list = []
    # pageno_list = []
    # recno_list = []
    with open(filename, "rb") as f:
        # Read file header and first page.
        head = np.fromfile(f, ">i4", 4668)
        if head.view("|S4")[3] != b"STDR":
            raise TypeError("Not an FST file")
        # Collect all header pages together (raw format).
        pages = []
        current_page = head[52:]
        while True:
            nrecs = current_page[5]
            # Get the raw headers (with page header stripped out).
            pages.append(current_page[8 : 8 + nrecs * 18].reshape(-1, 9, 2).view("i4"))
            # Address of next page (in 64-bit units)
            next_page_addr = int(current_page[4])
            if next_page_addr == 0:
                break
            f.seek(next_page_addr * 8 - 8, 0)
            current_page = np.fromfile(f, ">i4", 8 + 256 * 18)

            # recno_list.extend(list(range(nent)))
            # pageno_list.extend([pageno]*nent)
            # file_index_list.extend([index]*nent)

    raw = np.concatenate(pages, axis=0).view(">i4")
    # Decode the raw headers.
    nrecs = raw.shape[0]
    out = {}
    out["dltf"] = raw.view("B")[:, 0, 0] >> 7
    # out['lng'] = raw[:,0,0]
    # out['lng'] &= 0x00FFFFFF
    # out['swa'] = raw[:,0,1]
    out["nbits"] = raw.view("B")[:, 1, 3].copy()
    out["deet"] = raw[:, 1, 0]
    out["deet"] >>= 8
    out["grtyp"] = raw.view("|S1")[:, 1, 7].copy()
    out["ni"] = raw[:, 1, 1]
    out["ni"] >>= 8
    out["datyp"] = raw.view("B")[:, 2, 3].copy()
    out["nj"] = raw[:, 2, 0]
    out["nj"] >>= 8
    out["nk"] = raw[:, 2, 1]
    out["nk"] >>= 12
    out["npas"] = raw[:, 3, 0]
    out["npas"] >>= 6
    out["ig2"] = raw.view("B")[:, 3, 7].astype(">i4")
    out["ig4"] = raw[:, 3, 1]
    out["ig4"] >>= 8
    out["ig2"] <<= 8
    out["ig2"] |= raw.view("B")[:, 4, 3]
    out["ig1"] = raw[:, 4, 0]
    out["ig1"] >>= 8
    out["ig2"] <<= 8
    out["ig2"] |= raw.view("B")[:, 4, 7]
    out["ig3"] = raw[:, 4, 1]
    out["ig3"] >>= 8
    # Unpack the etiket, typvar, and nomvar.
    packed = raw.reshape(nrecs, -1)[:, 10:14]
    packed >>= 2
    s = np.empty((nrecs, 20), "B")
    for i in range(5):
        s[:, 4 - i : 20 : 5] = packed
        if i < 4:
            packed >>= 6
    s &= 0x3F
    s += 0x20
    out["etiket"] = s[:, :12].copy().view("|S12").flatten()
    out["typvar"] = s[:, 12:14].copy().view("|S2").flatten()
    out["nomvar"] = s[:, 15:19].copy().view("|S4").flatten()
    out["ip1"] = raw[:, 7, 0]
    out["ip1"] >>= 4
    out["ip2"] = raw[:, 7, 1]
    out["ip2"] >>= 4
    out["ip3"] = raw[:, 8, 0]
    out["ip3"] >>= 4
    dt = np.empty(nrecs, ">i4")
    out["datev"] = np.empty(nrecs, ">i4")
    np.divmod(raw[:, 8, 1], 8, out["datev"], dt)
    out["datev"] *= 10
    out["datev"] += dt
    dt[:] = out["deet"]
    dt *= out["npas"]
    dt //= 5
    check = dt.copy()
    raw[:, 8, 1] -= dt
    out["dateo"] = np.empty(nrecs, ">i4")
    np.divmod(raw[:, 8, 1], 8, out["dateo"], dt)
    out["dateo"] *= 10
    out["dateo"] += dt
    # Building block for record handles (keys).
    #
    # out['key'][:] = (np.array(file_index_list)&0x3FF) | ((np.array(recno_list)&0x1FF)<<10) | ((np.array(pageno_list)&0xFFF)<<19)
    r = np.arange(nrecs)
    out["key"] = ((r % 256) << 10) | ((r // 256) << 19)
    #
    # Note: to use the above keys, you'll need to offset by the file index.
    #       To find the file index, take a valid librmn key and mask by
    #       0x3FF.

    # Construct pandas dataframe and filter out deleted records
    out["nomvar"] = np.char.strip(out["nomvar"].astype("str"))
    out["typvar"] = np.char.strip(out["typvar"].astype("str"))
    out["etiket"] = np.char.strip(out["etiket"].astype("str"))
    out["grtyp"] = np.char.strip(out["grtyp"].astype("str"))
    df = pd.DataFrame(out)
    df = df[
        [
            "nomvar",
            "typvar",
            "etiket",
            "ni",
            "nj",
            "nk",
            "dateo",
            "ip1",
            "ip2",
            "ip3",
            "deet",
            "npas",
            "datyp",
            "nbits",
            "grtyp",
            "ig1",
            "ig2",
            "ig3",
            "ig4",
            "datev",
            "dltf",
            "key",
        ]
    ]
    df.query("dltf == 0", inplace=True)
    df.drop(columns=["dltf", "datev"], inplace=True, errors="ignore")
    return df


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Spit out FST record info.")
    parser.add_argument("fstfile", help="The file to look at.")
    args = parser.parse_args()
    print(read_headers(args.fstfile))
