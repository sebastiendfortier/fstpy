# -*- coding: utf-8 -*-
import datetime
from test import TEST_PATH, TMP_PATH

import pytest
from ci_fstcomp import fstcomp
from fstpy.dataframe_utils import select_with_meta
from fstpy.std_reader import StandardFileReader
from fstpy.std_writer import StandardFileWriter
from fstpy.utils import delete_file
from rpnpy.librmn.all import FSTDError

pytestmark = [pytest.mark.std_reader_regtests, pytest.mark.regressions]

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"ReaderStd/testsFiles/"

def test_1(plugin_test_dir):
    """Test l'option --input avec un fichier qui n'existe pas!"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc_.std"
    with pytest.raises(FSTDError):
        _ = StandardFileReader(source0).to_pandas()


def test_2(plugin_test_dir):
    """Test avec un fichier qui possede un champ de type entier."""
    # open and read source
    source0 = plugin_test_dir + "regdiag_2012061300_012_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    src_df0 = select_with_meta(src_df0,["UU","VV","T6"])
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [Select --fieldName UU,VV,T6] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_2.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UU_VV_T6_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_3(plugin_test_dir):
    """Test read write small"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #write the result
    results_file = TMP_PATH + "test_read_reg_3.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UUVV5x5_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.001)
    delete_file(results_file)
    assert(res)


def test_5(plugin_test_dir):
    """Test read write big"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_5.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    src_df0 = StandardFileReader(results_file).to_pandas()
    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.13)#,e_c_cor=0.001)
    # delete_file(results_file)
    assert(res)


def test_6(plugin_test_dir):
    """Test read write sigma12000 pressure"""
    # open and read source
    source0 = plugin_test_dir + "input_model"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = select(src_df0,'nomvar in ["UU","VV","TT"]')
    df = select_with_meta(src_df0,["UU","VV","TT"])
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [Select --fieldName UU,VV,TT] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_6.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "sigma12000_pressure_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_7(plugin_test_dir):
    """Test read write big noMetadata"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --noMetadata --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_7.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0,no_meta=True).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_noMeta_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


# def test_8(plugin_test_dir):
#     """Test read file with duplicated grid"""
#     # open and read source
#     source0 = plugin_test_dir + "fstdWithDuplicatedGrid_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()

#     #compute ReaderStd
#     # df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_read_reg_8.std"
#     delete_file(results_file)
#     StandardFileWriter(results_file, src_df0).to_fst()
#     print(results_file)
#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "reference_file_test_8.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res)


def test_9(plugin_test_dir):
    """Test read write 64bit"""
    # open and read source
    source0 = plugin_test_dir + "tt_stg_fileSrc.std+20210517"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

    #write the result
    results_file = TMP_PATH + "test_read_reg_9.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "tt_stg_fileSrc.std+20210517"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_10(plugin_test_dir):
    """Test read 3 file"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    source1 = plugin_test_dir + "windChill_file2cmp.std"
    source2 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader([source0,source1,source2]).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]} {sources[1]} {sources[2]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_10.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "stdPlusstd_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_11(plugin_test_dir):
    """Test read write ip3"""
    # open and read source
    source0 = plugin_test_dir + "ip3.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_11.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "ip3.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=.13)
    delete_file(results_file)
    assert(res)


def test_12(plugin_test_dir):
    """Test read write ip1 mb newstyle"""
    # open and read source
    source0 = plugin_test_dir + "UUVV93423264_hyb_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

    #write the result
    results_file = TMP_PATH + "test_read_reg_12.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UUVV93423264_hyb_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_13(plugin_test_dir):
    """Test for file containing 2 HY"""
    # open and read source
    source0 = plugin_test_dir + "2hy.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --noUnitConversion]

    #write the result
    results_file = TMP_PATH + "test_read_reg_13.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # # open and read comparison file
    # file_to_compare = plugin_test_dir + "nan"

    # #compare results
    # res = fstcomp(results_file,file_to_compare)
    # assert(res)


def test_14(plugin_test_dir):
    """Test reading fields with typvar == PZ"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PZ.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #['[ReaderStd --ignoreExtended --input {sources[0]}] >> ', '[WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]']

    #write the result
    results_file = TMP_PATH + "test_read_reg_14.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "NEW/typvar_pz_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_15(plugin_test_dir):
    """Test reading fields with typvar == PU"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PU.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_15.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "NEW/typvar_pu_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_16(plugin_test_dir):
    """Test reading fields with typvar == PI"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PI.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_16.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "NEW/typvar_pi_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_17(plugin_test_dir):
    """Test reading fields with typvar == PF"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PF.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_17.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "NEW/typvar_pf_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_18(plugin_test_dir):
    """Test reading fields with typvar == PM"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PM.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_18.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "NEW/typvar_pm_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_19(plugin_test_dir):
    """Test if HY is put in memory and written back when we have a grid with two kinds of level, one of them being hybrid"""
    # open and read source
    source0 = plugin_test_dir + "mb_plus_hybrid_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0['etiket'] = '33K80___X'

    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_read_reg_19.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "mb_plus_hybrid_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_20(plugin_test_dir):
    """Test if HY is put in memory and written back when we have a grid with hybrid level"""
    # open and read source
    source0 = plugin_test_dir + "mb_plus_hybrid_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0,['FN'])
    src_df0.loc[:,'etiket'] = '33K80___X'


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName FN] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_read_reg_20.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "read_write_hy2_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_21(plugin_test_dir):
    """Test that the HY is NOT written back when the final grid don't have a hybrid level"""
    # open and read source
    source0 = plugin_test_dir + "mb_plus_hybrid_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0,['PR'])
    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName PR] >> [WriterStd --output {destination_path} --ignoreExtended]

    src_df0.loc[:,'etiket'] = 'K80'

    #write the result
    results_file = TMP_PATH + "test_read_reg_21.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "read_write_hy3_file2cmp.std+20210517"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_22(plugin_test_dir):
    """Test that PT is NOT read by the reader when the level type of the fields on the grid is not sigma"""
    # open and read source
    source0 = plugin_test_dir + "pt_with_hybrid.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_read_reg_22.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "read_write_pt_when_no_sigma_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_23(plugin_test_dir):
    """Test that PT is NOT written back when there is a PT field created in memory and the level type of the fields on the grid is not sigma"""
    # open and read source
    source0 = plugin_test_dir + "kt_ai_hybrid.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0.loc[src_df0.nomvar == 'AI','nomvar'] = 'PT'
    # src_df0 = select_zap(src_df0,'nomvar=="AI"',nomvar='PT')

    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --input {sources[0]}] >> [ZapSmart --fieldNameFrom AI --fieldNameTo PT] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_read_reg_23.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "read_write_pt_when_no_sigma_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


# def test_25(plugin_test_dir):
#     """!!!! I dont get this test !!!! Test la lecture avec ip2 != deet * npas"""
#     # open and read source
#     source0 = plugin_test_dir + "2012121000_cancm3_m1_00_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()


#     #compute ReaderStd
#     # df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path} --writingMode APPEND]

#     #write the result
#     results_file = TMP_PATH + "test_read_reg_25.std"
#     StandardFileWriter(results_file, src_df0).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "2012121000_cancm3_m1_00_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res)


def test_26(plugin_test_dir):
    """Test la lecture d'un fichier pilot """
    # open and read source
    source0 = plugin_test_dir + "2015040800_030_piloteta"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path} ]

    #write the result
    results_file = TMP_PATH + "test_read_reg_26.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "2015040800_030_piloteta"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_28(plugin_test_dir):
    """Test lecture fichiers contenant caracteres speciaux ET parametre --input n'est pas le dernier"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_+fileSrc.std"
    source1 = plugin_test_dir + "wind+Chill_file2cmp.std"
    source2 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader([source0,source1,source2]).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #[ReaderStd --input {sources[0]} {sources[1]} {sources[2]} --ignoreExtended] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_read_reg_28.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "stdPlusstd_file2cmp.std"

    #compare results
    res = fstcomp(results_file, file_to_compare)
    delete_file(results_file)
    assert(res)


def test_29(plugin_test_dir):
    """Test lecture fichiers contenant des champs de donnees manquantes"""
    # open and read source
    source0 = plugin_test_dir + "missingData.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #['[ReaderStd --input {sources[0]} --ignoreExtended] >> ', '[WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE --flagMissingData]']

    #write the result
    results_file = TMP_PATH + "test_read_reg_29.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resulttest_29.std+20210712"

    #compare results
    # toctoc is present in cmp file, disable strick meta
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_30(plugin_test_dir):
    """Test lecture fichiers contenant des membres d'ensemble differents"""
    # open and read source
    source0 = plugin_test_dir + "ensemble_members.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    for i in src_df0.index:
        src_df0.at[i,'etiket'] = ''.join(['E16_0_0_',src_df0.at[i,'etiket'][-4:]])

    src_df0.loc[src_df0.nomvar.isin(['>>','^^']),'etiket'] = 'ER______X'
    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #['[ReaderStd --input {sources[0]}] >> ', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #write the result
    results_file = TMP_PATH + "test_read_reg_30.std"
    delete_file(results_file)

    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resulttest_30.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_31(plugin_test_dir):
    """Test lecture fichiers contenant des masques"""
    # open and read source
    source0 = plugin_test_dir + "data_with_mask.std"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()

    #['[ReaderStd --input {sources[0]}] >> ', '[Select --forecastHour 24] >>', '[WriterStd --output {destination_path}]']
    src_df0 = src_df0.loc[(src_df0.dateo==442080800) & (src_df0.deet==300) & (src_df0.npas==288)]
    src_df0.loc[:,'etiket']='RU210RKFX'
    src_df0.loc[:,'ip2'] = 24
    # print(src_df0[['nomvar','typvar','etiket','ni','nj','nk','dateo','ip1','ip2','ip3','deet','npas','datyp','nbits','grtyp','ig1','ig2','ig3','ig4']].to_string())
    #write the result
    results_file = TMP_PATH + "test_read_reg_31.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0, rewrite=True).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resulttest_31.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,columns=['nomvar', 'typvar', 'etiket','ni', 'nj', 'nk', 'dateo', 'ip1', 'ip2', 'ip3', 'deet', 'npas', 'grtyp', 'ig1', 'ig2', 'ig3', 'ig4'])
    delete_file(results_file)
    assert(res)


def test_32(plugin_test_dir):
    """Test lecture fichiers contenant des membres d'ensemble differents"""
    # open and read source
    source0 = plugin_test_dir + "ens_data_exclamation.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0 = select_with_meta(src_df0,["WGEX"])

    src_df0.loc[src_df0.nomvar.isin(['>>','^^']),'etiket'] = 'ER______X'
    #compute ReaderStd
    # df = ReaderStd(src_df0)
    #['[ReaderStd --input {sources[0]}] >>', '[Select --fieldName WGEX] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

    #write the result
    results_file = TMP_PATH + "test_read_reg_32.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resulttest_32.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)


def test_33(plugin_test_dir):
    """Test lecture fichiers contenant la coordonnee 5005"""
    # open and read source
    source0 = plugin_test_dir + "resulttest_33.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    #compute ReaderStd
    # src_df0 = ReaderStd(src_df0)
    #['[ReaderStd --input {sources[0]}]>>', '[WriterStd --output {destination_path}]']

    #write the result
    results_file = TMP_PATH + "test_read_reg_33.std"
    delete_file(results_file)
    StandardFileWriter(results_file, src_df0).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "resulttest_33.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res)
