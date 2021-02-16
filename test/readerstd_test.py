# -*- coding: utf-8 -*-
import pytest
from fstpy.std_reader import StandardFileReaderError, StandardFileReader, StandardFileWriter, load_data, select, fstcomp
from fstpy.utils import delete_file
from test import TEST_PATH, TMP_PATH

pytestmark = [pytest.mark.std_reader_regtests, pytest.mark.regressions]


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"ReaderStd/testsFiles/"

def test_regtest_1(plugin_test_dir):
    """Test #1 : Test l'option --input avec un fichier qui n'existe pas!"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc_.std"
    with pytest.raises(FileNotFoundError):
        src_df0 = StandardFileReader(source0)()
 

def test_regtest_2(plugin_test_dir):
    """Test #2 : Test avec un fichier qui possède un champ de type entier."""
    # open and read source
    source0 = plugin_test_dir + "regdiag_2012061300_012_fileSrc.std"
    src_df0 = StandardFileReader(source0)()


    #compute ReaderStd
    df = select(src_df0,'nomvar in ["UU","VV","T6"]')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [Select --fieldName UU,VV,T6] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_2.std"
    StandardFileWriter(results_file, df)()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UU_VV_T6_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_3(plugin_test_dir):
    """Test #3 : test_read_write_small"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    src_df0 = StandardFileReader(source0)()

    #write the result
    results_file = TMP_PATH + "test_3.std"
    StandardFileWriter(results_file, src_df0)()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UUVV5x5_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_5(plugin_test_dir):
    """Test #5 : test_read_write_big"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = StandardFileReader(source0)()

    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_5.std"
    StandardFileWriter(results_file, src_df0)()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_6(plugin_test_dir):
    """Test #6 : test_read_write_sigma12000_pressure"""
    # open and read source
    source0 = plugin_test_dir + "input_model"
    src_df0 = StandardFileReader(source0)()


    #compute ReaderStd
    df = select(src_df0,'nomvar in ["UU","VV","TT"]')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [Select --fieldName UU,VV,TT] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_6.std"
    StandardFileWriter(results_file, df)()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "sigma12000_pressure_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


# def test_regtest_7(plugin_test_dir):
#     """Test #7 : test_read_write_big_noMetadata"""
#     # open and read source
#     source0 = plugin_test_dir + "input_big_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()

#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --noMetadata --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_7.std"
#     StandardFileWriter(results_file, src_df0, add_meta_fields=False)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "input_big_noMeta_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res == True)


# def test_regtest_8():
#     """Test #8 : test_read_file_with_duplicated_grid"""
#     # open and read source
#     source0 = plugin_test_dir + "fstdWithDuplicatedGrid_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_8.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "reference_file_test_8.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_9():
#     """Test #9 : test_read_write_64bit"""
#     # open and read source
#     source0 = plugin_test_dir + "tt_stg_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_9.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "tt_stg_fileSrc.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_10():
#     """Test #10 : test_read_2_file"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()

#     source1 = plugin_test_dir + "windChill_file2cmp.std"
#     src_df1 = StandardFileReader(source1)

#     source2 = plugin_test_dir + "windModulus_file2cmp.std"
#     src_df2 = StandardFileReader(source2)


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]} {sources[1]} {sources[2]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_10.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "stdPlusstd_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_11():
#     """Test #11 : test_read_write_ip3"""
#     # open and read source
#     source0 = plugin_test_dir + "ip3.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_11.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "ip3.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_12():
#     """Test #12 : test_read_write_ip1_mb_newstyle"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVV93423264_hyb_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_12.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVV93423264_hyb_fileSrc.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_13():
#     """Test #13 : test for file containing 2 HY"""
#     # open and read source
#     source0 = plugin_test_dir + "2hy.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --noUnitConversion]

#     #write the result
#     results_file = TMP_PATH + "test_13.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "nan"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == False)


# def test_regtest_14():
#     """Test #14 : test reading fields with typvar == PZ"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PZ.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #['[ReaderStd --ignoreExtended --input {sources[0]}] >> ', '[WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_14.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "NEW/typvar_pz_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_15():
#     """Test #15 : test reading fields with typvar == PU"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PU.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_15.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "NEW/typvar_pu_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_16():
#     """Test #16 : test reading fields with typvar == PI"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PI.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_16.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "NEW/typvar_pi_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_17():
#     """Test #17 : test reading fields with typvar == PF"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PF.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_17.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "NEW/typvar_pf_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_18():
#     """Test #18 : test reading fields with typvar == PM"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5x2_fileSrc_PM.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_18.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "NEW/typvar_pm_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_19():
#     """Test #19 : test if HY is put in memory and written back when we have a grid with two kind of level, one of them being hybrid"""
#     # open and read source
#     source0 = plugin_test_dir + "mb_plus_hybrid_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path}]

#     #write the result
#     results_file = TMP_PATH + "test_19.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "mb_plus_hybrid_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_20():
#     """Test #20 : test if HY is put in memory and written back when we have a grid with hybrid level"""
#     # open and read source
#     source0 = plugin_test_dir + "mb_plus_hybrid_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [Select --fieldName FN] >> [WriterStd --output {destination_path}]

#     #write the result
#     results_file = TMP_PATH + "test_20.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "read_write_hy2_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_21():
#     """Test #21 : test that the HY is NOT written back when the final grid don't have a hybrid level"""
#     # open and read source
#     source0 = plugin_test_dir + "mb_plus_hybrid_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [Select --fieldName PR] >> [WriterStd --output {destination_path} --ignoreExtended]

#     #write the result
#     results_file = TMP_PATH + "test_21.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "read_write_hy3_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_22():
#     """Test #22 : test that PT is NOT read by the reader when the level type of the fields on the grid is not sigma"""
#     # open and read source
#     source0 = plugin_test_dir + "pt_with_hybrid.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path}]

#     #write the result
#     results_file = TMP_PATH + "test_22.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "read_write_pt_when_no_sigma_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_23():
#     """Test #23 : test that PT is NOT written back when there is a PT field created in memory and the level type of the fields on the grid is not sigma"""
#     # open and read source
#     source0 = plugin_test_dir + "kt_ai_hybrid.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [ZapSmart --fieldNameFrom AI --fieldNameTo PT] >> [WriterStd --output {destination_path}]

#     #write the result
#     results_file = TMP_PATH + "test_23.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "read_write_pt_when_no_sigma_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_25():
#     """Test #25 : Test la lecture avec ip2 != deet * npas"""
#     # open and read source
#     source0 = plugin_test_dir + "2012121000_cancm3_m1_00_fileSrc.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path} --writingMode APPEND]

#     #write the result
#     results_file = TMP_PATH + "test_25.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "2012121000_cancm3_m1_00_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_26():
#     """Test #26 : Test la lecture d'un fichier pilot """
#     # open and read source
#     source0 = plugin_test_dir + "2015040800_030_piloteta"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]}] >> [WriterStd --output {destination_path} ]

#     #write the result
#     results_file = TMP_PATH + "test_26.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "2015040800_030_piloteta"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_28():
#     """Test #28 : test lecture fichiers contenant caracteres speciaux ET parametre --input n'est pas le dernier"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVV5x5_+fileSrc.std"
#     src_df0 = StandardFileReader(source0)()

#     source1 = plugin_test_dir + "wind+Chill_file2cmp.std"
#     src_df1 = StandardFileReader(source1)

#     source2 = plugin_test_dir + "windModulus_file2cmp.std"
#     src_df2 = StandardFileReader(source2)


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #[ReaderStd --input {sources[0]} {sources[1]} {sources[2]} --ignoreExtended] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_28.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "stdPlusstd_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_29():
#     """Test #29 : test lecture fichiers contenant des champs de donnees manquantes"""
#     # open and read source
#     source0 = plugin_test_dir + "missingData.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #['[ReaderStd --input {sources[0]} --ignoreExtended] >> ', '[WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE --flagMissingData]']

#     #write the result
#     results_file = TMP_PATH + "test_29.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_29.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_30():
#     """Test #30 : test lecture fichiers contenant des membres d'ensemble differents"""
#     # open and read source
#     source0 = plugin_test_dir + "ensemble_members.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #['[ReaderStd --input {sources[0]}] >> ', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_30.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_30.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_31():
#     """Test #31 : test lecture fichiers contenant des masques"""
#     # open and read source
#     source0 = plugin_test_dir + "data_with_mask.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #['[ReaderStd --input {sources[0]}] >> ', '[Select --forecastHour 24] >>', '[WriterStd --output {destination_path}]']

#     #write the result
#     results_file = TMP_PATH + "test_31.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_31.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_32():
#     """Test #32 : test lecture fichiers contenant des membres d'ensemble differents"""
#     # open and read source
#     source0 = plugin_test_dir + "ens_data_exclamation.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #['[ReaderStd --input {sources[0]}] >>', '[Select --fieldName WGEX] >>', '[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]']

#     #write the result
#     results_file = TMP_PATH + "test_32.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_32.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


# def test_regtest_33():
#     """Test #33 : test lecture fichiers contenant la coordonnee 5005"""
#     # open and read source
#     source0 = plugin_test_dir + "resulttest_33.std"
#     src_df0 = StandardFileReader(source0)()


#     #compute ReaderStd
#     df = ReaderStd(src_df0)
#     #['[ReaderStd --input {sources[0]}]>>', '[WriterStd --output {destination_path}]']

#     #write the result
#     results_file = TMP_PATH + "test_33.std"
#     StandardFileWriter(results_file, df)()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "resulttest_33.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     assert(res == True)


