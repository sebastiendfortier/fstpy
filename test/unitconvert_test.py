# -*- coding: utf-8 -*-
import pytest
import fstpy.std_reader as fstr
import fstpy.std_writer as fstw
import fstpy.dataframe_utils as fstdfut
import fstpy.unit as fstuc
import fstpy.utils as fstut
from test import TMP_PATH, TEST_PATH


pytestmark = [pytest.mark.unit_regtests, pytest.mark.regressions]


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"UnitConvert/testsFiles/"

def test_regtest_1(plugin_test_dir):
    """Test #1 : test a case simple conversion"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = fstr.StandardFileReader(source0,load_data=True,decode_metadata=True).to_pandas()
    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [fstuc.do_unit_conversion --unit kilometer_per_hour] >> [WriterStd --output {destination_path} --noUnitConversion]
    df = fstdfut.zap(df,ip1=41394464)
    #write the result
    results_file = TMP_PATH + "test_1.std"
    fstut.delete_file(results_file)

    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmhExtended_file2cmp.std"

    #compare results
    res = fstdfut.fstdfut.fstcomp(results_file,file_to_compare)
    fstut.delete_file(results_file)
    assert(res == True)


def test_regtest_2(plugin_test_dir):
    """Test #2 : test a case with no conversion"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0, 'knot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [fstuc.do_unit_conversion --unit knot] >> [Zap --pdsLabel WINDMODULUS --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df = fstdfut.zap(df,etiket='WINDMODULUS')

    #write the result
    results_file = TMP_PATH + "test_2.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulus_file2cmp.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_3(plugin_test_dir):
    """Test #3 : test a case with no conversion (with extended info)"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'knot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [fstuc.do_unit_conversion --unit knot] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_3.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulusExtended_file2cmp.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_4(plugin_test_dir):
    """Test #4 : test a case with simple conversion and another plugin 2D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [fstuc.do_unit_conversion --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    df = fstdfut.zap(df,nomvar='UV*')
    #write the result
    results_file = TMP_PATH + "test_4.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh_file2cmp.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_5(plugin_test_dir):
    """Test #5 : test a case with simple conversion and another plugin 3D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5x2_fileSrc.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [fstuc.do_unit_conversion --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df = fstdfut.zap(df,nomvar='UV*')
    #write the result
    results_file = TMP_PATH + "test_5.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh3D_file2cmp.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_6(plugin_test_dir):
    """Test #6 : test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'kelvin')
    df = fstuc.do_unit_conversion(df,'fahrenheit')
    df = fstuc.do_unit_conversion(df,'celsius')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> (([Select --fieldName TT] >> [fstuc.do_unit_conversion --unit kelvin] >> [fstuc.do_unit_conversion --unit fahrenheit] >> [fstuc.do_unit_conversion --unit celsius]) + [Select --fieldName TT --exclude]) >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    df = fstdfut.select(df,'nomvar!="TT"')
    df = fstdfut.zap(nomvar='R1558V0N')
    #write the result
    results_file = TMP_PATH + "test_6.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UUVVTT5x5_fileSrc.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_7(plugin_test_dir):
    """Test #7 : test a case for output file mode in standard format"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()

    df0 = fstdfut.select(src_df0,'(nomvar=="TT") and (etiket==R1558V0N)')
    #compute fstuc.do_unit_conversion
    df0 = fstuc.do_unit_conversion(df0,'kelvin')

    df0 = fstdfut.select(src_df0,'(nomvar in ["UU","VV"]) and (etiket==R1558V0N)')
    #compute fstuc.do_unit_conversion
    df0 = fstuc.do_unit_conversion(df0,'kilometer_per_hour')

    df0 = fstdfut.select(src_df0,'(nomvar=="GZ") and (etiket==R1558V0N)')
    #compute fstuc.do_unit_conversion
    df0 = fstuc.do_unit_conversion(df0,'foot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
    # (
    # (
    # (
    # ([Select --fieldName TT --pdsLabel R1558V0N] >> [fstuc.do_unit_conversion --unit kelvin]) + 
    # ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [fstuc.do_unit_conversion --unit kilometer_per_hour]) + 
    # ([Select --fieldName GZ --pdsLabel R1558V0N] >> [fstuc.do_unit_conversion --unit foot])
    # ) >> [fstuc.do_unit_conversion --unit STD] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]) + 
    # [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]) >> 
    # [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_7.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_fileSrc.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_8(plugin_test_dir):
    """Test #8 : test a case with complete roundtrip conversion celcius -> kelvin -> celsius"""
    # open and read source
    source0 = plugin_test_dir + "TTES_fileSrc.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'kelvin')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> (([Select --fieldName TT] >> [fstuc.do_unit_conversion --unit kelvin]) + [Select --fieldName ES]) >> [fstuc.do_unit_conversion --unit celsius] >> [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_8.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "TTES_fileSrc.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_9(plugin_test_dir):
    """Test #9 : test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius in GeorgeKIndex context"""
    # open and read source
    source0 = plugin_test_dir + "TTES_fileSrc.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'kelvin')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> (([Select --fieldName TT] >> [fstuc.do_unit_conversion --unit kelvin]) + ([Select --fieldName ES] >> [fstuc.do_unit_conversion --unit fahrenheit])) >> [fstuc.do_unit_conversion --unit celsius] >> [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_9.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "TTES_fileSrc.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_10(plugin_test_dir):
    """Test #10 : test a case for output file mode in standard format"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'kelvin')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> (((([Select --fieldName TT --pdsLabel R1558V0N] >> [fstuc.do_unit_conversion --unit kelvin]) + ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [fstuc.do_unit_conversion --unit kilometer_per_hour]) + ([Select --fieldName GZ --pdsLabel R1558V0N] >> ([fstuc.do_unit_conversion --unit foot] + [Zap --fieldName ZGZ --doNotFlagAsZapped]))) >> [fstuc.do_unit_conversion --unit STD --ignoreMissing] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]) + [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]) >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_10.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_file2cmp.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_11(plugin_test_dir):
    """Test #11 : test --ignoremissing"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'scoobidoo')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [fstuc.do_unit_conversion --unit scoobidoo --ignoreMissing] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_11.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "ignoremissing_file2cmp.std"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_12(plugin_test_dir):
    """Test #12 : test bad unit"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = fstr.StandardFileReader(source0).to_pandas()


    #compute fstuc.do_unit_conversion
    df = fstuc.do_unit_conversion(src_df0,'scoobidoobidoo')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [fstuc.do_unit_conversion --unit scoobidoobidoo]

    #write the result
    results_file = TMP_PATH + "test_12.std"
    fstw.StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "nan"

    #compare results
    res = fstdfut.fstcomp(results_file,file_to_compare)
    assert(res == False)


