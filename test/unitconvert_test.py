# -*- coding: utf-8 -*-
from fstpy.dataframe_utils import select,zap,fstcomp
from fstpy.std_reader import StandardFileReader
from fstpy.std_writer import StandardFileWriter
from fstpy.unit import do_unit_conversion
from fstpy.utils import delete_file
from test import TMP_PATH, TEST_PATH
import pandas as pd
import pytest


pytestmark = [pytest.mark.unit_regtests, pytest.mark.regressions]


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"UnitConvert/testsFiles/"

@pytest.fixture
def windmodulus(df):
    uu_df = select(df,'nomvar=="UU"')
    vv_df = select(df,'nomvar=="VV"')
    uv_df = vv_df.copy(deep=True)
    uv_df = zap(uv_df,nomvar='UV')
    for i in uv_df.index:
        uu = (uu_df.at[i,'d'])
        vv = (vv_df.at[i,'d']) 
        uv_df.at[i,'d'] = (uu**2 + vv**2)**.5
    return uv_df

def test_regtest_1(plugin_test_dir):
    """Test #1 : test a case simple conversion"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()
    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit kilometer_per_hour] >> [WriterStd --output {destination_path} --noUnitConversion]
    df = zap(df,ip1=41394464)
    #write the result
    results_file = TMP_PATH + "test_1.std"
    delete_file(results_file)

    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmhExtended_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_2(plugin_test_dir):
    """Test #2 : test a case with no conversion"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute do_unit_conversion
    df = do_unit_conversion(src_df0, 'knot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit knot] >> [Zap --pdsLabel WINDMODULUS --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df = zap(df,etiket='WINDMODULUS')

    #write the result
    results_file = TMP_PATH + "test_2.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulus_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_3(plugin_test_dir):
    """Test #3 : test a case with no conversion (with extended info)"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'knot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit knot] >> [WriterStd --output {destination_path}]

    #write the result
    results_file = TMP_PATH + "test_3.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulusExtended_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_4(plugin_test_dir):
    """Test #4 : test a case with simple conversion and another plugin 2D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0 = windmodulus(src_df0)
    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [UnitConvert --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    df = zap(df,nomvar='UV*')
    #write the result
    results_file = TMP_PATH + "test_4.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_5(plugin_test_dir):
    """Test #5 : test a case with simple conversion and another plugin 3D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5x2_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0 = windmodulus(src_df0)
    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [UnitConvert --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df = zap(df,nomvar='UV*')
    #write the result
    results_file = TMP_PATH + "test_5.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh3D_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_6(plugin_test_dir):
    """Test #6 : test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()
    uuvv_df = select(src_df0,'nomvar!="TT"')
    tt_df = select(src_df0,'nomvar=="TT"')
    #compute do_unit_conversion
    tt_df = do_unit_conversion(tt_df,'kelvin')
    tt_df = do_unit_conversion(tt_df,'fahrenheit')
    tt_df = do_unit_conversion(tt_df,'celsius')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
    # (
    # ([Select --fieldName TT] >> [UnitConvert --unit kelvin] >> 
    # [UnitConvert --unit fahrenheit] >> 
    # [UnitConvert --unit celsius]
    # ) + 
    # [Select --fieldName TT --exclude]) >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    df = pd.concat([uuvv_df,tt_df])
    df = zap(df,nomvar='R1558V0N')
    #write the result
    results_file = TMP_PATH + "test_6.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UUVVTT5x5_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_7(plugin_test_dir):
    """Test #7 : test a case for output file mode in standard format"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    tt_df = select(src_df0,'(nomvar=="TT") and (etiket==R1558V0N)')
    #compute do_unit_conversion
    tt_df = do_unit_conversion(tt_df,'kelvin')

    uuvv_df = select(src_df0,'(nomvar in ["UU","VV"]) and (etiket==R1558V0N)')
    #compute do_unit_conversion
    uuvv_df = do_unit_conversion(uuvv_df,'kilometer_per_hour')

    gz_df = select(src_df0,'(nomvar=="GZ") and (etiket==R1558V0N)')
    #compute do_unit_conversion
    gz_df = do_unit_conversion(gz_df,'foot')

    df = pd.concat([tt_df,uuvv_df,gz_df])
    df = do_unit_conversion(df,standard_unit=True)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
    # (
    # (
    # (
    # ([Select --fieldName TT --pdsLabel R1558V0N] >> [UnitConvert --unit kelvin]) + 
    # ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [UnitConvert --unit kilometer_per_hour]) + 
    # ([Select --fieldName GZ --pdsLabel R1558V0N] >> [UnitConvert --unit foot])
    # ) >> [UnitConvert --unit STD] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]) + 
    # [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]) >> 
    # [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_7.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_8(plugin_test_dir):
    """Test #8 : test a case with complete roundtrip conversion celcius -> kelvin -> celsius"""
    # open and read source
    source0 = plugin_test_dir + "TTES_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    es_df = select(src_df0,'nomvar=="ES"')
    tt_df = select(src_df0,'nomvar=="TT"')

    #compute do_unit_conversion
    tt_df = do_unit_conversion(tt_df,'kelvin')
    all_df = pd.concat([es_df,tt_df])
    all_df = do_unit_conversion(all_df,'celsius')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
    # (
    # ([Select --fieldName TT] >> [UnitConvert --unit kelvin]) + [Select --fieldName ES]) >> 
    # [UnitConvert --unit celsius] >> 
    # [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    all_df = zap(all_df,etiket="TESTGEORGESK")
    #write the result
    results_file = TMP_PATH + "test_8.std"
    StandardFileWriter(results_file, all_df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "TTES_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_9(plugin_test_dir):
    """Test #9 : test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius in GeorgeKIndex context"""
    # open and read source
    source0 = plugin_test_dir + "TTES_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    es_df = select(src_df0,'nomvar=="ES"')
    tt_df = select(src_df0,'nomvar=="TT"')

    #compute do_unit_conversion
    tt_df = do_unit_conversion(tt_df,'kelvin')
    es_df = do_unit_conversion(es_df,'fahrenheit')
    all_df = pd.concat([es_df,tt_df])
    all_df = do_unit_conversion(all_df,'celsius')

    #compute do_unit_conversion
    all_df = zap(all_df,etiket="TESTGEORGESK")
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
    # (([Select --fieldName TT] >> [UnitConvert --unit kelvin]) + 
    # ([Select --fieldName ES] >> [UnitConvert --unit fahrenheit])) >> 
    # [UnitConvert --unit celsius] >> [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >> 
    # [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_9.std"
    StandardFileWriter(results_file, all_df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "TTES_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_10(plugin_test_dir):
    """Test #10 : test a case for output file mode in standard format"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    tt_df = select(src_df0,'nomvar=="TT"')
    uuvv_df = select(src_df0,'nomvar in ["UU","VV"')
    gz_df = select(src_df0,'nomvar=="GZ"')
    gz_df = zap(gz_df,nomvar='ZGZ')
    #compute do_unit_conversion
    tt_df = do_unit_conversion(tt_df,'kelvin')
    uuvv_df = do_unit_conversion(uuvv_df,'kilometer_per_hour')
    gz_df = do_unit_conversion(gz_df,'foot')

    all_df = pd.concat([tt_df,uuvv_df,gz_df])
    all_df = do_unit_conversion(all_df,standard_unit=True)
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
    # (
    # (
    # (
    # ([Select --fieldName TT --pdsLabel R1558V0N] >> [UnitConvert --unit kelvin]) + 
    # ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [UnitConvert --unit kilometer_per_hour]) + 
    # ([Select --fieldName GZ --pdsLabel R1558V0N] >> ([UnitConvert --unit foot] + [Zap --fieldName ZGZ --doNotFlagAsZapped])
    # )
    # ) >> 
    # [UnitConvert --unit STD --ignoreMissing] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]) + [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]) >> 
    # [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_10.std"
    StandardFileWriter(results_file, all_df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_11(plugin_test_dir):
    """Test #11 : test --ignoremissing"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'scoobidoo')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit scoobidoo --ignoreMissing] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    #write the result
    results_file = TMP_PATH + "test_11.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "ignoremissing_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == True)


def test_regtest_12(plugin_test_dir):
    """Test #12 : test bad unit"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'scoobidoobidoo')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit scoobidoobidoo]

    #write the result
    results_file = TMP_PATH + "test_12.std"
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "nan"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    assert(res == False)


