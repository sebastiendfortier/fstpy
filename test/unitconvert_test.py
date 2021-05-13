# -*- coding: utf-8 -*-
from fstpy.dataframe_utils import select,zap,fstcomp
from fstpy.std_reader import StandardFileReader
from fstpy.std_writer import StandardFileWriter
from fstpy.unit import do_unit_conversion
from fstpy.utils import delete_file
from test import TMP_PATH, TEST_PATH
import pandas as pd
import pytest
from fstpy.exceptions import UnitConversionError


pytestmark = [pytest.mark.unit_regtests, pytest.mark.regressions]


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"UnitConvert/testsFiles/"


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
    df['ip1']=41394464
    df['etiket']='WINDMOX'
    #write the result
    results_file = TMP_PATH + "test_unitconv_1.std"
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
    results_file = TMP_PATH + "test_unitconv_2.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulus_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_3(plugin_test_dir):
    """Test #3 : test a case with no conversion (with extended info)"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'knot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit knot] >> [WriterStd --output {destination_path}]

    df['ip1']=41394464
    df['etiket']='WINDMOX'

    #write the result
    results_file = TMP_PATH + "test_unitconv_3.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulusExtended_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_4(plugin_test_dir):
    """Test #4 : test a case with simple conversion and another plugin 2D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    src_df0 = StandardFileReader(source0,load_data=True).to_pandas()

    src_df0 = windmodulus(src_df0)
    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [UnitConvert --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    df = zap(df,nomvar='UV*')
    df['nomvar'] = 'UV*'
    df['etiket'] = 'WINDMODULUS'
    #write the result
    results_file = TMP_PATH + "test_unitconv_4.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


def test_regtest_5(plugin_test_dir):
    """Test #5 : test a case with simple conversion and another plugin 3D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5x2_fileSrc.std"
    src_df0 = StandardFileReader(source0,load_data=True).to_pandas()

    src_df0 = windmodulus(src_df0)
    #compute do_unit_conversion
    df = do_unit_conversion(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [UnitConvert --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df['nomvar'] = 'UV*'
    df['etiket'] = 'WINDMODULUS'
    #write the result
    results_file = TMP_PATH + "test_unitconv_5.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh3D_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare)
    delete_file(results_file)
    assert(res == True)


# def test_regtest_6(plugin_test_dir):
#     """Test #6 : test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius"""
#     # open and read source
#     source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
#     src_df0 = StandardFileReader(source0,load_data=True).to_pandas()
#     uuvv_df = select(src_df0,'nomvar!="TT"')
#     tt_df = select(src_df0,'nomvar=="TT"')
#     #compute do_unit_conversion
#     print(tt_df['d'])
#     tt_df = do_unit_conversion(tt_df,'kelvin')
#     print(tt_df['d'])
#     tt_df = do_unit_conversion(tt_df,'fahrenheit')
#     print(tt_df['d'])
#     tt_df = do_unit_conversion(tt_df,'celsius')
#     print(tt_df['d'])
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
#     # (
#     # ([Select --fieldName TT] >> [UnitConvert --unit kelvin] >> 
#     # [UnitConvert --unit fahrenheit] >> 
#     # [UnitConvert --unit celsius]
#     # ) + 
#     # [Select --fieldName TT --exclude]) >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     df = pd.concat([uuvv_df,tt_df])
#     # df = zap(df,etiket='R1558V0N')
    
#     #write the result
#     results_file = TMP_PATH + "test_unitconv_6.std"
#     delete_file(results_file)
#     StandardFileWriter(results_file, df).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "UUVVTT5x5_fileSrc.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res == True)


# def test_regtest_7(plugin_test_dir):
#     """Test #7 : test a case for output file mode in standard format"""
#     # open and read source
#     source0 = plugin_test_dir + "input_big_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()

#     tt_df = select(src_df0,'(nomvar=="TT") and (etiket=="R1558V0N")')
#     #compute do_unit_conversion
#     tt_df = do_unit_conversion(tt_df,'kelvin')

#     uuvv_df = select(src_df0,'(nomvar in ["UU","VV"]) and (etiket=="R1558V0N")')
#     #compute do_unit_conversion
#     uuvv_df = do_unit_conversion(uuvv_df,'kilometer_per_hour')

#     gz_df = select(src_df0,'(nomvar=="GZ") and (etiket=="R1558V0N")')
#     #compute do_unit_conversion
#     gz_df = do_unit_conversion(gz_df,'foot')

#     df = pd.concat([tt_df,uuvv_df,gz_df],ignore_index=True)
#     df = do_unit_conversion(df,standard_unit=True)
#     df['etiket'] = 'R1558V0N'

#     others_df = select(src_df0,'(nomvar in ["TT","UU","VV","GZ"]) and (etiket!="R1558V0N")')

#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
#     # (
#     # (
#     # (
#     # ([Select --fieldName TT --pdsLabel R1558V0N] >> [UnitConvert --unit kelvin]) + 
#     # ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [UnitConvert --unit kilometer_per_hour]) + 
#     # ([Select --fieldName GZ --pdsLabel R1558V0N] >> [UnitConvert --unit foot])
#     # ) >> [UnitConvert --unit STD] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]) + 
#     # [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]) >> 
#     # [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     all_df = pd.concat([df,others_df],ignore_index=True)
#     #write the result
#     results_file = TMP_PATH + "test_unitconv_7.std"
#     delete_file(results_file)
#     StandardFileWriter(results_file, all_df).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "input_big_fileSrc.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res == True)


# def test_regtest_8(plugin_test_dir):
#     """Test #8 : test a case with complete roundtrip conversion celcius -> kelvin -> celsius"""
#     # open and read source
#     source0 = plugin_test_dir + "TTES_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()

#     es_df = select(src_df0,'nomvar=="ES"')
#     tt_df = select(src_df0,'nomvar=="TT"')

#     #compute do_unit_conversion
#     tt_df = do_unit_conversion(tt_df,'kelvin')
#     es_df = do_unit_conversion(es_df,'celsius')

#     es_df['etiket'] = 'TESTGEORGESK'
#     all_df = pd.concat([es_df,tt_df],ignore_index=True)
    
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
#     # (
#     # ([Select --fieldName TT] >> [UnitConvert --unit kelvin]) + [Select --fieldName ES]) >> 
#     # [UnitConvert --unit celsius] >> 
#     # [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_unitconv_8.std"
#     delete_file(results_file)
#     StandardFileWriter(results_file, all_df).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "TTES_fileSrc.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res == True)


# def test_regtest_9(plugin_test_dir):
#     """Test #9 : test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius in GeorgeKIndex context"""
#     # open and read source
#     source0 = plugin_test_dir + "TTES_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()


#     es_df = select(src_df0,'nomvar=="ES"')
#     tt_df = select(src_df0,'nomvar=="TT"')

#     #compute do_unit_conversion
#     tt_df = do_unit_conversion(tt_df,'kelvin')
#     es_df = do_unit_conversion(es_df,'fahrenheit')

#     all_df = pd.concat([es_df,tt_df],ignore_index=True)
#     all_df = do_unit_conversion(all_df,'celsius')
    
#     #compute do_unit_conversion
#     all_df = zap(all_df,etiket="TESTGEORGESK")
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
#     # (([Select --fieldName TT] >> [UnitConvert --unit kelvin]) + 
#     # ([Select --fieldName ES] >> [UnitConvert --unit fahrenheit])) >> 
#     # [UnitConvert --unit celsius] >> [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >> 
#     # [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_unitconv_9.std"
#     delete_file(results_file)
#     StandardFileWriter(results_file, all_df).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "TTES_fileSrc.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res == True)


# def test_regtest_10(plugin_test_dir):
#     """Test #10 : test a case for output file mode in standard format"""
#     # open and read source
#     source0 = plugin_test_dir + "input_big_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()

#     tt_df = select(src_df0,'nomvar=="TT" and etiket=="R1558V0N"')
#     uuvv_df = select(src_df0,'nomvar in ["UU","VV"] and etiket=="R1558V0N"')
#     gz_df = select(src_df0,'nomvar=="GZ" and etiket=="R1558V0N"')
#     gz_df = zap(gz_df,nomvar='ZGZ')
#     #compute do_unit_conversion
#     tt_df = do_unit_conversion(tt_df,'kelvin')
#     uuvv_df = do_unit_conversion(uuvv_df,'kilometer_per_hour')
#     gz_df = do_unit_conversion(gz_df,'foot')

#     # gz_df['nomvar'] = 'ZGZ'

#     all_df = pd.concat([tt_df,uuvv_df,gz_df],ignore_index=True)
#     all_df = do_unit_conversion(all_df,standard_unit=True)

#     others_df = select(src_df0,'(nomvar in ["TT","UU","VV","GZ"]) and (etiket!="R1558V0N")')
#     all_df = pd.concat([all_df,others_df],ignore_index=True)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >> 
#     # (
#     # (
#     # (
#     # ([Select --fieldName TT --pdsLabel R1558V0N] >> [UnitConvert --unit kelvin]) + 
#     # ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [UnitConvert --unit kilometer_per_hour]) + 
#     # ([Select --fieldName GZ --pdsLabel R1558V0N] >> ([UnitConvert --unit foot] + [Zap --fieldName ZGZ --doNotFlagAsZapped])
#     # )
#     # ) >> 
#     # [UnitConvert --unit STD --ignoreMissing] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]) + [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]) >> 
#     # [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = TMP_PATH + "test_unitconv_10.std"
#     delete_file(results_file)
#     StandardFileWriter(results_file, all_df).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "input_big_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res == True)


# def test_regtest_12(plugin_test_dir):
#     """Test #12 : test bad unit"""
#     # open and read source
#     source0 = plugin_test_dir + "windModulus_file2cmp.std"
#     src_df0 = StandardFileReader(source0).to_pandas()


#     #compute do_unit_conversion
#     with pytest.raises(UnitConversionError):
#         df = do_unit_conversion(src_df0,'scoobidoobidoo')
    