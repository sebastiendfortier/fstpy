# -*- coding: utf-8 -*-
from fstpy.std_reader import StandardFileReader
from fstpy.std_writer import StandardFileWriter
from fstpy.unit import UnitConversionError, unit_convert
from fstpy.utils import delete_file
from test import TMP_PATH, TEST_PATH
import pandas as pd
import pytest
from ci_fstcomp import fstcomp
import secrets

pytestmark = [pytest.mark.unit_regtests] #, pytest.mark.regressions


@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"UnitConvert/testsFiles/"


def windmodulus(df):
    uu_df = df.loc[df.nomvar=="UU"].reset_index(drop=True)
    vv_df = df.loc[df.nomvar=="VV"].reset_index(drop=True)
    uv_df = vv_df.copy(deep=True)
    uv_df.loc[:,'nomvar']='UV'
    for i in uv_df.index:
        uu = (uu_df.at[i,'d'])
        vv = (vv_df.at[i,'d'])
        uv_df.at[i,'d'] = (uu**2 + vv**2)**.5
    return uv_df

def test_1(plugin_test_dir):
    """Test a case simple conversion"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()
    #compute unit_convert
    df = unit_convert(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit kilometer_per_hour] >> [WriterStd --output {destination_path} --noUnitConversion]
    df['ip1']=41394464
    df['etiket']='WINDMOX'
    df.loc[:, 'typvar'] = 'PU'
    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_1.std"])
    delete_file(results_file)

    # df['datyp']=5
    # df['nbits']=32
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmhExtended_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.1)
    delete_file(results_file)
    assert(res)


def test_2(plugin_test_dir):
    """Test a case with no conversion"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute unit_convert
    df = unit_convert(src_df0, 'knot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit knot] >> [Zap --pdsLabel WINDMODULUS --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df.loc[:,'etiket']='WINDMODULUS'

    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_2.std"])
    delete_file(results_file)
    # df['datyp']=5
    # df['nbits']=32
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulus_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.1)
    # delete_file(results_file)
    assert(res)


def test_3(plugin_test_dir):
    """Test a case with no conversion (with extended info)"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute unit_convert
    df = unit_convert(src_df0,'knot')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [UnitConvert --unit knot] >> [WriterStd --output {destination_path}]

    df['ip1']=41394464
    df['etiket']='WINDMOX'

    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_3.std"])
    delete_file(results_file)
    # df['datyp']=5
    # df['nbits']=32
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "windModulusExtended_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.1)
    delete_file(results_file)
    assert(res)


def test_4(plugin_test_dir):
    """Test a case with simple conversion and another plugin 2D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0 = windmodulus(src_df0)
    #compute unit_convert
    df = unit_convert(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [UnitConvert --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    df.loc[:,'nomvar']='UV*'
    df['nomvar'] = 'UV*'
    df['etiket'] = 'WINDMODULUS'
    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_4.std"])
    delete_file(results_file)
    # df['datyp']=5
    # df['nbits']=32
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.1)
    delete_file(results_file)
    assert(res)


def test_5(plugin_test_dir):
    """Test a case with simple conversion and another plugin 3D"""
    # open and read source
    source0 = plugin_test_dir + "UUVV5x5x2_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    src_df0 = windmodulus(src_df0)
    #compute unit_convert
    df = unit_convert(src_df0,'kilometer_per_hour')
    #[ReaderStd --ignoreExtended --input {sources[0]}] >> [WindModulus] >> [UnitConvert --unit kilometer_per_hour] >> [Zap --fieldName UV* --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df['nomvar'] = 'UV*'
    df['etiket'] = 'WINDMODULUS'
    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_5.std"])
    delete_file(results_file)
    # df['datyp']=5
    # df['nbits']=32
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "unitConvertUVInKmh3D_file2cmp.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.1)
    delete_file(results_file)
    assert(res)


def test_6(plugin_test_dir):
    """Test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius"""
    # open and read source
    source0 = plugin_test_dir + "UUVVTT5x5_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()

    other_df = src_df0.loc[src_df0.nomvar!="TT"]
    tt_df = src_df0.loc[src_df0.nomvar=="TT"]
    #compute unit_convert

    tt_df = unit_convert(tt_df,'kelvin')

    tt_df = unit_convert(tt_df,'fahrenheit')

    tt_df = unit_convert(tt_df,'celsius')

    #[ReaderStd --ignoreExtended --input {sources[0]}] >>
    # (
    # ([Select --fieldName TT] >> [UnitConvert --unit kelvin] >>
    # [UnitConvert --unit fahrenheit] >>
    # [UnitConvert --unit celsius]
    # ) +
    # [Select --fieldName TT --exclude]) >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    df = pd.concat([other_df,tt_df],ignore_index=True)


    # df = zap(df,etiket='R1558V0N')

    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_6.std"])
    delete_file(results_file)
    # df['datyp']=5
    # df['nbits']=32
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "UUVVTT5x5_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.1)
    delete_file(results_file)
    assert(res)


def test_7(plugin_test_dir):
    """Test a case for output file mode in standard format"""
    # open and read source
    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    meta_df = src_df0.loc[src_df0.nomvar.isin(['^^','>>'])]


    tt_df = src_df0.loc[(src_df0.nomvar=="TT") & (src_df0.etiket=="R1558V0N")]
    #compute unit_convert
    tt_df = unit_convert(tt_df,'kelvin')

    uuvv_df = src_df0.loc[(src_df0.nomvar.isin(["UU","VV"])) & (src_df0.etiket=="R1558V0N")]
    #compute unit_convert
    uuvv_df = unit_convert(uuvv_df,'kilometer_per_hour')

    gz_df = src_df0.loc[(src_df0.nomvar=="GZ") & (src_df0.etiket=="R1558V0N")]
    #compute unit_convert
    gz_df = unit_convert(gz_df,'foot')

    all_df = pd.concat([tt_df,uuvv_df,gz_df],ignore_index=True)

    all_df = unit_convert(all_df,standard_unit=True)


    others_df = src_df0.loc[(~src_df0.nomvar.isin(['^^','>>'])) & (src_df0.etiket!="R1558V0N")]

    #[ReaderStd --ignoreExtended --input {sources[0]}] >>
    # (
    #   (
    #    (
    #     ([Select --fieldName TT --pdsLabel R1558V0N] >> [UnitConvert --unit kelvin]) +
    #     ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [UnitConvert --unit kilometer_per_hour]) +
    #     ([Select --fieldName GZ --pdsLabel R1558V0N] >> [UnitConvert --unit foot])
    #    ) >> [UnitConvert --unit STD] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]
    #   ) +  [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]
    # ) >>
    # [WriterStd --output {destination_path} --noUnitConversion --ignoreExtended --IP1EncodingStyle OLDSTYLE]

    all_df = pd.concat([all_df,others_df,meta_df],ignore_index=True)
    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_7.std"])
    delete_file(results_file)
    all_df['datyp']=5
    all_df['nbits']=32
    StandardFileWriter(results_file, all_df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "input_big_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.1)
    delete_file(results_file)
    assert(res)


def test_8(plugin_test_dir):
    """Test a case with complete roundtrip conversion celcius -> kelvin -> celsius"""
    # open and read source
    source0 = plugin_test_dir + "TTES_fileSrc.std"
    src_df0 = StandardFileReader(source0, decode_metadata=True).to_pandas()

    es_df = src_df0.loc[src_df0.nomvar=="ES"]
    tt_df = src_df0.loc[src_df0.nomvar=="TT"]

    #compute unit_convert
    tt_df = unit_convert(tt_df,'kelvin')

    all_df = pd.concat([es_df,tt_df],ignore_index=True)

    all_df = unit_convert(all_df,'celsius')

    # "[ReaderStd --ignoreExtended --input {sources[0]}] >>
    # (
    # ([Select --fieldName TT] >> [UnitConvert --unit kelvin]) +  [Select --fieldName ES]
    # ) >>
    # [UnitConvert --unit celsius] >> [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]",


    #write the result
    results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_8.std"])
    delete_file(results_file)
    #all_df['datyp']=5
    #all_df['nbits']=32
    StandardFileWriter(results_file, all_df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "TTES_fileSrc.std"

    #compare results
    res = fstcomp(results_file,file_to_compare,e_max=0.01)
    delete_file(results_file)
    assert(res)


# def test_9(plugin_test_dir):
#     """Test a case with complete roundtrip conversion celcius -> kelvin -> fahrenheit -> celsius in GeorgeKIndex context"""
#     # open and read source
#     source0 = plugin_test_dir + "TTES_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()

#     es_df = src_df0.loc[src_df0.nomvar=="ES"]
#     tt_df = src_df0.loc[src_df0.nomvar=="TT"]

#     #compute unit_convert
#     tt_df = unit_convert(tt_df,'kelvin')
#     es_df = unit_convert(es_df,'fahrenheit')

#     all_df = pd.concat([es_df,tt_df],ignore_index=True)
#     all_df = unit_convert(all_df,'celsius')

#     #compute unit_convert
#     all_df.loc[:,'etiket']="TESTGEORGESK"
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >>
#     # (
#     # ([Select --fieldName TT] >> [UnitConvert --unit kelvin]) +
#     # ([Select --fieldName ES] >> [UnitConvert --unit fahrenheit])
#     # ) >>
#     # [UnitConvert --unit celsius] >>
#     # [Zap --pdsLabel TESTGEORGESK --doNotFlagAsZapped] >>
#     # [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_9.std"])
#     delete_file(results_file)
#     all_df['datyp']=5
#     all_df['nbits']=32
#     StandardFileWriter(results_file, all_df).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "TTES_fileSrc.std+20210713E32"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res)


# def test_10(plugin_test_dir):
#     """Test a case for output file mode in standard format"""
#     # open and read source
#     source0 = plugin_test_dir + "input_big_fileSrc.std"
#     src_df0 = StandardFileReader(source0).to_pandas()

#     tt_df = src_df0.loc[(src_df0.nomvar=="TT") & (src_df0.etiket=="R1558V0N")]
#     tt_df = unit_convert(tt_df,'kelvin')

#     uuvv_df = src_df0.loc[(src_df0.nomvar.isin(["UU","VV"])) & (src_df0.etiket=="R1558V0N"')]
#     uuvv_df = unit_convert(uuvv_df,'kilometer_per_hour')

#     gz_df = src_df0.query('nomvar=="GZ" and etiket=="R1558V0N"')
#     gz_df2 = gz_df.copy(deep=True)

#     gz_df = unit_convert(gz_df,'foot')

#     gz_df2['nomvar']='ZGZ'
#     #compute unit_convert


#     all_df = pd.concat([tt_df,uuvv_df,gz_df,gz_df2],ignore_index=True)
#     all_df = unit_convert(all_df,standard_unit=True)

#     others_df = src_df0.loc[src_df0.etiket!="R1558V0N"]
#     all_df = pd.concat([all_df,others_df],ignore_index=True)
#     #[ReaderStd --ignoreExtended --input {sources[0]}] >>
#     # (
#     #   (
#     #       (
#     #           ([Select --fieldName TT --pdsLabel R1558V0N] >> [UnitConvert --unit kelvin]) +
#     #           ([Select --fieldName UU,VV --pdsLabel R1558V0N] >> [UnitConvert --unit kilometer_per_hour]) +
#     #           ([Select --fieldName GZ --pdsLabel R1558V0N] >> ([UnitConvert --unit foot] + [Zap --fieldName ZGZ --doNotFlagAsZapped]))
#     #       ) >>  [UnitConvert --unit STD --ignoreMissing] >> [Zap --pdsLabel R1558V0N --doNotFlagAsZapped]
#     #   ) + [Select --fieldName TT,UU,VV,GZ --pdsLabel R1558V0N --exclude]
#     # ) >>
#     # [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]

#     #write the result
#     results_file = ''.join([TMP_PATH, secrets.token_hex(16), "test_unitconv_10.std"])
#     delete_file(results_file)
#     all_df['datyp']=5
#     all_df['nbits']=32
#     StandardFileWriter(results_file, all_df).to_fst()

#     # open and read comparison file
#     file_to_compare = plugin_test_dir + "input_big_file2cmp.std"

#     #compare results
#     res = fstcomp(results_file,file_to_compare)
#     delete_file(results_file)
#     assert(res)


def test_12(plugin_test_dir):
    """Test bad unit"""
    # open and read source
    source0 = plugin_test_dir + "windModulus_file2cmp.std"
    src_df0 = StandardFileReader(source0).to_pandas()


    #compute unit_convert
    with pytest.raises(UnitConversionError):
        df = unit_convert(src_df0,'scoobidoobidoo')
