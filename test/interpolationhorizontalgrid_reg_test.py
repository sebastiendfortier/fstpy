# -*- coding: utf-8 -*-
from test import TEST_PATH, TMP_PATH

import pytest
from fstpy.dataframe_utils import fstcomp
from fstpy.interpolationhorizontalgrid import InterpolationHorizontalGrid
from fstpy.std_reader import StandardFileReader
from fstpy.std_writer import StandardFileWriter
from fstpy.utils import delete_file
import numpy as np
import pandas as pd
import rpnpy.librmn.all as rmn

pytestmark = [pytest.mark.interpolation_regtests] #, pytest.mark.regressions

@pytest.fixture
def plugin_test_dir():
    return TEST_PATH +"InterpolationHorizontalGrid/testsFiles/"

def convip(df,nomvar='',style=rmn.CONVIP_ENCODE):
    def convertip(df,i):
        ip1 = df.at[i,'ip1']
        (val, kind) = rmn.convertIp(rmn.CONVIP_DECODE, int(ip1))
        if kind != -1:
            df.at[i,'ip1'] = rmn.convertIp(style, val, kind)

    for i in df.index:
        if nomvar != '':
            if df.at[i,'nomvar'] == nomvar:
                convertip(df,i)
        else:
            convertip(df,i)
    return df        

def test_regtest_1(plugin_test_dir):
    """Test #1 :   Interpolation with multiple different input grid"""
    # open and read source

    source0 = plugin_test_dir + "input_big_fileSrc.std"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()


    #compute Pressure
    df = InterpolationHorizontalGrid(src_df0,method='user',grtyp='N',ni=191,nj=141,param1=79.0,param2=117.0,param3=57150.0,param4=21.0,interpolation_type='bi-linear',extrapolation_type='maximum').compute()
    #[ReaderStd --ignoreExtended --input {sources[0]}] >>[Pressure --coordinateType ETA_COORDINATE --referenceField TT] >>[Zap --pdsLabel R1580V0N] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df.loc[:,'typvar'] = 'PI'
    df.loc[:,'etiket'] = np.where(df.etiket == 'R1558V0N','R1558V0_N',df.etiket)
    df.loc[:,'etiket'] = np.where(df.etiket == 'G0928V4N','G0928V4_N',df.etiket)
    df.loc[:,'etiket'] = np.where(df.etiket == 'MXWIND','MXWIND__X',df.etiket)
    # df['datyp'] = 5
    # df['nbits'] = 32
    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_1.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()
    
    # open and read comparison file
    file_to_compare = plugin_test_dir + "interpolationHoriz_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_1"
    # print(file_to_compare)

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    delete_file(results_file)
    assert(res == True)

def test_regtest_2(plugin_test_dir):
    """Test #2 :   Interpolation with scalar fields only"""
    # open and read source
    source0 = plugin_test_dir + "4panneaux_input4_fileSrc.std"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()


    #compute Pressure
    df = InterpolationHorizontalGrid(src_df0,method='user',grtyp='N',ni=191,nj=141,param1=79.0,param2=117.0,param3=57150.0,param4=21.0,interpolation_type='bi-linear',extrapolation_type='maximum').compute()
    #[ReaderStd --ignoreExtended --input {sources[0]}] >>[Pressure --coordinateType ETA_COORDINATE --referenceField TT] >>[Zap --pdsLabel R1580V0N] >> [WriterStd --output {destination_path} --ignoreExtended --IP1EncodingStyle OLDSTYLE]
    df.loc[:,'typvar'] = 'PI'
    df.loc[:,'etiket'] = np.where(df.etiket == 'R1558V0N','R1558V0_N',df.etiket)
    df.loc[:,'etiket'] = np.where(df.etiket == 'G0928V4N','G0928V4_N',df.etiket)
    df.loc[:,'etiket'] = np.where(df.etiket == 'MXWIND','MXWIND__X',df.etiket)
    # df['datyp'] = 5
    # df['nbits'] = 32
    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_1.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()
    
    # open and read comparison file
    file_to_compare = plugin_test_dir + "interpolationHorizScalar_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_2"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    delete_file(results_file)
    assert(res == True)

def test_regtest_3(plugin_test_dir):
    """Test #3 :   Interpolation with vectorial fields only"""
    # open and read source
    source0 = plugin_test_dir + "inputUUVV.std"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()


    #compute Pressure
    df = InterpolationHorizontalGrid(src_df0,method='user',grtyp='N',ni=191,nj=141,param1=79.0,param2=117.0,param3=57150.0,param4=21.0,interpolation_type='bi-linear',extrapolation_type='maximum').compute()
    #"[ReaderStd --input {sources[0]}] >> [InterpolationHorizontalGrid -m USER_DEFINED --gridType TYPE_N --xyDimensions 191,141 -p 79.0,117.0,57150.0,21.0 --interpolationType BI-LINEAR --extrapolationType MAXIMUM] >> [Zap --nbitsForDataStorage E32]>>[WriterStd --output {destination_path} --IP1EncodingStyle OLDSTYLE]"
    df.loc[:,'typvar'] = 'PI'
    df.loc[:,'etiket'] = np.where(df.etiket == 'R1558V0N','R1558V0_N',df.etiket)
    df.loc[:,'etiket'] = np.where(df.etiket == 'G0928V4N','G0928V4_N',df.etiket)
    df.loc[:,'etiket'] = np.where(df.etiket == 'MXWIND','MXWIND__X',df.etiket)
    # df['datyp'] = 5
    # df['nbits'] = 32
    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_1.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()
    
    # open and read comparison file
    file_to_compare = plugin_test_dir + "interpolationHorizVectorial_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_3"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    delete_file(results_file)
    assert(res == True)    

def test_regtest_5(plugin_test_dir):
    """Test #5 :   Interpolation with FIELD_DEFINED"""
    # open and read source
    source0 = plugin_test_dir + "TTUUVVKTRT.std"

    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()

    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df0,method='field',nomvar='RT',interpolation_type='nearest',extrapolation_type='nearest').compute()
    #[ReaderStd --input {sources[0]}] >> [InterpolationHorizontalGrid -m FIELD_DEFINED --fieldName RT --interpolationType NEAREST --extrapolationType NEAREST] >> [WriterStd --output {destination_path} --makeIP1EncodingWorkWithTests]


    df = convip(df,nomvar='',style=rmn.CONVIP_ENCODE_OLD)

    # df['datyp'] = 5
    # df['nbits'] = 32

    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_5.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "fieldDefined_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_5"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    delete_file(results_file)
    assert(res == True)


def test_regtest_6(plugin_test_dir):
    """Test #6 :   Interpolation with FIELD_DEFINED, make sure HY follow"""
    # open and read source
    source0 = plugin_test_dir + "TT_RT_reghyb"

    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()

    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df0,method='field',nomvar='RT',interpolation_type='nearest',extrapolation_type='nearest').compute()
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName TT,RT] >> [InterpolationHorizontalGrid -m FIELD_DEFINED --fieldName RT --interpolationType NEAREST --extrapolationType NEAREST] >> [WriterStd --output {destination_path} ]

    # df['datyp'] = 5
    # df['nbits'] = 32
    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_6.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "fieldDefinedWithHY_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_6"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False)
    delete_file(results_file)
    assert(res == True)


def test_regtest_7(plugin_test_dir):
    """Test #7 :  Interpolation d'un champ scalaire (TT) d'une grille U vers une grille Z"""
    # open and read source
    source0 = plugin_test_dir + "2015072100_240_TTESUUVV_YinYang.std"
 
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()
    src_df0 = src_df0.query('nomvar in ["TT",">>","^^","^>","!!","HY"]')

    source1 = plugin_test_dir + "2015072100_240_TTESUUVV_GridZ.std"
    src_df1 = StandardFileReader(source1,decode_metadata=True).to_pandas()
    src_df1 = src_df1.query('nomvar in ["ES",">>","^^","^>","!!","HY"]')

    src_df = pd.concat([src_df0,src_df1],ignore_index=True)
    # print(src_df[['nomvar','ni','nj','ip1','ip2','ig1','ig2']])
    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df,method='field',nomvar='ES',interpolation_type='bi-cubic',extrapolation_type='nearest').compute()
    #([ReaderStd --input {sources[0]}] >> [Select --fieldName TT]) + ([ReaderStd --input {sources[1]}] >> [Select --fieldName ES]) >> [InterpolationHorizontalGrid -m FIELD_DEFINED --fieldName ES --interpolationType BI-CUBIC --extrapolationType NEAREST] >> [WriterStd --output {destination_path}]

    # df['datyp'] = 5
    # df['nbits'] = 32

    df = convip(df,nomvar='',style=rmn.CONVIP_ENCODE)


    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_7.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "InterpHorizGridUtoZ_rmn19_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_7"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False)
    # delete_file(results_file)
    assert(res == True)


def test_regtest_8(plugin_test_dir):
    """Test #8 :  Interpolation d'un champ scalaire (TT) d'une grille Z vers une grille U"""
    # open and read source
    source0 = plugin_test_dir + "2015072100_240_TTESUUVV_YinYang.std"
 
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()
    src_df0 = src_df0.query('nomvar  in ["ES",">>","^^","^>","!!","HY"]')

    source1 = plugin_test_dir + "2015072100_240_TTESUUVV_GridZ.std"
 
    src_df1 = StandardFileReader(source1,decode_metadata=True).to_pandas()
    src_df1 = src_df1.query('nomvar  in ["TT",">>","^^","^>","!!","HY"]')

    src_df = pd.concat([src_df0,src_df1],ignore_index=True)

    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df,method='field',nomvar='ES',interpolation_type='bi-cubic',extrapolation_type='nearest').compute()
    #([ReaderStd --input {sources[0]}] >> [Select --fieldName ES]) + ([ReaderStd --input {sources[1]}] >> [Select --fieldName TT]) >> [InterpolationHorizontalGrid -m FIELD_DEFINED --fieldName ES --interpolationType BI-CUBIC --extrapolationType NEAREST] >> [WriterStd --output {destination_path} ]

    # for i in df.index:
    #     if df.at[i,'nomvar'] != 'ES':
    #         df.at[i,'datyp'] = 5
    #         df.at[i,'nbits'] = 32

    df = convip(df,nomvar='ES',style=rmn.CONVIP_ENCODE)

    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_8.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "InterpHorizGridZtoU_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_8"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    # delete_file(results_file)
    assert(res == True)


def test_regtest_9(plugin_test_dir):
    """Test #9 :  Interpolation de champs vectoriels (UU,VV) d'une grille U vers une grille Z"""
    # open and read source
    source0 = plugin_test_dir + "2015072100_240_TTESUUVV_YinYang.std"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()
    src_df0 = src_df0.query('nomvar in ["UU","VV",">>","^^","^>","!!","HY"]')

    source1 = plugin_test_dir + "2015072100_240_TTESUUVV_GridZ.std"
    src_df1 = StandardFileReader(source1).to_pandas()
    src_df1 = src_df1.query('nomvar in ["TT",">>","^^","^>","!!","HY"]')

    src_df = pd.concat([src_df0,src_df1],ignore_index=True)

    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df,method='field',nomvar='TT',interpolation_type='bi-cubic',extrapolation_type='nearest').compute()
    #([ReaderStd --input {sources[0]}] >> [Select --fieldName UU,VV]) + ([ReaderStd --input {sources[1]}] >> [Select --fieldName TT]) >> [InterpolationHorizontalGrid -m FIELD_DEFINED --fieldName TT --interpolationType BI-CUBIC --extrapolationType NEAREST] >> [Select --fieldName UU,VV] >> [WriterStd --output {destination_path} ]

    # df['datyp'] = 5
    # df['nbits'] = 32
    df = convip(df,nomvar='',style=rmn.CONVIP_ENCODE)
    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_9.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "InterpHorizGridUtoZ_UUVV_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_9"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    delete_file(results_file)
    assert(res == True)


def test_regtest_10(plugin_test_dir):
    """Test #10 :  Interpolation de champs vectoriels (UU,VV) d'une grille Z vers une grille U"""
    # open and read source
    source0 = plugin_test_dir + "2015072100_240_TTESUUVV_YinYang.std"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()
    src_df0 = src_df0.query('nomvar  in ["TT",">>","^^","^>","!!","HY"]')

    source1 = plugin_test_dir + "2015072100_240_TTESUUVV_GridZ.std"
    src_df1 = StandardFileReader(source1,decode_metadata=True).to_pandas()
    src_df1 = src_df1.query('nomvar in ["UU","VV",">>","^^","^>","!!","HY"]')

    src_df = pd.concat([src_df0,src_df1],ignore_index=True)

    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df,method='field',nomvar='TT',interpolation_type='bi-cubic',extrapolation_type='nearest').compute()
    #([ReaderStd --input {sources[0]}] >> [Select --fieldName TT]) + ([ReaderStd --input {sources[1]}] >> [Select --fieldName UU,VV]) >> [InterpolationHorizontalGrid -m FIELD_DEFINED --fieldName TT --interpolationType BI-CUBIC --extrapolationType NEAREST] >> [Select --fieldName UU,VV] >> [WriterStd --output {destination_path} ]

    # df['datyp'] = 5
    # df['nbits'] = 32
    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_10.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "InterpHorizGridZtoU_UUVV_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_10"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    delete_file(results_file)
    assert(res == True)


def test_regtest_11(plugin_test_dir):
    """Test #11 :  Interpolation de champs vectoriels et scalaires d'une grille Z vers une grille U avec un fichier a interpoler contenant 2 toctocs."""
    # open and read source
    source0 = plugin_test_dir + "glbpres_TT_UU_VV.std"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()
    src_df0 = src_df0.query('nomvar in ["TT","UU","VV",">>","^^","^>","!!","HY"]')

    source1 = plugin_test_dir + "2015072100_240_TTESUUVV_YinYang.std"
    src_df1 = StandardFileReader(source1,decode_metadata=True).to_pandas()
    src_df1 = src_df1.query('nomvar in ["ES",">>","^^","^>","!!","HY"]')

    # source2 = plugin_test_dir + "2015072100_240_TTESUUVV_GridZ.std"
    # src_df2 = StandardFileReader(source2)

    src_df = pd.concat([src_df0,src_df1],ignore_index=True)

    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df,method='field',nomvar='ES',interpolation_type='bi-cubic',extrapolation_type='nearest').compute()
    #([ReaderStd --input {sources[0]}] >> [Select --fieldName TT,UU,VV]) + ([ReaderStd --input {sources[1]}] >> [Select --fieldName ES]) >> [InterpolationHorizontalGrid -m FIELD_DEFINED --fieldName ES --interpolationType BI-CUBIC --extrapolationType NEAREST] >> [WriterStd --output {destination_path} ]

    # df['datyp'] = 5
    # df['nbits'] = 32
    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_11.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "InterpHorizGridUtoZ_manyToctocs_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_11"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False,allclose=True)
    delete_file(results_file)
    assert(res == True)


def test_regtest_13(plugin_test_dir):
    """Test #13 :   test extrapolation with negative value"""
    # open and read source
    source0 = plugin_test_dir + "TT_RT_reghyb"
    src_df0 = StandardFileReader(source0,decode_metadata=True).to_pandas()
    src_df0 = src_df0.query('nomvar  in ["TT",">>","^^","^>","!!","HY"]')

    #compute InterpolationHorizontalGrid
    df = InterpolationHorizontalGrid(src_df0,method='user',grtyp='N',ni=152,nj=120,param1=52.0,param2=120.0,param3=50000.0,param4=21.0,interpolation_type='nearest',extrapolation_type='value',extrapolation_value=-888.8).compute()
    #[ReaderStd --input {sources[0]}] >> [Select --fieldName TT] >> [InterpolationHorizontalGrid -m USER_DEFINED --gridType TYPE_N --xyDimensions 152,120 -p 52.0,120.0,50000.0,21.0 --interpolationType NEAREST --extrapolationType VALUE=-888.8] >> [WriterStd --output {destination_path} ]

    # df['datyp'] = 5
    # df['nbits'] = 32

    #write the result
    results_file = TMP_PATH + "test_interpgrid_reg_13.std"
    delete_file(results_file)
    StandardFileWriter(results_file, df).to_fst()

    # open and read comparison file
    file_to_compare = plugin_test_dir + "extrapolationNegativeValue_file2cmp.std"
    # file_to_compare =  "/fs/site4/eccc/cmd/w/sbf000/testFiles/InterpolationHorizontalGrid/result_test_13"

    #compare results
    res = fstcomp(results_file,file_to_compare,exclude_meta=False)
    delete_file(results_file)
    assert(res == True)


